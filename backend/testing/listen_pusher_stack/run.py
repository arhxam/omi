"""Run isolated high-fidelity tests for the listen -> pusher chain.

Run through ``run.sh`` so Firebase supplies a fresh Firestore emulator.  This
supervisor owns only the Redis, backend, pusher, and Parakeet child processes
that it starts; it never probes or stops user services.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

import httpx
import redis
import websockets
from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter

ROOT = Path(__file__).resolve().parents[3]
BACKEND = ROOT / 'backend'
PYTHON = BACKEND / '.venv' / 'bin' / 'python'
ADMIN_KEY = 'omi-listen-pusher-stack-admin-'
PROJECT = 'demo-omi-listen-stack'
LOCAL_TASK_TOKEN = 'omi-listen-pusher-stack-local-task'


class StackFailure(AssertionError):
    """An actionable scenario assertion failure."""


@dataclass
class Child:
    name: str
    process: subprocess.Popen[bytes]
    log_path: Path


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        return int(probe.getsockname()[1])


def _wait_for_port(port: int, *, label: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise StackFailure(f'{label} did not listen on 127.0.0.1:{port} within {timeout:.0f}s')


def _wait_until(predicate: Callable[[], bool], *, label: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise StackFailure(f'timed out waiting for {label}')


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding='utf-8').splitlines():
        with suppress(json.JSONDecodeError):
            value = json.loads(line)
            if isinstance(value, dict):
                rows.append(value)
    return rows


class Stack:
    def __init__(self, state_dir: Path, *, durable_dispatch: bool = False):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.durable_dispatch = durable_dispatch
        self.redis_port = _free_port()
        self.backend_port = _free_port()
        self.pusher_port = _free_port()
        self.parakeet_port = _free_port()
        self.children: dict[str, Child] = {}
        self.env = self._environment()
        self.firestore = firestore.Client(project=PROJECT)

    def _environment(self) -> dict[str, str]:
        firestore_host = os.getenv('FIRESTORE_EMULATOR_HOST')
        if not firestore_host:
            raise StackFailure('FIRESTORE_EMULATOR_HOST is required; run backend/testing/listen_pusher_stack/run.sh')
        if not firestore_host.startswith(('127.0.0.1:', 'localhost:')):
            raise StackFailure('listen-pusher stack only accepts a loopback Firestore emulator')
        isolated_home = self.state_dir / 'home'
        isolated_config = self.state_dir / 'config'
        isolated_home.mkdir(exist_ok=True)
        isolated_config.mkdir(exist_ok=True)
        # Never inherit developer credentials, provider keys, proxy settings, or
        # cloud CLI configuration. The test process owns only local child
        # services and has a private, empty HOME/XDG config root.
        env = {key: os.environ[key] for key in ('PATH', 'LANG', 'LC_ALL', 'TZ', 'TMPDIR') if os.getenv(key)}
        env.update(
            {
                'HOME': str(isolated_home),
                'XDG_CONFIG_HOME': str(isolated_config),
                'CLOUDSDK_CONFIG': str(isolated_config / 'gcloud'),
                'NO_PROXY': '127.0.0.1,localhost',
                'no_proxy': '127.0.0.1,localhost',
                'FIRESTORE_EMULATOR_HOST': firestore_host,
                'OMI_HARNESS_INSTANCE': 'listen-pusher-stack',
                'OMI_ENV_STAGE': 'offline',
                'PROVIDER_MODE': 'offline',
                'FIREBASE_PROJECT_ID': PROJECT,
                'GOOGLE_CLOUD_PROJECT': PROJECT,
                'GCLOUD_PROJECT': PROJECT,
                'FIRESTORE_DATABASE_ID': '(default)',
                'ENCRYPTION_SECRET': 'omi_listen_pusher_stack_test_secret_32_bytes',
                'ADMIN_KEY': ADMIN_KEY,
                'REDIS_DB_HOST': '127.0.0.1',
                'REDIS_DB_PORT': str(self.redis_port),
                'HOSTED_PUSHER_API_URL': f'http://127.0.0.1:{self.pusher_port}',
                'HOSTED_PARAKEET_API_URL': f'http://127.0.0.1:{self.parakeet_port}',
                'STT_SERVICE_MODELS': 'parakeet',
                'TRIAL_PAYWALL_ENABLED': 'false',
                'LISTEN_FINALIZATION_DISPATCH_MODE': 'cloud_tasks' if self.durable_dispatch else 'inline',
                'OMI_STACK_STATE_DIR': str(self.state_dir),
                'PYTHONPATH': str(BACKEND),
            }
        )
        if self.durable_dispatch:
            # The durable scenario records the exact task proto instead of
            # sending it to Cloud Tasks.  The worker still receives a real
            # HTTP request and checks the configured audience/identity before
            # it can touch the Firestore job.
            handler_url = f'http://127.0.0.1:{self.backend_port}/v1/conversation-finalization-jobs/run'
            env.update(
                {
                    'SYNC_TASKS_PROJECT': PROJECT,
                    'SYNC_TASKS_LOCATION': 'us-central1',
                    'LISTEN_FINALIZATION_TASKS_QUEUE': 'conversation-finalization',
                    'LISTEN_FINALIZATION_TASKS_HANDLER_URL': handler_url,
                    'LISTEN_FINALIZATION_TASKS_INVOKER_SA': 'local-finalization@demo-omi-listen-stack.iam.gserviceaccount.com',
                    'LISTEN_FINALIZATION_LOCAL_TASK_TOKEN': LOCAL_TASK_TOKEN,
                }
            )
        return env

    def _start(self, name: str, command: list[str], *, extra_env: dict[str, str] | None = None) -> Child:
        if name in self.children:
            raise StackFailure(f'{name} is already running')
        log_path = self.state_dir / f'{name}.log'
        process_env = self.env.copy()
        if extra_env:
            process_env.update(extra_env)
        # A recovery scenario restarts a child deliberately. Keep both sides
        # of that boundary in retained evidence instead of overwriting the
        # process that accepted the durable handoff.
        output = log_path.open('ab')
        process = subprocess.Popen(
            command,
            cwd=BACKEND,
            env=process_env,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        child = Child(name, process, log_path)
        self.children[name] = child
        return child

    def start(self, *, pusher_drop_opcode: int | None = None) -> None:
        redis_binary = shutil.which('redis-server')
        if not redis_binary:
            raise StackFailure('redis-server is required; install Redis and retry')
        self._start(
            'redis',
            [
                redis_binary,
                '--port',
                str(self.redis_port),
                '--save',
                '',
                '--appendonly',
                'no',
                '--protected-mode',
                'yes',
            ],
        )
        _wait_for_port(self.redis_port, label='isolated Redis')
        redis.Redis(host='127.0.0.1', port=self.redis_port).ping()
        self._start(
            'parakeet',
            [
                str(PYTHON),
                '-m',
                'uvicorn',
                'testing.listen_pusher_stack.parakeet_stub:app',
                '--host',
                '127.0.0.1',
                '--port',
                str(self.parakeet_port),
            ],
        )
        _wait_for_port(self.parakeet_port, label='Parakeet stub')
        pusher_env = (
            {'OMI_STACK_DROP_PUBLISHING_ON_OPCODE': str(pusher_drop_opcode)} if pusher_drop_opcode is not None else None
        )
        self._start(
            'pusher',
            [
                str(PYTHON),
                '-m',
                'uvicorn',
                'testing.listen_pusher_stack.pusher_app:app',
                '--host',
                '127.0.0.1',
                '--port',
                str(self.pusher_port),
            ],
            extra_env=pusher_env,
        )
        _wait_for_port(self.pusher_port, label='pusher')
        self._start_backend()

    def _start_backend(self) -> None:
        module = 'testing.listen_pusher_stack.durable_dispatch_app:app' if self.durable_dispatch else 'main:app'
        self._start(
            'backend',
            [str(PYTHON), '-m', 'uvicorn', module, '--host', '127.0.0.1', '--port', str(self.backend_port)],
        )
        _wait_for_port(self.backend_port, label='listen backend', timeout=45.0)

    def restart_pusher(self, *, drop_opcode: int | None = None) -> None:
        self.stop('pusher')
        pusher_env = {'OMI_STACK_DROP_PUBLISHING_ON_OPCODE': str(drop_opcode)} if drop_opcode is not None else None
        self._start(
            'pusher',
            [
                str(PYTHON),
                '-m',
                'uvicorn',
                'testing.listen_pusher_stack.pusher_app:app',
                '--host',
                '127.0.0.1',
                '--port',
                str(self.pusher_port),
            ],
            extra_env=pusher_env,
        )
        _wait_for_port(self.pusher_port, label='restarted pusher')

    def restart_backend(self) -> None:
        self.stop('backend')
        self._start_backend()

    def stop(self, name: str) -> None:
        child = self.children.pop(name, None)
        if not child or child.process.poll() is not None:
            return
        with suppress(ProcessLookupError):
            os.killpg(child.process.pid, signal.SIGTERM)
        try:
            child.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(child.process.pid, signal.SIGKILL)
            child.process.wait(timeout=5)

    def close(self) -> None:
        for name in list(self.children):
            self.stop(name)

    @property
    def pusher_events(self) -> list[dict[str, Any]]:
        return _read_events(self.state_dir / 'pusher.jsonl')

    @property
    def task_events(self) -> list[dict[str, Any]]:
        return _read_events(self.state_dir / 'finalization-tasks.jsonl')

    @property
    def worker_events(self) -> list[dict[str, Any]]:
        return _read_events(self.state_dir / 'finalization-worker.jsonl')

    def seed_user(self, uid: str) -> None:
        # Private cloud is enabled solely to exercise 103 + 101.  The pusher
        # process receives the real frames, but storage leaves are not invoked
        # by this harness because it intentionally has no cloud credentials.
        self.firestore.collection('users').document(uid).set(
            {
                'id': uid,
                'language': 'en',
                'private_cloud_sync_enabled': True,
                'data_protection_level': 'standard',
                'transcription_preferences': {'uses_custom_stt': False},
            }
        )

    def conversation(self, uid: str, conversation_id: str) -> dict[str, Any] | None:
        snapshot = (
            self.firestore.collection('users').document(uid).collection('conversations').document(conversation_id).get()
        )
        return snapshot.to_dict() if snapshot.exists else None

    def jobs_for(self, uid: str, conversation_id: str) -> list[dict[str, Any]]:
        query = self.firestore.collection('conversation_finalization_jobs').where(filter=FieldFilter('uid', '==', uid))
        jobs: list[dict[str, Any]] = []
        for document in query.stream():
            job = document.to_dict() or {}
            if job.get('conversation_id') != conversation_id:
                continue
            job['id'] = document.id
            jobs.append(job)
        return jobs

    def age_conversation(self, uid: str, conversation_id: str) -> None:
        self.firestore.collection('users').document(uid).collection('conversations').document(conversation_id).update(
            {'finished_at': datetime.now(timezone.utc) - timedelta(seconds=121)}
        )


async def _receive_until(websocket: Any, predicate: Callable[[Any], bool], *, label: str, timeout: float = 15.0) -> Any:
    deadline = time.monotonic() + timeout
    seen: list[Any] = []
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=max(0.05, remaining))
        except asyncio.TimeoutError:
            continue
        if not isinstance(message, str):
            continue
        with suppress(json.JSONDecodeError):
            payload = json.loads(message)
            seen.append(payload)
            if predicate(payload):
                return payload
    raise StackFailure(f'timed out waiting for {label}; observed {len(seen)} JSON messages')


async def _connect(stack: Stack, uid: str, session_id: str | None) -> tuple[Any, dict[str, Any]]:
    parameters: dict[str, Any] = {
        'language': 'en',
        'sample_rate': 8000,
        'codec': 'pcm8',
        'source': 'desktop',
        'stt_service': 'parakeet',
    }
    if session_id:
        parameters['client_conversation_id'] = session_id
    params = urlencode(parameters)
    websocket = await websockets.connect(
        f'ws://127.0.0.1:{stack.backend_port}/v4/listen?{params}',
        extra_headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
        max_size=10 * 1024 * 1024,
    )
    session = await _receive_until(
        websocket,
        lambda payload: isinstance(payload, dict) and payload.get('type') == 'conversation_session',
        label='conversation session',
    )
    await _receive_until(
        websocket,
        lambda payload: isinstance(payload, dict)
        and payload.get('type') == 'service_status'
        and payload.get('status') == 'ready',
        label='ready',
    )
    return websocket, session


async def _record_audio(websocket: Any) -> None:
    # pcm8 converts to real pcm16 in the production receiver.  This exceeds
    # its STT buffer threshold and makes the real Parakeet socket emit a segment.
    await websocket.send(bytes([128]) * 16_000)
    await _receive_until(
        websocket,
        lambda payload: isinstance(payload, list) and bool(payload) and payload[0].get('id') == 'stack-segment-1',
        label='streamed transcript',
    )


async def _request_finalization(stack: Stack, conversation_id: str, uid: str) -> httpx.Response:
    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as client:
        return await client.post(
            f'http://127.0.0.1:{stack.backend_port}/v1/conversations/{conversation_id}/finalize',
            headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
        )


async def _finalization_status(stack: Stack, conversation_id: str, uid: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as client:
        response = await client.get(
            f'http://127.0.0.1:{stack.backend_port}/v1/conversations/{conversation_id}/finalization',
            headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
        )
    if response.status_code != 200:
        raise StackFailure(f'finalization status returned HTTP {response.status_code}: {response.text[:300]}')
    payload = response.json()
    if not isinstance(payload, dict):
        raise StackFailure('finalization status did not return an object')
    return payload


async def _deliver_finalization_task(
    stack: Stack, task: dict[str, Any], *, authorization: str | None
) -> httpx.Response:
    payload = task.get('payload')
    url = task.get('url')
    if not isinstance(payload, dict) or not isinstance(url, str):
        raise StackFailure('recorded Cloud Tasks wake-up did not contain an HTTP payload and URL')
    headers = {'X-CloudTasks-TaskRetryCount': '0'}
    if authorization:
        headers['Authorization'] = authorization
    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as client:
        return await client.post(url, json=payload, headers=headers)


def _wait_for_job(
    stack: Stack, uid: str, conversation_id: str, status: str, *, timeout: float = 25.0
) -> dict[str, Any]:
    result: list[dict[str, Any]] = []

    def found() -> bool:
        nonlocal result
        result = stack.jobs_for(uid, conversation_id)
        return len(result) == 1 and result[0].get('status') == status

    _wait_until(found, label=f'finalization job {status}', timeout=timeout)
    return result[0]


def _assert_local_provider_admission(stack: Stack, conversation_id: str) -> None:
    """Assert local seams were reached once, without claiming external delivery."""
    events = stack.pusher_events
    fanouts = [
        event
        for event in events
        if event.get('event') == 'integration_fanout_skipped' and event.get('conversation_id') == conversation_id
    ]
    if len(fanouts) != 1:
        raise StackFailure(f'expected one durable finalization fanout admission, observed {len(fanouts)}')
    audio_flushes = [
        event
        for event in events
        if event.get('event') == 'audio_storage_skipped' and event.get('conversation_id') == conversation_id
    ]
    if len(audio_flushes) != 1 or not audio_flushes[0].get('bytes'):
        raise StackFailure('pusher did not admit the private-cloud audio queue exactly once')


async def _normal_and_terminal_reconnect(stack: Stack) -> None:
    uid = 'stack-normal'
    session_id = str(uuid.uuid4())
    stack.seed_user(uid)
    websocket, first_session = await _connect(stack, uid, session_id)
    if first_session.get('conversation_id') != session_id or first_session.get('status') != 'in_progress':
        raise StackFailure('new desktop recording did not bind its requested native session UUID')
    await _record_audio(websocket)
    await websocket.close(code=1000)
    job = _wait_for_job(stack, uid, session_id, 'completed')
    conversation = stack.conversation(uid, session_id)
    if not conversation or conversation.get('status') != 'completed' or not conversation.get('has_content'):
        raise StackFailure('normal recording was not persisted and completed through the lifecycle owner')
    events = stack.pusher_events
    opcodes = {event.get('opcode') for event in events if event.get('direction') == 'in'}
    required = {101, 102, 103, 104}
    if not required.issubset(opcodes):
        raise StackFailure(f'normal path missed pusher frames: expected {sorted(required)}, observed {sorted(opcodes)}')
    if not any(
        event.get('direction') == 'out' and event.get('opcode') == 201 and event.get('success') for event in events
    ):
        raise StackFailure('normal path did not receive a successful pusher result frame')
    if job.get('fanout_status') != 'completed':
        raise StackFailure('normal finalization job completed without durable fanout completion')
    _assert_local_provider_admission(stack, session_id)

    # A terminal reconnect with the same native recording UUID must replay the
    # old binding and create a fresh active one, never a second job for the old.
    websocket, replay_session = await _connect(stack, uid, session_id)
    if replay_session.get('conversation_id') != session_id or replay_session.get('status') != 'completed':
        raise StackFailure('terminal native reconnect did not replay the completed recording binding')
    await websocket.close(code=1000)
    jobs = stack.jobs_for(uid, session_id)
    if len(jobs) != 1 or jobs[0].get('id') != job.get('id'):
        raise StackFailure('terminal reconnect changed the original durable finalization job')


async def _empty_recording(stack: Stack) -> None:
    uid = 'stack-empty'
    session_id = str(uuid.uuid4())
    stack.seed_user(uid)
    websocket, _ = await _connect(stack, uid, session_id)
    await websocket.close(code=1000)
    stack.age_conversation(uid, session_id)
    # Single-channel clean disconnects intentionally defer processing. A later
    # session sees this stale recording as timed out, routes it through the
    # production lifecycle owner, and tombstones it after the pending delay.
    websocket, _ = await _connect(stack, uid, None)

    def deleted() -> bool:
        return stack.conversation(uid, session_id) is None

    _wait_until(deleted, label='stale empty desktop recording deletion', timeout=20.0)
    await websocket.close(code=1000)
    if stack.jobs_for(uid, session_id):
        raise StackFailure('empty desktop recording created a finalization job')


async def _pusher_restart_replay(stack: Stack) -> None:
    uid = 'stack-restart'
    session_id = str(uuid.uuid4())
    stack.seed_user(uid)
    # Restart into a deterministic test mode that drops the first 104 before
    # job claim.  The backend session stays live and must replay its pending
    # exact job/generation after the real pusher process is restarted.
    stack.restart_pusher(drop_opcode=104)
    websocket, _ = await _connect(stack, uid, session_id)
    await _record_audio(websocket)
    stack.age_conversation(uid, session_id)

    def dropped() -> bool:
        return any(event.get('event') == 'intentional_drop_before_dispatch' for event in stack.pusher_events)

    _wait_until(dropped, label='pusher drop after finalization handoff', timeout=20.0)
    first_request = next(
        event
        for event in stack.pusher_events
        if event.get('direction') == 'in' and event.get('opcode') == 104 and event.get('conversation_id') == session_id
    )
    stack.restart_pusher()
    job = _wait_for_job(stack, uid, session_id, 'completed', timeout=30.0)
    finalization_requests = [
        event
        for event in stack.pusher_events
        if event.get('direction') == 'in' and event.get('opcode') == 104 and event.get('conversation_id') == session_id
    ]
    if len(finalization_requests) < 2:
        raise StackFailure('pusher restart did not replay the pending finalization request')
    replay = finalization_requests[-1]
    if (
        replay.get('finalization_job_id') != first_request.get('finalization_job_id')
        or replay.get('dispatch_generation') != first_request.get('dispatch_generation')
        or replay.get('finalization_job_id') != job.get('id', replay.get('finalization_job_id'))
    ):
        raise StackFailure('pusher replay changed its durable finalization identity')
    if job.get('attempt_count') != 1:
        raise StackFailure(
            f'pusher restart processed the durable job more than once: attempts={job.get("attempt_count")}'
        )
    _assert_local_provider_admission(stack, session_id)
    await websocket.close(code=1000)


async def _durable_rest_finalization_survives_backend_restart(stack: Stack) -> None:
    """Exercise REST -> Firestore outbox -> task worker across a process loss."""
    if not stack.durable_dispatch:
        raise StackFailure('durable finalization scenario requires a Cloud Tasks dispatch stack')

    uid = 'stack-rest-finalization'
    session_id = str(uuid.uuid4())
    stack.seed_user(uid)
    websocket, session = await _connect(stack, uid, session_id)
    if session.get('conversation_id') != session_id:
        raise StackFailure('durable REST recording did not keep its requested native UUID')
    await _record_audio(websocket)

    def content_persisted() -> bool:
        conversation = stack.conversation(uid, session_id)
        return bool(conversation and conversation.get('has_content'))

    _wait_until(content_persisted, label='persisted content before REST finalization')
    accepted = await _request_finalization(stack, session_id, uid)
    if accepted.status_code != 200:
        raise StackFailure(f'durable REST finalization returned HTTP {accepted.status_code}: {accepted.text[:300]}')
    accepted_payload = accepted.json()
    conversation_payload = accepted_payload.get('conversation') if isinstance(accepted_payload, dict) else None
    if not isinstance(conversation_payload, dict) or conversation_payload.get('status') != 'processing':
        raise StackFailure('durable REST finalization did not promptly return the admitted processing snapshot')

    task_events: list[dict[str, Any]] = []

    def task_recorded() -> bool:
        nonlocal task_events
        task_events = [event for event in stack.task_events if event.get('event') == 'task_created']
        return len(task_events) == 1

    _wait_until(task_recorded, label='opaque Cloud Tasks wake-up')
    task = task_events[0]
    job = _wait_for_job(stack, uid, session_id, 'queued')
    expected_payload = {'job_id': job['id'], 'dispatch_generation': 1}
    if task.get('payload') != expected_payload:
        raise StackFailure(f'Cloud Tasks wake-up was not the exact opaque job payload: {task.get("payload")}')
    payload_text = json.dumps(task['payload'], sort_keys=True)
    if uid in payload_text or session_id in payload_text:
        raise StackFailure('Cloud Tasks wake-up exposed a user or conversation identifier')
    expected_task_name = (
        f'projects/{PROJECT}/locations/us-central1/queues/conversation-finalization/'
        f'tasks/listen-finalization-{job["id"]}-1'
    )
    if task.get('task_name') != expected_task_name:
        raise StackFailure('durable finalization created an unexpected named Cloud Tasks task')
    expected_url = f'http://127.0.0.1:{stack.backend_port}/v1/conversation-finalization-jobs/run'
    if task.get('url') != expected_url or task.get('oidc_audience') != expected_url:
        raise StackFailure('durable finalization task did not target the configured worker audience')
    if task.get('oidc_service_account') != 'local-finalization@demo-omi-listen-stack.iam.gserviceaccount.com':
        raise StackFailure('durable finalization task did not retain its configured OIDC invoker')
    if task.get('dispatch_deadline_seconds') != 1500:
        raise StackFailure('durable finalization task did not retain the production dispatch deadline')
    task_headers = {str(key).lower(): value for key, value in dict(task.get('headers') or {}).items()}
    if task_headers.get('content-type') != 'application/json':
        raise StackFailure('durable finalization task omitted its JSON content type')

    queued_status = await _finalization_status(stack, session_id, uid)
    if (
        queued_status.get('job_id') != job['id']
        or queued_status.get('status') != 'queued'
        or queued_status.get('terminal')
        or not queued_status.get('retryable')
        or queued_status.get('attempt_count') != 0
    ):
        raise StackFailure(f'queued finalization status projection was incorrect: {queued_status}')

    # The real route must reject an unauthenticated Cloud Tasks delivery before
    # claiming the job.  This test seam substitutes only remote JWKS lookup;
    # it leaves the worker route, dependency binding, and opaque parser real.
    denied = await _deliver_finalization_task(stack, task, authorization=None)
    if denied.status_code != 403:
        raise StackFailure(f'worker accepted an unauthenticated task delivery: HTTP {denied.status_code}')
    if _wait_for_job(stack, uid, session_id, 'queued').get('attempt_count') != 0:
        raise StackFailure('unauthenticated task delivery changed the durable job claim state')

    # Lose the backend after the durable task exists but before it is delivered.
    # The original WebSocket has no in-process finalizer fallback to recover it.
    stack.restart_backend()
    delivered = await _deliver_finalization_task(stack, task, authorization=f'Bearer {LOCAL_TASK_TOKEN}')
    if delivered.status_code != 200 or delivered.json() != {'status': 'done'}:
        raise StackFailure(
            f'first task delivery did not complete the real worker: {delivered.status_code} {delivered.text[:300]}'
        )

    completed_status = await _finalization_status(stack, session_id, uid)
    if (
        completed_status.get('job_id') != job['id']
        or completed_status.get('status') != 'completed'
        or not completed_status.get('terminal')
        or completed_status.get('retryable')
        or completed_status.get('attempt_count') != 1
    ):
        raise StackFailure(f'completed finalization status projection was incorrect: {completed_status}')
    completed_job = _wait_for_job(stack, uid, session_id, 'completed')
    if completed_job.get('fanout_status') != 'completed':
        raise StackFailure('durable task worker completed the job without durable integration fanout completion')

    worker_events = stack.worker_events
    expected_worker_events = {'process_completed': 1, 'memory_extraction_skipped': 1, 'integration_fanout_skipped': 1}
    observed_worker_events = {
        event: sum(row.get('event') == event for row in worker_events) for event in expected_worker_events
    }
    if observed_worker_events != expected_worker_events:
        raise StackFailure(f'worker did not reach each provider seam exactly once: {observed_worker_events}')
    process_event = next(event for event in worker_events if event.get('event') == 'process_completed')
    if not process_event.get('persisted') or not process_event.get('force_process'):
        raise StackFailure('durable REST worker did not persist the route-required force_process finalization')
    if not process_event.get('defer_memory_extraction'):
        raise StackFailure('durable finalizer did not retain its owned memory-extraction ordering')

    duplicate = await _deliver_finalization_task(stack, task, authorization=f'Bearer {LOCAL_TASK_TOKEN}')
    if duplicate.status_code != 200 or duplicate.json() != {'status': 'acked', 'job_status': 'completed'}:
        raise StackFailure(
            f'duplicate task delivery was not safely acknowledged: {duplicate.status_code} {duplicate.text[:300]}'
        )
    if len([event for event in stack.task_events if event.get('event') == 'task_created']) != 1:
        raise StackFailure('worker delivery recreated a durable Cloud Tasks task')
    if _wait_for_job(stack, uid, session_id, 'completed').get('attempt_count') != 1:
        raise StackFailure('duplicate task delivery ran the completed finalizer more than once')
    if len(stack.worker_events) != len(worker_events):
        raise StackFailure('duplicate task delivery repeated a provider-side finalization leaf')

    with suppress(Exception):
        await websocket.close(code=1000)


async def run_inline_scenarios(stack: Stack) -> None:
    await _normal_and_terminal_reconnect(stack)
    await _empty_recording(stack)
    await _pusher_restart_replay(stack)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, help='directory for sanitized process logs and JSONL evidence')
    parser.add_argument('--keep', action='store_true', help='preserve generated evidence after a successful run')
    parser.add_argument(
        '--suite',
        choices=('all', 'inline', 'durable'),
        default='all',
        help='run the full gauntlet (default), only live listen/pusher scenarios, or only durable REST finalization',
    )
    args = parser.parse_args()
    if not PYTHON.exists():
        raise SystemExit(f'missing backend virtual environment: {PYTHON}; run backend/scripts/sync-python-deps.sh')
    state_dir = args.state_dir or Path(tempfile.mkdtemp(prefix='omi-listen-pusher-stack-'))
    state_dir.mkdir(parents=True, exist_ok=True)
    stacks: list[Stack] = []
    try:
        completed_suites: list[str] = []
        if args.suite in {'all', 'inline'}:
            inline_stack = Stack(state_dir / 'inline')
            stacks.append(inline_stack)
            inline_stack.start()
            asyncio.run(run_inline_scenarios(inline_stack))
            inline_stack.close()
            completed_suites.append('listen-pusher')

        if args.suite in {'all', 'durable'}:
            durable_stack = Stack(state_dir / 'durable-finalization', durable_dispatch=True)
            stacks.append(durable_stack)
            durable_stack.start()
            asyncio.run(_durable_rest_finalization_survives_backend_restart(durable_stack))
            durable_stack.close()
            completed_suites.append('durable-finalization')

        noun = 'gauntlet' if len(completed_suites) == 1 else 'gauntlets'
        print(f'{" and ".join(completed_suites)} stack {noun} passed; evidence: {state_dir}')
        return 0
    except Exception as error:
        print(f'listen-pusher stack gauntlet failed; evidence retained: {state_dir}', file=sys.stderr)
        raise
    finally:
        for stack in reversed(stacks):
            stack.close()
        if not args.keep and not sys.exc_info()[0]:
            shutil.rmtree(state_dir)


if __name__ == '__main__':
    raise SystemExit(main())
