"""Run the isolated Sync Cloud Tasks stack gauntlet.

Invoke through ``run.sh`` so Firebase owns a fresh Firestore emulator.  This
supervisor starts only its private Redis and ASGI child processes; it never
probes, reuses, or stops developer services.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import google.auth
import httpx
import redis
from google.auth import credentials as google_auth_credentials
from google.cloud import firestore

ROOT = Path(__file__).resolve().parents[3]
BACKEND = ROOT / 'backend'
PYTHON = BACKEND / '.venv' / 'bin' / 'python'
PROJECT = 'demo-omi-sync-cloud-tasks-stack'
ADMIN_KEY = 'omi-sync-cloud-tasks-stack-admin-'
LOCAL_OIDC_AUDIENCE = 'https://sync-stack.local/v2/sync-jobs/run'
LOCAL_INVOKER_SA = 'sync-stack-invoker@demo-omi-sync-cloud-tasks-stack.iam.gserviceaccount.com'
LOCAL_OIDC_TOKEN = f'local-sync-oidc:{LOCAL_INVOKER_SA}'
SYNC_QUEUE = 'sync-jobs'
SENSITIVE_UID = 'sync-stack-sensitive-uid'
TRANSCRIPT_TOKEN = 'Sync stack transcript verified.'
DEVICE_HASH = '01234567'
DEVICE_ID = f'ios_{DEVICE_HASH}'
ENCRYPTION_SECRET = 'omi_sync_cloud_tasks_stack_test_secret_32_bytes'


class StackFailure(AssertionError):
    """An actionable assertion failure from one local stack scenario."""


@dataclass
class Child:
    name: str
    process: subprocess.Popen[bytes]
    log_path: Path


def _anonymous_google_credentials(
    *_args: Any, **_kwargs: Any
) -> tuple[google_auth_credentials.AnonymousCredentials, str]:
    return google_auth_credentials.AnonymousCredentials(), PROJECT


# The parent process also connects only to the emulator; never allow ADC lookup.
google.auth.default = _anonymous_google_credentials


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


def _wait_until(predicate: Callable[[], bool], *, label: str, timeout: float = 25.0) -> None:
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
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.logs_dir = state_dir / 'logs'
        self.evidence_dir = state_dir / 'evidence'
        self.storage_dir = state_dir / 'local-storage'
        self.redis_port = _free_port()
        self.backend_port = _free_port()
        self.children: dict[str, Child] = {}
        self.http = httpx.Client(trust_env=False)
        self.control_token = secrets.token_urlsafe(32)
        self.created_uids: set[str] = set()
        # Runner-side evidence goes through the same sanitizer as the ASGI
        # child. This process is itself short-lived, so these explicit values
        # cannot affect a developer shell after the gauntlet exits.
        os.environ['OMI_SYNC_STACK_STATE_DIR'] = str(state_dir)
        os.environ['OMI_SYNC_STACK_SENSITIVE_UID'] = SENSITIVE_UID
        os.environ['OMI_SYNC_STACK_TRANSCRIPT_TOKEN'] = TRANSCRIPT_TOKEN
        # The runner uses the production conversation encoder when it seeds
        # capture provenance, so it must match the isolated child process.
        # It affects only this short-lived emulator command, never its caller.
        os.environ['ENCRYPTION_SECRET'] = ENCRYPTION_SECRET
        # The runner itself touches Firestore only through its loopback
        # emulator.  Do not let a dynamic production helper import discover a
        # developer credential file while encoding the seed record.
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
        os.environ.pop('SERVICE_ACCOUNT_JSON', None)
        self.env = self._environment()
        self.firestore = firestore.Client(project=PROJECT)

    def _environment(self) -> dict[str, str]:
        firestore_host = os.getenv('FIRESTORE_EMULATOR_HOST', '').strip()
        host = firestore_host.rsplit(':', 1)[0] if ':' in firestore_host else firestore_host
        if host not in {'127.0.0.1', 'localhost'}:
            raise StackFailure('FIRESTORE_EMULATOR_HOST must be a loopback endpoint; run via run.sh')

        isolated_home = self.state_dir / 'home'
        isolated_config = self.state_dir / 'config'
        for directory in (isolated_home, isolated_config, self.logs_dir, self.evidence_dir, self.storage_dir):
            directory.mkdir(parents=True, exist_ok=True)

        # Deliberately inherit no provider keys, ADC paths, proxy settings, or
        # cloud CLI configuration. The app wrapper independently rejects any
        # non-loopback socket/DNS use.
        env = {key: os.environ[key] for key in ('PATH', 'LANG', 'LC_ALL', 'TZ', 'TMPDIR') if os.getenv(key)}
        env.update(
            {
                'HOME': str(isolated_home),
                'XDG_CONFIG_HOME': str(isolated_config),
                'CLOUDSDK_CONFIG': str(isolated_config / 'gcloud'),
                'NO_PROXY': '127.0.0.1,localhost',
                'no_proxy': '127.0.0.1,localhost',
                'FIRESTORE_EMULATOR_HOST': firestore_host,
                'FIREBASE_AUTH_EMULATOR_HOST': '127.0.0.1:9099',
                'OMI_HARNESS_INSTANCE': 'sync-cloud-tasks-stack',
                'OMI_ENV_STAGE': 'offline',
                'PROVIDER_MODE': 'offline',
                'LOCAL_DEVELOPMENT': 'true',
                'FIREBASE_PROJECT_ID': PROJECT,
                'GOOGLE_CLOUD_PROJECT': PROJECT,
                'GCLOUD_PROJECT': PROJECT,
                'FIRESTORE_DATABASE_ID': '(default)',
                'ENCRYPTION_SECRET': ENCRYPTION_SECRET,
                'ADMIN_KEY': ADMIN_KEY,
                'REDIS_DB_HOST': '127.0.0.1',
                'REDIS_DB_PORT': str(self.redis_port),
                'REDIS_DB_PASSWORD': '',
                'SYNC_DISPATCH_MODE': 'cloud_tasks',
                'SYNC_LEDGER_FENCE_MODE': 'active',
                'SYNC_TASKS_PROJECT': PROJECT,
                'SYNC_TASKS_LOCATION': 'local',
                'SYNC_TASKS_QUEUE': SYNC_QUEUE,
                'SYNC_TASKS_HANDLER_URL': f'http://127.0.0.1:{self.backend_port}/v2/sync-jobs/run',
                'SYNC_TASKS_OIDC_AUDIENCE': LOCAL_OIDC_AUDIENCE,
                'SYNC_TASKS_INVOKER_SA': LOCAL_INVOKER_SA,
                'SYNC_TASKS_MAX_ATTEMPTS': '2',
                'HTTP_SYNC_JOBS_RUN_TIMEOUT': '30',
                'FAIR_USE_ENABLED': 'false',
                'TRIAL_PAYWALL_ENABLED': 'false',
                'STT_PRERECORDED_MODEL': 'parakeet',
                'STT_SERVICE_MODELS': 'parakeet',
                'HOSTED_PARAKEET_API_URL': 'http://127.0.0.1:1',
                'BUCKET_TEMPORAL_SYNC_LOCAL': 'sync-temporal',
                'BUCKET_SPEECH_PROFILES': 'speech-profiles',
                'BUCKET_POSTPROCESSING': 'postprocessing',
                'BUCKET_PRIVATE_CLOUD_SYNC': 'omi-private-cloud-sync',
                'BUCKET_MEMORIES_RECORDINGS': 'memories-recordings',
                'BUCKET_APP_THUMBNAILS': 'app-thumbnails',
                'BUCKET_CHAT_FILES': 'chat-files',
                'BUCKET_DESKTOP_UPDATES': 'desktop-updates',
                'STRIPE_SECRET_KEY': '',
                'OMI_SYNC_STACK_STATE_DIR': str(self.state_dir),
                'OMI_SYNC_STACK_STORAGE_DIR': str(self.storage_dir),
                'OMI_SYNC_STACK_CONTROL_TOKEN': self.control_token,
                'OMI_SYNC_STACK_SENSITIVE_UID': SENSITIVE_UID,
                'OMI_SYNC_STACK_TRANSCRIPT_TOKEN': TRANSCRIPT_TOKEN,
                'PYTHONPATH': str(BACKEND),
            }
        )
        return env

    @property
    def base_url(self) -> str:
        return f'http://127.0.0.1:{self.backend_port}'

    @property
    def control_headers(self) -> dict[str, str]:
        return {'X-Omi-Sync-Stack-Control': self.control_token}

    def _start(self, name: str, command: list[str], *, extra_env: dict[str, str] | None = None) -> Child:
        if name in self.children:
            raise StackFailure(f'{name} is already running')
        log_path = self.logs_dir / f'{name}.log'
        process_env = self.env.copy()
        if extra_env:
            process_env.update(extra_env)
        output = log_path.open('wb')
        process = subprocess.Popen(
            command,
            cwd=BACKEND,
            env=process_env,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        child = Child(name=name, process=process, log_path=log_path)
        self.children[name] = child
        return child

    def start(self) -> None:
        redis_binary = shutil.which('redis-server')
        if not redis_binary:
            raise StackFailure('redis-server is required; install Redis and retry')
        self._start(
            'redis',
            [
                redis_binary,
                '--bind',
                '127.0.0.1',
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
            'backend',
            [
                str(PYTHON),
                '-m',
                'uvicorn',
                'testing.sync_cloud_tasks_stack.app:app',
                '--host',
                '127.0.0.1',
                '--port',
                str(self.backend_port),
            ],
        )
        _wait_for_port(self.backend_port, label='sync backend', timeout=45.0)

        def healthy() -> bool:
            try:
                response = self.http.get(f'{self.base_url}/__sync-stack/health', timeout=1.0)
                return response.status_code == 200 and response.json().get('status') == 'ok'
            except (httpx.HTTPError, ValueError):
                return False

        _wait_until(healthy, label='sync stack health endpoint', timeout=20.0)

    def stop(self, name: str) -> None:
        child = self.children.pop(name, None)
        if child is None or child.process.poll() is not None:
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
        self.http.close()

    def cleanup_workspace_files(self) -> None:
        sync_root = (BACKEND / 'syncing').resolve()
        for uid in self.created_uids:
            target = (sync_root / uid).resolve()
            if target.parent != sync_root or not uid.startswith(SENSITIVE_UID):
                raise StackFailure(f'refusing unexpected sync cleanup target: {target}')
            if target.exists():
                shutil.rmtree(target)

    def seed_user(self, uid: str) -> None:
        self.created_uids.add(uid)
        self.firestore.collection('users').document(uid).set(
            {
                'id': uid,
                'language': 'en',
                'private_cloud_sync_enabled': False,
                'data_protection_level': 'enhanced',
                'transcription_preferences': {'uses_custom_stt': False},
            }
        )

    def evidence_events(self, stream: str) -> list[dict[str, Any]]:
        return _read_events(self.evidence_dir / f'{stream}.jsonl')

    def task_control(self, task_id: str) -> dict[str, Any]:
        response = self.http.get(
            f'{self.base_url}/__sync-stack/tasks/{task_id}',
            headers=self.control_headers,
            timeout=5.0,
        )
        if response.status_code != 200:
            raise StackFailure(f'captured task {task_id} was unavailable: HTTP {response.status_code}')
        payload = response.json()
        if not isinstance(payload, dict):
            raise StackFailure('captured task control endpoint returned a non-object')
        return payload

    def staged_blob_path(self, blob_path: str) -> Path:
        relative = PurePosixPath(blob_path)
        if relative.is_absolute() or '..' in relative.parts:
            raise StackFailure('task yielded an unsafe staged blob path')
        return self.storage_dir / 'sync-temporal' / Path(*relative.parts)


def _pcm16_upload_bytes() -> bytes:
    """One second of framed PCM16 audio accepted by the production v2 decoder."""
    frame = struct.pack('<h', 1200) * 1600  # 100 ms at 16 kHz mono PCM16
    return b''.join(struct.pack('<I', len(frame)) + frame for _ in range(10))


def _fresh_pcm_filename() -> str:
    timestamp = int(time.time()) - 60
    return f'audio_omi_pcm16_16000_1_fs160_{timestamp}.bin'


def _record_job_event(stack: Stack, event: dict[str, Any]) -> None:
    """Write runner-side job metadata through the evidence sanitizer."""
    from testing.sync_cloud_tasks_stack.events import write_event

    write_event('jobs', event)


def _submit_and_capture_task(stack: Stack, uid: str) -> tuple[str, dict[str, Any]]:
    stack.seed_user(uid)
    filename = _fresh_pcm_filename()
    audio = _pcm16_upload_bytes()
    capture_conversation_id = f'sync-stack-capture-{uuid.uuid4().hex}'
    _seed_capture_conversation(stack, uid, capture_conversation_id)
    manifest_response = stack.http.post(
        f'{stack.base_url}/v2/sync-capture-manifest',
        json={
            'conversation_id': capture_conversation_id,
            'files': [{'name': filename, 'sha256': hashlib.sha256(audio).hexdigest()}],
        },
        headers={
            'Authorization': f'Bearer {ADMIN_KEY}{uid}',
            'X-App-Platform': 'ios',
            'X-Device-Id-Hash': DEVICE_HASH,
        },
        timeout=10.0,
    )
    if manifest_response.status_code != 200:
        raise StackFailure(f'fresh capture manifest returned HTTP {manifest_response.status_code}')
    manifest_body = manifest_response.json()
    manifest = manifest_body.get('manifest') if isinstance(manifest_body, dict) else None
    if not isinstance(manifest, str) or not manifest:
        raise StackFailure('fresh capture manifest response was missing its signed manifest')

    response = stack.http.post(
        f'{stack.base_url}/v2/sync-local-files?conversation_id={capture_conversation_id}',
        files=[('files', (filename, audio, 'application/octet-stream'))],
        headers={
            'Authorization': f'Bearer {ADMIN_KEY}{uid}',
            'X-App-Platform': 'ios',
            'X-Device-Id-Hash': DEVICE_HASH,
            'X-Omi-Sync-Capture-Manifest': manifest,
        },
        timeout=20.0,
    )
    if response.status_code != 202:
        raise StackFailure(f'sync admission returned HTTP {response.status_code}')
    admitted = response.json()
    job_id = admitted.get('job_id') if isinstance(admitted, dict) else None
    if not isinstance(job_id, str) or admitted.get('status') != 'queued' or admitted.get('lane') != 'fresh':
        raise StackFailure('sync admission did not return a queued fresh job')

    captured: dict[str, Any] = {}

    def task_captured() -> bool:
        nonlocal captured
        try:
            captured = stack.task_control(job_id)
        except (httpx.HTTPError, StackFailure):
            return False
        return isinstance(captured.get('body'), dict)

    _wait_until(task_captured, label=f'Cloud Tasks capture for job {job_id}', timeout=10.0)
    _assert_task_contract(stack, job_id, uid, capture_conversation_id, captured)
    _assert_unauthenticated_delivery_is_rejected(stack, uid, job_id, captured)
    return job_id, captured


def _seed_capture_conversation(stack: Stack, uid: str, conversation_id: str) -> None:
    """Seed only the existing server-capture provenance required by fresh v2."""
    # A raw transcript-segment list is not a real enhanced-protection
    # Firestore shape.  Use the same public encoder the production write path
    # uses so the merge/reprocess branch reads an authentic empty capture.
    from database import conversations as conversations_db

    now = datetime.now(timezone.utc)
    capture = {
        'id': conversation_id,
        'created_at': now - timedelta(seconds=90),
        'started_at': now - timedelta(seconds=120),
        'finished_at': now,
        'source': 'omi',
        'language': 'en',
        'structured': {
            'title': 'Local capture provenance',
            'overview': '',
            'emoji': '🧪',
            'category': 'other',
            'action_items': [],
            'events': [],
        },
        'transcript_segments': [],
        'private_cloud_sync_enabled': False,
        'status': 'completed',
        'discarded': False,
        'is_locked': False,
        'data_protection_level': 'enhanced',
        'client_device_id': DEVICE_ID,
        'client_platform': 'ios',
    }
    encoded_capture = conversations_db.encode_conversation_for_write(uid, capture, level='enhanced')
    if isinstance(encoded_capture.get('transcript_segments'), list):
        raise StackFailure('capture provenance was not encoded like an enhanced production conversation')
    stack.firestore.collection('users').document(uid).collection('conversations').document(conversation_id).set(
        encoded_capture
    )


def _assert_task_contract(
    stack: Stack,
    job_id: str,
    uid: str,
    capture_conversation_id: str,
    captured: dict[str, Any],
) -> None:
    body = captured.get('body')
    if not isinstance(body, dict):
        raise StackFailure('captured task body is missing')
    expected_keys = {
        'schema_version',
        'job_id',
        'uid',
        'raw_blob_paths',
        'source',
        'should_lock',
        'conversation_id',
        'lane',
        'content_id',
        'ledger_fence_mode',
        'client_device_id',
        'client_platform',
        'capture_time_trust',
        'recording_age_seconds',
    }
    if (
        body.get('schema_version') != 1
        or body.get('job_id') != job_id
        or body.get('uid') != uid
        or body.get('conversation_id') != capture_conversation_id
        or body.get('client_device_id') != DEVICE_ID
        or body.get('client_platform') != 'ios'
        or body.get('lane') != 'fresh'
        or body.get('capture_time_trust') != 'device_bound'
        or body.get('ledger_fence_mode') != 'active'
        or not expected_keys.issubset(body)
    ):
        raise StackFailure('captured task body did not preserve the v2 Sync worker contract')
    recording_age_seconds = body.get('recording_age_seconds')
    if (
        not isinstance(recording_age_seconds, int)
        or isinstance(recording_age_seconds, bool)
        or not 0 <= recording_age_seconds <= 5 * 60
    ):
        raise StackFailure('captured fresh task did not preserve a recent device-bound recording age')
    raw_paths = body.get('raw_blob_paths')
    if not isinstance(raw_paths, list) or len(raw_paths) != 1 or not isinstance(raw_paths[0], str):
        raise StackFailure('captured task did not contain exactly one staged raw blob path')
    if not stack.staged_blob_path(raw_paths[0]).is_file():
        raise StackFailure('v2 admission did not stage the raw PCM before Cloud Tasks capture')

    events = [
        event
        for event in stack.evidence_events('tasks')
        if event.get('event') == 'task_captured' and event.get('task_id') == job_id
    ]
    if len(events) != 1:
        raise StackFailure(f'expected one task evidence event for {job_id}, saw {len(events)}')
    event = events[0]
    if (
        event.get('queue') != SYNC_QUEUE
        or event.get('http_method') != 'POST'
        or event.get('url_loopback') is not True
        or event.get('url_path') != '/v2/sync-jobs/run'
        or event.get('oidc_audience') != LOCAL_OIDC_AUDIENCE
        or event.get('oidc_service_account') != LOCAL_INVOKER_SA
        or event.get('payload_schema_version') != 1
        or event.get('raw_blob_count') != 1
        or event.get('dispatch_deadline_seconds') != 1500
    ):
        raise StackFailure('real Cloud Tasks task construction did not match queue/OIDC/schema expectations')


def _deliver_task(stack: Stack, task: dict[str, Any], *, retry_count: int) -> httpx.Response:
    body = task.get('body')
    url = task.get('url')
    if not isinstance(body, dict) or not isinstance(url, str):
        raise StackFailure('task control response is malformed')
    response = stack.http.post(
        url,
        json=body,
        headers={
            'Authorization': f'Bearer {LOCAL_OIDC_TOKEN}',
            'X-CloudTasks-TaskRetryCount': str(retry_count),
        },
        timeout=35.0,
    )
    _record_job_event(
        stack,
        {
            'event': 'worker_delivery',
            'task_id': task.get('task_id'),
            'retry_count': retry_count,
            'http_status': response.status_code,
        },
    )
    return response


def _assert_unauthenticated_delivery_is_rejected(stack: Stack, uid: str, job_id: str, task: dict[str, Any]) -> None:
    """Exercise the production Cloud Tasks dependency before any valid delivery."""
    body = task.get('body')
    url = task.get('url')
    if not isinstance(body, dict) or not isinstance(url, str):
        raise StackFailure('task control response is malformed before auth-boundary check')
    denied = stack.http.post(url, json=body, timeout=10.0)
    if denied.status_code != 403:
        raise StackFailure(f'unauthenticated Cloud Tasks delivery returned HTTP {denied.status_code}, expected 403')
    status = stack.http.get(
        f'{stack.base_url}/v2/sync-local-files/{job_id}',
        headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
        timeout=5.0,
    )
    if status.status_code != 200 or status.json().get('status') != 'queued':
        raise StackFailure('rejected Cloud Tasks delivery changed the queued Sync job state')


def _poll_terminal_job(stack: Stack, uid: str, job_id: str, *, timeout: float = 20.0) -> dict[str, Any]:
    result: dict[str, Any] = {}

    def terminal() -> bool:
        nonlocal result
        response = stack.http.get(
            f'{stack.base_url}/v2/sync-local-files/{job_id}',
            headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
            timeout=5.0,
        )
        if response.status_code != 200:
            raise StackFailure(f'status poll returned HTTP {response.status_code}')
        body = response.json()
        if not isinstance(body, dict):
            raise StackFailure('status poll returned a non-object')
        result = body
        return body.get('status') in {'completed', 'partial_failure', 'failed'}

    _wait_until(terminal, label=f'terminal Sync job {job_id}', timeout=timeout)
    return result


def _assert_durable_success(stack: Stack, uid: str, job_id: str) -> str:
    status = _poll_terminal_job(stack, uid, job_id)
    result = status.get('result')
    if status.get('status') != 'completed' or not isinstance(result, dict):
        raise StackFailure(f'Sync worker did not publish a durable completed result for job {job_id}')
    new_conversations = result.get('new_memories')
    updated_conversations = result.get('updated_memories')
    if not isinstance(new_conversations, list) or not isinstance(updated_conversations, list):
        raise StackFailure('Sync result did not expose its durable conversation ids')
    conversations = [*new_conversations, *updated_conversations]
    if len(conversations) != 1 or not isinstance(conversations[0], str):
        raise StackFailure('deterministic processor did not produce exactly one durable conversation id')
    conversation_id = conversations[0]
    conversation_response = stack.http.get(
        f'{stack.base_url}/v1/conversations/{conversation_id}',
        headers={'Authorization': f'Bearer {ADMIN_KEY}{uid}'},
        timeout=10.0,
    )
    if conversation_response.status_code != 200:
        raise StackFailure(f'durable conversation read returned HTTP {conversation_response.status_code}')
    conversation = conversation_response.json()
    segments = conversation.get('transcript_segments') if isinstance(conversation, dict) else None
    if (
        not isinstance(conversation, dict)
        or conversation.get('status') != 'completed'
        or not isinstance(segments, list)
        or len(segments) != 1
        or segments[0].get('text') != TRANSCRIPT_TOKEN
    ):
        raise StackFailure('durable conversation did not contain the deterministic provider result')

    task = stack.task_control(job_id)
    body = task['body']
    expected_conversation_id = body.get('conversation_id') if isinstance(body, dict) else None
    if not isinstance(expected_conversation_id, str) or conversation_id != expected_conversation_id:
        raise StackFailure('Sync worker did not merge/reprocess the conversation selected during fresh admission')
    content_id = body.get('content_id') if isinstance(body, dict) else None
    if not isinstance(content_id, str):
        raise StackFailure('task content id is missing')
    ledger = (
        stack.firestore.collection('users').document(uid).collection('sync_content_ledger').document(content_id).get()
    )
    ledger_data = ledger.to_dict() if ledger.exists else None
    if not isinstance(ledger_data, dict) or ledger_data.get('status') != 'completed':
        raise StackFailure('Firestore sync content ledger was not durably completed')
    _record_job_event(
        stack,
        {
            'event': 'durable_job_verified',
            'job_id': job_id,
            'status': status.get('status'),
            'conversation_id': conversation_id,
            'ledger_status': ledger_data.get('status'),
        },
    )
    return conversation_id


def _stt_invocation_count(stack: Stack, job_id: str) -> int:
    return sum(
        1
        for event in stack.evidence_events('providers')
        if event.get('event') == 'stt_completed' and event.get('job_id') == job_id
    )


def _first_delivery_failure_preserves_staging(stack: Stack) -> None:
    uid = f'{SENSITIVE_UID}-cleanup-retry'
    job_id, task = _submit_and_capture_task(stack, uid)
    raw_path = task['body']['raw_blob_paths'][0]
    staged_path = stack.staged_blob_path(raw_path)
    first = _deliver_task(stack, task, retry_count=0)
    if first.status_code != 500:
        raise StackFailure(f'intentional first worker cleanup failure returned HTTP {first.status_code}, expected 500')
    _assert_durable_success(stack, uid, job_id)
    if not staged_path.is_file():
        raise StackFailure('first delivery failure deleted staged task material before Cloud Tasks retry')
    if _stt_invocation_count(stack, job_id) != 1:
        raise StackFailure('first delivery ran the deterministic STT leaf more than once')

    retry = _deliver_task(stack, task, retry_count=1)
    if retry.status_code != 200 or retry.json().get('status') != 'acked':
        raise StackFailure('retry did not ACK the already-terminal job through the real worker route')
    if staged_path.exists():
        raise StackFailure('retry did not clean the preserved staged task material')
    if _stt_invocation_count(stack, job_id) != 1:
        raise StackFailure('terminal cleanup retry unexpectedly transcribed audio again')


def _duplicate_delivery_is_exactly_once(stack: Stack) -> None:
    uid = f'{SENSITIVE_UID}-duplicate-delivery'
    job_id, task = _submit_and_capture_task(stack, uid)
    first = _deliver_task(stack, task, retry_count=0)
    if first.status_code != 200 or first.json().get('status') != 'done':
        raise StackFailure('first Cloud Tasks delivery did not complete through the real worker route')
    _assert_durable_success(stack, uid, job_id)
    if _stt_invocation_count(stack, job_id) != 1:
        raise StackFailure('first successful delivery did not invoke STT exactly once')

    duplicate = _deliver_task(stack, task, retry_count=1)
    if duplicate.status_code != 200 or duplicate.json().get('status') != 'acked':
        raise StackFailure('duplicate Cloud Tasks delivery was not ACKed as terminal')
    if _stt_invocation_count(stack, job_id) != 1:
        raise StackFailure('duplicate Cloud Tasks delivery re-ran the provider pipeline')


def _assert_evidence_is_sanitized(stack: Stack) -> None:
    expected_streams = {'tasks', 'jobs', 'providers', 'worker'}
    present = {path.stem for path in stack.evidence_dir.glob('*.jsonl')}
    if not expected_streams.issubset(present):
        raise StackFailure(f'missing sanitized evidence streams: {sorted(expected_streams - present)}')
    for path in stack.evidence_dir.glob('*.jsonl'):
        content = path.read_text(encoding='utf-8')
        if SENSITIVE_UID in content or TRANSCRIPT_TOKEN in content:
            raise StackFailure(f'sensitive uid or transcript leaked into evidence: {path.name}')
        for line in content.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise StackFailure(f'non-JSON evidence line in {path.name}') from error
            if not isinstance(value, dict):
                raise StackFailure(f'non-object evidence event in {path.name}')


def run_scenarios(stack: Stack) -> None:
    _first_delivery_failure_preserves_staging(stack)
    _duplicate_delivery_is_exactly_once(stack)
    _assert_evidence_is_sanitized(stack)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, help='directory for sanitized evidence and private process logs')
    parser.add_argument('--keep', action='store_true', help='preserve the state directory after a successful run')
    args = parser.parse_args()
    if not PYTHON.exists():
        raise SystemExit(f'missing backend virtual environment: {PYTHON}; run backend/scripts/sync-python-deps.sh')

    state_dir = args.state_dir or Path(tempfile.mkdtemp(prefix='omi-sync-cloud-tasks-stack-'))
    state_dir.mkdir(parents=True, exist_ok=True)
    stack = Stack(state_dir)
    succeeded = False
    try:
        stack.start()
        run_scenarios(stack)
        succeeded = True
        print(f'sync Cloud Tasks stack gauntlet passed; sanitized evidence: {stack.evidence_dir}')
        return 0
    except Exception:
        print(f'sync Cloud Tasks stack gauntlet failed; state retained: {state_dir}', file=sys.stderr)
        raise
    finally:
        stack.close()
        if succeeded:
            stack.cleanup_workspace_files()
            if not args.keep:
                shutil.rmtree(state_dir)


if __name__ == '__main__':
    raise SystemExit(main())
