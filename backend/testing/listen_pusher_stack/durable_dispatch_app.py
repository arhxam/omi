"""Instrumented backend entrypoint for the local durable-finalization scenario.

The harness keeps the production REST route, Firestore outbox transaction,
Cloud Tasks task construction, task worker, Redis lease, and finalizer owner.
It replaces only remote Cloud Tasks/OIDC and provider leaves with loopback
test seams, so no local run can use developer credentials or send user data.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import HTTPException, Request
from google.api_core.exceptions import AlreadyExists

from models.conversation_enums import ConversationStatus
from utils import cloud_tasks
from utils.conversations import finalizer
from utils.conversations import lifecycle as lifecycle_service

LOCAL_TASK_TOKEN = os.getenv('LISTEN_FINALIZATION_LOCAL_TASK_TOKEN', 'omi-listen-pusher-stack-local-task')
TASK_EVENTS_FILENAME = 'finalization-tasks.jsonl'
WORKER_EVENTS_FILENAME = 'finalization-worker.jsonl'
_TASK_EVENTS_LOCK = Lock()


def _state_path(filename: str) -> Path | None:
    state_dir = os.getenv('OMI_STACK_STATE_DIR')
    return Path(state_dir) / filename if state_dir else None


def _record(filename: str, event: dict[str, Any]) -> None:
    path = _state_path(filename)
    if path is None:
        return
    with path.open('a', encoding='utf-8') as output:
        output.write(json.dumps(event, sort_keys=True) + '\n')


class _LocalCloudTasksClient:
    """Records the real task proto at the external Cloud Tasks boundary."""

    @staticmethod
    def queue_path(project: str, location: str, queue: str) -> str:
        return f'projects/{project}/locations/{location}/queues/{queue}'

    @staticmethod
    def task_path(project: str, location: str, queue: str, task_id: str) -> str:
        return f'projects/{project}/locations/{location}/queues/{queue}/tasks/{task_id}'

    def create_task(self, *, parent: str, task: Any, **_kwargs: Any) -> Any:
        http_request = task.http_request
        try:
            payload = json.loads(bytes(http_request.body).decode('utf-8'))
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError('local Cloud Tasks client received an invalid task payload') from error
        if not isinstance(payload, dict):
            raise ValueError('local Cloud Tasks client received a non-object task payload')
        task_name = str(task.name)
        # Several REST retries can reach this boundary concurrently. Keep the
        # local recorder atomic too, otherwise two test threads could both
        # observe an empty JSONL file and falsely record two named tasks.
        with _TASK_EVENTS_LOCK:
            if any(event.get('task_name') == task_name for event in _read_task_events()):
                # Match Cloud Tasks named-task deduplication across backend
                # restarts. The production dispatcher catches this exception
                # and treats an uncertain duplicate handoff as a safe success.
                _record(
                    TASK_EVENTS_FILENAME,
                    {
                        'event': 'task_already_exists',
                        'parent': parent,
                        'task_name': task_name,
                        'payload': payload,
                    },
                )
                raise AlreadyExists(f'local task already exists: {task_name}')
            _record(
                TASK_EVENTS_FILENAME,
                {
                    'event': 'task_created',
                    'parent': parent,
                    'task_name': task_name,
                    'url': str(http_request.url),
                    'payload': payload,
                    'headers': dict(http_request.headers),
                    'oidc_audience': str(http_request.oidc_token.audience),
                    'oidc_service_account': str(http_request.oidc_token.service_account_email),
                    'dispatch_deadline_seconds': int(task.dispatch_deadline.seconds),
                },
            )
        return task


def _read_task_events() -> list[dict[str, Any]]:
    path = _state_path(TASK_EVENTS_FILENAME)
    if path is None or not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding='utf-8').splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _verify_local_task(request: Request, *, audience: str, invoker_sa: str, **_kwargs: Any) -> int:
    """Replace remote JWKS lookup while preserving the worker's auth boundary."""
    expected_audience = os.getenv('LISTEN_FINALIZATION_TASKS_HANDLER_URL', '')
    expected_invoker = os.getenv('LISTEN_FINALIZATION_TASKS_INVOKER_SA', '')
    if audience != expected_audience or invoker_sa != expected_invoker:
        raise HTTPException(status_code=403, detail='Local task route has unexpected configured identity')
    if request.headers.get('authorization') != f'Bearer {LOCAL_TASK_TOKEN}':
        raise HTTPException(status_code=403, detail='Invalid local task identity')
    try:
        return int(request.headers.get('x-cloudtasks-taskretrycount', '0'))
    except ValueError:
        return 0


def _offline_process_conversation(uid: str, _language: str, conversation: Any, **kwargs: Any) -> Any:
    """Persist a deterministic completed result through the production owner."""
    conversation.status = ConversationStatus.completed
    persisted = lifecycle_service.persist_processed_conversation(uid, conversation.model_dump())
    _record(
        WORKER_EVENTS_FILENAME,
        {
            'event': 'process_completed',
            'persisted': bool(persisted),
            'force_process': kwargs.get('force_process'),
            'defer_memory_extraction': kwargs.get('defer_memory_extraction'),
        },
    )
    return conversation


def _offline_extract_memories(_uid: str, _conversation: Any) -> None:
    _record(WORKER_EVENTS_FILENAME, {'event': 'memory_extraction_skipped'})


async def _offline_trigger_integrations(_uid: str, _conversation: Any, *, idempotency_key: str, **_kwargs: Any) -> None:
    _record(
        WORKER_EVENTS_FILENAME, {'event': 'integration_fanout_skipped', 'fanout_key_present': bool(idempotency_key)}
    )


# Replace external leaves before main imports the task worker and routers.
cloud_tasks._tasks_client = _LocalCloudTasksClient()
cloud_tasks._verify_cloud_tasks_oidc = _verify_local_task
finalizer.process_conversation = _offline_process_conversation
finalizer.extract_memories = _offline_extract_memories
finalizer.trigger_external_integrations = _offline_trigger_integrations

from main import app  # noqa: E402  (install harness seams before importing the ASGI app)
