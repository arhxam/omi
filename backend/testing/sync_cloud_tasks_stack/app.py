"""Instrumented ASGI entrypoint for the local Sync Cloud Tasks gauntlet.

This module patches only service/provider leaves before importing ``main``.
The production FastAPI app, Sync admission route, task builder, Redis/Firestore
job state, PCM decoder, and Cloud Tasks worker route remain unchanged.
"""

from __future__ import annotations

import os
import socket
import uuid
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import google.auth
from google.auth import credentials as google_auth_credentials
from google.oauth2 import id_token
from fastapi import Header, HTTPException, Request

from .cloud_tasks import CloudTasksRecorder
from .events import write_event
from .storage import configure_storage_dir, patch_google_storage

_LOOPBACK_HOSTS = frozenset({'127.0.0.1', '::1', 'localhost', '0.0.0.0'})
_ORIGINAL_SOCKET_CONNECT = socket.socket.connect
_ORIGINAL_SOCKET_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_CREATE_CONNECTION = socket.create_connection
_ORIGINAL_GETADDRINFO = socket.getaddrinfo


def _network_host(address: Any) -> str | None:
    if isinstance(address, tuple) and address:
        return str(address[0])
    return None


def _assert_loopback(address: Any) -> None:
    host = _network_host(address)
    if host is None or host in _LOOPBACK_HOSTS:
        return
    raise RuntimeError(f'sync stack blocked outbound network connection to {host!r}')


def _guarded_connect(sock: socket.socket, address: Any) -> Any:
    _assert_loopback(address)
    return _ORIGINAL_SOCKET_CONNECT(sock, address)


def _guarded_connect_ex(sock: socket.socket, address: Any) -> int:
    _assert_loopback(address)
    return _ORIGINAL_SOCKET_CONNECT_EX(sock, address)


def _guarded_create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
    _assert_loopback(address)
    return _ORIGINAL_CREATE_CONNECTION(address, *args, **kwargs)


def _guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
    if host is not None and str(host) not in _LOOPBACK_HOSTS:
        raise RuntimeError(f'sync stack blocked outbound DNS lookup for {host!r}')
    return _ORIGINAL_GETADDRINFO(host, *args, **kwargs)


# Apply the no-egress guard before backend imports can instantiate a provider.
socket.socket.connect = _guarded_connect
socket.socket.connect_ex = _guarded_connect_ex
socket.create_connection = _guarded_create_connection
socket.getaddrinfo = _guarded_getaddrinfo


def _anonymous_google_credentials(
    *_args: Any, **_kwargs: Any
) -> tuple[google_auth_credentials.AnonymousCredentials, str]:
    project = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('FIREBASE_PROJECT_ID') or 'demo-omi-sync-stack'
    return google_auth_credentials.AnonymousCredentials(), project


# Firestore emulator traffic must never trigger ADC/metadata discovery.
google.auth.default = _anonymous_google_credentials


def _verify_local_oidc(token: str, _request: Any, *, audience: str | None = None, **_kwargs: Any) -> dict[str, Any]:
    """Validate the test token while leaving the production dependency intact."""
    expected_identity = os.getenv('SYNC_TASKS_INVOKER_SA', '')
    expected_token = f"local-sync-oidc:{expected_identity}"
    expected_audience = os.getenv('SYNC_TASKS_OIDC_AUDIENCE', '')
    if not expected_identity or token != expected_token or audience != expected_audience:
        raise ValueError('local Cloud Tasks OIDC verifier rejected the token')
    write_event('worker', {'event': 'local_oidc_verified', 'audience': audience, 'identity_verified': True})
    return {
        'email': expected_identity,
        'email_verified': True,
    }


# ``utils.cloud_tasks.verify_cloud_tasks_oidc`` still validates header shape,
# configured audience, service-account identity, and retry-count parsing.  This
# replaces only Google certificate/token verification at the external boundary.
id_token.verify_oauth2_token = _verify_local_oidc


_storage_root = os.getenv('OMI_SYNC_STACK_STORAGE_DIR', '').strip()
if not _storage_root:
    raise RuntimeError('OMI_SYNC_STACK_STORAGE_DIR is required')
configure_storage_dir(_storage_root)
patch_google_storage()


import utils.cloud_tasks as production_cloud_tasks  # noqa: E402
import utils.sync.pipeline as sync_pipeline  # noqa: E402
from models.conversation import Conversation  # noqa: E402
from models.conversation_enums import ConversationStatus  # noqa: E402
from models.structured import Structured  # noqa: E402
from utils.conversations import lifecycle as lifecycle_service  # noqa: E402

task_recorder = CloudTasksRecorder()
production_cloud_tasks._tasks_client = task_recorder


def _job_id_from_path(path: str) -> str:
    # Production paths are syncing/<uid>/<job-id>/<file>; evidence never gets
    # the path itself, only the opaque Cloud Tasks job id.
    return Path(path).parent.name


def _local_vad(path: str, *, return_segments: bool = False, **_kwargs: Any) -> Any:
    """External VAD leaf: prove the real PCM decoder produced readable WAV."""
    with wave.open(path, 'rb') as wav_file:
        duration = wav_file.getnframes() / float(wav_file.getframerate())
    segments = [{'start': 0.0, 'end': min(duration, 1.0)}] if duration >= 1.0 else []
    write_event(
        'providers',
        {
            'event': 'vad_result',
            'job_id': _job_id_from_path(path),
            'segment_count': len(segments),
        },
    )
    return segments if return_segments else bool(segments)


def _local_prerecorded(
    audio_url: str,
    *,
    return_language: bool = False,
    **_kwargs: Any,
) -> Any:
    """Deterministic STT leaf; no provider request or transcript evidence."""
    job_id = audio_url.rsplit('/', 1)[-1]
    write_event('providers', {'event': 'stt_completed', 'job_id': job_id, 'word_count': 1})
    words = [
        {
            'timestamp': [0.0, 1.0],
            'speaker': 'SPEAKER_00',
            'text': os.getenv('OMI_SYNC_STACK_TRANSCRIPT_TOKEN', 'sync-stack-transcript'),
        }
    ]
    return (words, 'en') if return_language else words


def _local_process_conversation(
    uid: str,
    language: str | None = None,
    conversation: Any = None,
    *,
    language_code: str | None = None,
    **_kwargs: Any,
) -> Conversation:
    """Deterministic LLM/process leaf with real lifecycle-backed persistence."""
    if conversation is None:
        raise ValueError('sync stack deterministic processor requires a conversation')
    effective_language = language_code or language or 'en'
    started_at = getattr(conversation, 'started_at', None) or datetime.now(timezone.utc)
    finished_at = getattr(conversation, 'finished_at', None) or started_at
    existing_id = getattr(conversation, 'id', None)
    completed = Conversation(
        id=existing_id if isinstance(existing_id, str) and existing_id else f'sync-stack-{uuid.uuid4().hex}',
        created_at=getattr(conversation, 'created_at', None) or started_at,
        started_at=started_at,
        finished_at=finished_at,
        source=getattr(conversation, 'source', None),
        language=effective_language,
        structured=Structured(
            title='Sync stack deterministic conversation',
            overview='Deterministic local processor result.',
            emoji='🧪',
            category='other',
        ),
        transcript_segments=list(getattr(conversation, 'transcript_segments', [])),
        private_cloud_sync_enabled=bool(getattr(conversation, 'private_cloud_sync_enabled', False)),
        status=ConversationStatus.completed,
        is_locked=bool(getattr(conversation, 'is_locked', False)),
        client_device_id=getattr(conversation, 'client_device_id', None),
        client_platform=getattr(conversation, 'client_platform', None),
    )
    if isinstance(existing_id, str) and existing_id:
        persisted = lifecycle_service.persist_processed_conversation(uid, completed.model_dump())
    else:
        lifecycle_service.persist_imported_conversation(uid, completed.model_dump())
        persisted = True
    if not persisted:
        raise RuntimeError('sync stack deterministic processor lost conversation lifecycle ownership')
    write_event('providers', {'event': 'conversation_persisted', 'conversation_id': completed.id})
    return completed


def _delete_segment_blob_immediately(path: str, *_args: Any, **_kwargs: Any) -> None:
    """The local STT leaf has consumed the segment synchronously; retain no audio."""
    sync_pipeline.delete_syncing_temporal_file(path)


# All replacements are external leaves.  Decode, VAD segmentation/cropping,
# job ledger, queue worker, status polling, and conversation DB writes remain
# production code.
sync_pipeline.vad_is_empty = _local_vad
sync_pipeline.get_prerecorded_service = lambda _language='en': ('parakeet', 'en', 'parakeet')
sync_pipeline.prerecorded = _local_prerecorded
sync_pipeline.process_conversation = _local_process_conversation
sync_pipeline.schedule_syncing_temporal_file_deletion = _delete_segment_blob_immediately
sync_pipeline.FAIR_USE_ENABLED = False


from routers import sync as sync_router  # noqa: E402

# The gauntlet isolates entitlement setup from the sync behavior under test.
sync_router.has_transcription_credits = lambda _uid: True
sync_router.FAIR_USE_ENABLED = False

_original_delete_staged_blobs_async = sync_router._delete_staged_blobs_async
_cleanup_fault_remaining = 1


async def _delete_staged_blobs_with_first_delivery_fault(blob_paths: list) -> None:
    """Force exactly one post-terminal cleanup retry while preserving the task input."""
    global _cleanup_fault_remaining
    if _cleanup_fault_remaining:
        _cleanup_fault_remaining -= 1
        write_event('jobs', {'event': 'intentional_cleanup_failure', 'blob_count': len(blob_paths)})
        raise RuntimeError('sync stack intentional staged-cleanup failure')
    await _original_delete_staged_blobs_async(blob_paths)


sync_router._delete_staged_blobs_async = _delete_staged_blobs_with_first_delivery_fault


from main import app  # noqa: E402


def _require_control(
    request: Request,
    control_token: str | None = Header(None, alias='X-Omi-Sync-Stack-Control'),
) -> None:
    client_host = request.client.host if request.client else None
    if client_host not in _LOOPBACK_HOSTS or control_token != os.getenv('OMI_SYNC_STACK_CONTROL_TOKEN'):
        raise HTTPException(status_code=403, detail='sync stack control is loopback-only')


@app.get('/__sync-stack/health', include_in_schema=False)
def sync_stack_health() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/__sync-stack/tasks/{task_id}', include_in_schema=False)
def sync_stack_task(
    task_id: str, request: Request, control_token: str | None = Header(None, alias='X-Omi-Sync-Stack-Control')
):
    _require_control(request, control_token)
    captured = task_recorder.task(task_id)
    if captured is None:
        raise HTTPException(status_code=404, detail='captured task not found')
    # This response is loopback-only control data, not evidence.  It lets the
    # runner send the exact production payload over a real worker HTTP request.
    return {'task_id': captured.task_id, 'url': captured.url, 'body': captured.body}
