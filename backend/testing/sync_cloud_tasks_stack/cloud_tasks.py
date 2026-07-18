"""Local Cloud Tasks control-plane recorder for the Sync gauntlet."""

from __future__ import annotations

import copy
import json
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from .events import write_event


@dataclass(frozen=True)
class CapturedTask:
    task_id: str
    body: dict[str, Any]
    url: str


class CloudTasksRecorder:
    """Implements the tiny Cloud Tasks client surface Sync uses in production.

    ``utils.cloud_tasks`` still constructs the real ``tasks_v2.Task`` proto.
    This recorder validates and captures that proto at the SDK boundary without
    writing its sensitive payload to disk.
    """

    def __init__(self):
        self._tasks: dict[str, CapturedTask] = {}
        self._lock = threading.Lock()

    @staticmethod
    def queue_path(project: str, location: str, queue: str) -> str:
        return f'projects/{project}/locations/{location}/queues/{queue}'

    @staticmethod
    def task_path(project: str, location: str, queue: str, task: str) -> str:
        return f'projects/{project}/locations/{location}/queues/{queue}/tasks/{task}'

    def create_task(self, *, parent: str, task: tasks_v2.Task, **_kwargs: Any) -> tasks_v2.Task:
        if not isinstance(task, tasks_v2.Task):
            raise TypeError(f'expected tasks_v2.Task, got {type(task).__name__}')
        task_id = task.name.rsplit('/', 1)[-1]
        if not task_id:
            raise ValueError('Cloud Tasks task name is missing its task id')
        try:
            body = json.loads(bytes(task.http_request.body).decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError('Sync task body is not valid JSON') from error
        if not isinstance(body, dict):
            raise ValueError('Sync task body must be a JSON object')

        with self._lock:
            if task_id in self._tasks:
                write_event('tasks', {'event': 'named_task_deduplicated', 'task_id': task_id})
                raise AlreadyExists(f'task {task_id} already exists')
            self._tasks[task_id] = CapturedTask(task_id=task_id, body=body, url=task.http_request.url)

        parsed_url = urlparse(task.http_request.url)
        expected_post = int(task.http_request.http_method) == int(tasks_v2.HttpMethod.POST)
        queue = parent.rsplit('/', 1)[-1]
        raw_paths = body.get('raw_blob_paths')
        write_event(
            'tasks',
            {
                'event': 'task_captured',
                'task_id': task_id,
                'queue': queue,
                'http_method': 'POST' if expected_post else 'unexpected',
                'url_loopback': parsed_url.hostname in {'127.0.0.1', 'localhost'},
                'url_path': parsed_url.path,
                'oidc_audience': task.http_request.oidc_token.audience,
                'oidc_service_account': task.http_request.oidc_token.service_account_email,
                'payload_schema_version': body.get('schema_version'),
                'payload_keys': sorted(body.keys()),
                'raw_blob_count': len(raw_paths) if isinstance(raw_paths, list) else -1,
                'dispatch_deadline_seconds': int(task.dispatch_deadline.seconds),
            },
        )
        return task

    def task(self, task_id: str) -> CapturedTask | None:
        with self._lock:
            captured = self._tasks.get(task_id)
            return copy.deepcopy(captured) if captured is not None else None
