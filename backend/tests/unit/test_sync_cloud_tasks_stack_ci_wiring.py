"""Wiring contract for the blocking Sync Cloud Tasks stack gauntlet."""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


def test_sync_cloud_tasks_stack_gauntlet_has_a_deterministic_hermetic_ci_job() -> None:
    workflow = (_REPO_ROOT / '.github' / 'workflows' / 'backend-hermetic-e2e.yml').read_text(encoding='utf-8')
    package = json.loads((_REPO_ROOT / 'package.json').read_text(encoding='utf-8'))
    contracts = json.loads((_REPO_ROOT / 'backend' / 'testing' / 'workflow_contracts.json').read_text(encoding='utf-8'))

    assert "- 'package.json'" in workflow
    assert "- 'package-lock.json'" in workflow
    assert '  sync-cloud-tasks-stack-gauntlet:' in workflow
    job = workflow.split('  sync-cloud-tasks-stack-gauntlet:\n', 1)[1]

    assert 'timeout-minutes: 20' in job
    assert 'uses: actions/setup-python@v6' in job
    assert 'uses: astral-sh/setup-uv@ecd24dd710f2fb0dca1693a67af11fc4a5c5ec84' in job
    assert 'uv venv .venv' in job
    assert 'uv pip sync pylock.toml --python .venv/bin/python' in job
    assert 'uses: actions/setup-node@v7' in job
    assert "node-version: '22'" in job
    assert 'cache-dependency-path: package-lock.json' in job
    assert 'npm ci --ignore-scripts' in job
    assert 'uses: actions/setup-java@v5' in job
    assert "java-version: '21'" in job
    assert 'sudo apt-get install --yes redis-server' in job
    assert 'npm run test:sync-cloud-tasks-stack:emulator' in job

    assert package['scripts']['test:sync-cloud-tasks-stack:emulator'] == 'backend/testing/sync_cloud_tasks_stack/run.sh'
    sync_contract = next(contract for contract in contracts['workflows'] if contract['id'] == 'sync_cloud_tasks')
    assert {
        'backend/routers/sync.py',
        'backend/utils/sync/pipeline.py',
        'backend/database/sync_jobs.py',
        'backend/testing/sync_cloud_tasks_stack/**',
    }.issubset(sync_contract['sources'])
    assert 'tests/unit/test_sync_cloud_tasks_stack_ci_wiring.py' in sync_contract['tests']

    runner = (_REPO_ROOT / 'backend' / 'testing' / 'sync_cloud_tasks_stack' / 'run.py').read_text(encoding='utf-8')
    entrypoint = (_REPO_ROOT / 'backend' / 'testing' / 'sync_cloud_tasks_stack' / 'app.py').read_text(encoding='utf-8')
    readme = (_REPO_ROOT / 'backend' / 'testing' / 'sync_cloud_tasks_stack' / 'README.md').read_text(encoding='utf-8')

    # Static wiring tripwire: the emulator run is the behavioral proof. These
    # assertions make removal of its fresh admission/retry boundary visible to
    # focused unit selection before CI.
    assert '_first_delivery_failure_preserves_staging' in runner
    assert '_duplicate_delivery_is_exactly_once' in runner
    assert '_assert_unauthenticated_delivery_is_rejected' in runner
    assert "body.get('capture_time_trust') != 'device_bound'" in runner
    assert 'conversation_id != expected_conversation_id' in runner
    assert "event.get('oidc_service_account') != LOCAL_INVOKER_SA" in runner
    assert 'sync-capture-manifest' in runner
    assert 'CloudTasksRecorder' in entrypoint
    assert 'multipart PCM v2 upload' in readme
