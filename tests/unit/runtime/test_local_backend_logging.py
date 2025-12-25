import asyncio

import pytest

from matterstack.core.backend import JobState
from matterstack.core.workflow import Task
from matterstack.runtime.backends.local import LocalBackend


@pytest.mark.asyncio
async def test_local_backend_logging_creation(tmp_path):
    """
    Verifies that LocalBackend creates stdout.log and stderr.log files
    in the task directory, which are accessible to the user.
    """
    workspace_root = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace_root))

    task_id = "test_logging_task"
    task = Task(
        task_id=task_id,
        command="echo 'Hello Logging'",
        files={},
        env={},
        image="ubuntu:latest"  # Required by Task definition
    )

    # Submit task
    await backend.submit(task)

    # Wait for completion
    for _ in range(20):
        status = await backend.poll(task_id)
        if status.state in [JobState.COMPLETED_OK, JobState.COMPLETED_ERROR, JobState.CANCELLED]:
            break
        await asyncio.sleep(0.1)

    assert status.state == JobState.COMPLETED_OK

    # Check physical files existence
    task_dir = workspace_root / task_id
    stdout_path = task_dir / "stdout.log"
    stderr_path = task_dir / "stderr.log"

    assert stdout_path.exists(), "stdout.log should be created"
    assert stderr_path.exists(), "stderr.log should be created"

    content = stdout_path.read_text().strip()
    assert content == "Hello Logging"

    # Check get_logs method
    logs = await backend.get_logs(task_id)
    assert logs["stdout"].strip() == "Hello Logging"
    assert logs["stderr"] == ""

@pytest.mark.asyncio
async def test_local_backend_stderr_capture(tmp_path):
    """
    Verifies that stderr is captured correctly.
    """
    workspace_root = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace_root))

    task_id = "test_stderr_task"
    # Command that writes to stderr
    task = Task(
        task_id=task_id,
        command="echo 'Error Message' >&2",
        files={},
        env={},
        image="ubuntu:latest"
    )

    await backend.submit(task)

    # Wait for completion
    for _ in range(20):
        status = await backend.poll(task_id)
        if status.state in [JobState.COMPLETED_OK, JobState.COMPLETED_ERROR]:
            break
        await asyncio.sleep(0.1)

    assert status.state == JobState.COMPLETED_OK

    # Check logs
    task_dir = workspace_root / task_id
    stderr_path = task_dir / "stderr.log"

    assert stderr_path.exists()
    content = stderr_path.read_text().strip()
    assert content == "Error Message"
