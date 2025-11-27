import asyncio

from matterstack.core.backend import JobState
from matterstack.core.workflow import Task
from matterstack.runtime.backends.local import LocalBackend
from matterstack.orchestration.api import run_task_async


def test_run_task_async_with_local_backend(tmp_path):
    """Basic integration test for run_task_async using LocalBackend."""
    workspace_root = tmp_path / "orchestration_workspace"
    backend = LocalBackend(workspace_root=str(workspace_root))

    task = Task(
        image="local",  # Ignored by LocalBackend, but required by Task
        command='echo "hello"',
        files={},
        env={},
    )

    result = asyncio.run(run_task_async(task, backend))

    assert result.status.state == JobState.COMPLETED_OK
    assert "hello" in result.logs.stdout

    # Workspace path should point to the per-job directory and exist
    assert result.workspace_path.exists()
    assert result.workspace_path.is_dir()