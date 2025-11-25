import asyncio
from pathlib import Path

from matterstack.core.backend import JobState
from matterstack.core.workflow import Task
from matterstack.runtime.backends.local import LocalBackend
from matterstack.orchestration.api import run_task_async
from matterstack.config.profiles import load_profile


def _write_local_profile_config(tmp_path: Path, dry_run: bool) -> Path:
    """Helper to write a simple local profile config and return its path."""

    config_path = tmp_path / "matterstack.yaml"
    config_path.write_text(
        f"""profiles:
  local_test:
    type: local
    workspace_root: ./tmp_results
    dry_run: {str(dry_run).lower()}
"""
    )
    return config_path


def test_load_profile_from_explicit_config(tmp_path):
    config_path = _write_local_profile_config(tmp_path, dry_run=True)

    profile = load_profile("local_test", config_path=str(config_path))

    assert profile.name == "local_test"
    assert profile.type == "local"
    assert profile.local is not None
    assert profile.local.workspace_root == "./tmp_results"
    assert profile.local.dry_run is True


def test_local_profile_creates_local_backend(tmp_path):
    config_path = _write_local_profile_config(tmp_path, dry_run=True)
    profile = load_profile("local_test", config_path=str(config_path))

    backend = profile.create_backend()

    assert isinstance(backend, LocalBackend)
    assert backend.dry_run is True
    # LocalBackend resolves workspace_root to an absolute Path
    assert str(backend.workspace_root).endswith("tmp_results")


def test_run_task_async_with_profile(tmp_path):
    # For this test we want a real execution to capture logs, so dry_run=False.
    config_path = _write_local_profile_config(tmp_path, dry_run=False)

    task = Task(
        image="local",
        command='echo "hello"',
        files={},
        env={},
    )

    result = asyncio.run(
        run_task_async(
            task,
            backend=None,
            profile="local_test",
            config_path=str(config_path),
        )
    )

    assert result.status.state == JobState.COMPLETED
    assert "hello" in result.logs.stdout
    assert result.workspace_path.exists()
    assert result.workspace_path.is_dir()