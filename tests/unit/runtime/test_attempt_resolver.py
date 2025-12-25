"""
Tests for the attempt resolver module.
"""

from pathlib import Path
from unittest.mock import MagicMock

from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.runtime.operators._attempt_resolver import (
    AttemptContext,
    get_or_create_store,
    resolve_attempt_context,
    resolve_attempt_id,
)


class TestAttemptContext:
    """Tests for AttemptContext dataclass."""

    def test_is_attempt_aware_true_when_attempt_id_present(self):
        ctx = AttemptContext(
            attempt_id="attempt-123",
            store=None,
            full_path=Path("/some/path"),
            relative_path=Path("tasks/t1/attempts/attempt-123"),
            operator_uuid=None,
        )
        assert ctx.is_attempt_aware is True

    def test_is_attempt_aware_false_when_attempt_id_none(self):
        ctx = AttemptContext(
            attempt_id=None,
            store=None,
            full_path=Path("/some/path"),
            relative_path=Path("operators/hpc/uuid-123"),
            operator_uuid="uuid-123",
        )
        assert ctx.is_attempt_aware is False


def _create_seeded_store(tmp_path, with_attempt: bool = False):
    """Helper to create a seeded store with run, task, and optionally an attempt."""
    from matterstack.storage.state_store import SQLiteStateStore

    run_root = tmp_path / "runs" / "run-1"
    run_root.mkdir(parents=True)

    handle = RunHandle(workspace_slug="test-ws", run_id="run-1", root_path=run_root)
    store = SQLiteStateStore(handle.db_path)
    store.create_run(handle)

    # Add workflow with task
    wf = Workflow()
    task = Task(task_id="task-1", image="ubuntu", command="echo hello")
    wf.add_task(task)
    store.add_workflow(wf, handle.run_id)

    attempt_id = None
    if with_attempt:
        attempt_id = store.create_attempt(
            run_id=handle.run_id,
            task_id="task-1",
            operator_type="HPC",
        )

    return store, handle, attempt_id


class TestResolveAttemptId:
    """Tests for resolve_attempt_id function."""

    def test_returns_attempt_id_when_found(self, tmp_path):
        """Successful resolution returns attempt_id and store."""
        store, handle, expected_attempt_id = _create_seeded_store(tmp_path, with_attempt=True)

        # Resolve
        attempt_id, returned_store = resolve_attempt_id(handle.db_path, "task-1")

        assert attempt_id == expected_attempt_id
        assert returned_store is not None

    def test_returns_none_when_no_attempt(self, tmp_path):
        """Returns None for attempt_id when no attempt exists."""
        store, handle, _ = _create_seeded_store(tmp_path, with_attempt=False)

        # Resolve
        attempt_id, returned_store = resolve_attempt_id(handle.db_path, "task-1")

        assert attempt_id is None
        assert returned_store is not None

    def test_returns_none_when_task_not_found(self, tmp_path):
        """Returns None for attempt_id when task doesn't exist."""
        from matterstack.storage.state_store import SQLiteStateStore

        run_root = tmp_path / "runs" / "run-1"
        run_root.mkdir(parents=True)
        handle = RunHandle(workspace_slug="test-ws", run_id="run-1", root_path=run_root)
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)

        # Resolve a non-existent task
        attempt_id, returned_store = resolve_attempt_id(handle.db_path, "nonexistent-task")

        assert attempt_id is None
        assert returned_store is not None


class TestResolveAttemptContext:
    """Tests for resolve_attempt_context function."""

    def test_attempt_aware_layout(self, tmp_path):
        """Uses attempt-aware layout when attempt exists."""
        store, handle, expected_attempt_id = _create_seeded_store(tmp_path, with_attempt=True)

        # Resolve
        ctx = resolve_attempt_context(handle.root_path, handle.db_path, "task-1", "hpc")

        assert ctx.is_attempt_aware
        assert ctx.attempt_id == expected_attempt_id
        assert ctx.operator_uuid is None
        assert f"tasks/task-1/attempts/{expected_attempt_id}" in str(ctx.relative_path)

    def test_legacy_layout_fallback(self, tmp_path):
        """Falls back to legacy layout when no attempt exists."""
        store, handle, _ = _create_seeded_store(tmp_path, with_attempt=False)

        # Resolve
        ctx = resolve_attempt_context(handle.root_path, handle.db_path, "task-1", "hpc")

        assert not ctx.is_attempt_aware
        assert ctx.attempt_id is None
        assert ctx.operator_uuid is not None
        assert "operators/hpc" in str(ctx.relative_path)

    def test_legacy_layout_on_db_error(self, tmp_path):
        """Falls back to legacy layout on database errors."""
        run_root = tmp_path / "runs" / "run-1"
        run_root.mkdir(parents=True)
        db_path = tmp_path / "nonexistent" / "state.db"

        # Resolve with bad DB path
        ctx = resolve_attempt_context(run_root, db_path, "task-1", "hpc")

        assert not ctx.is_attempt_aware
        assert ctx.attempt_id is None
        assert ctx.operator_uuid is not None


class TestGetOrCreateStore:
    """Tests for get_or_create_store function."""

    def test_returns_existing_store_from_context(self, tmp_path):
        """Returns store from context if available."""
        mock_store = MagicMock()
        ctx = AttemptContext(
            attempt_id="attempt-1",
            store=mock_store,
            full_path=Path("/some/path"),
            relative_path=Path("tasks/t1/attempts/attempt-1"),
            operator_uuid=None,
        )

        result = get_or_create_store(ctx, tmp_path / "state.db")

        assert result is mock_store

    def test_creates_new_store_when_context_has_none(self, tmp_path):
        """Creates new store when context.store is None."""
        from matterstack.storage.state_store import SQLiteStateStore

        run_root = tmp_path / "runs" / "run-1"
        run_root.mkdir(parents=True)
        handle = RunHandle(workspace_slug="test-ws", run_id="run-1", root_path=run_root)

        # Initialize DB first
        SQLiteStateStore(handle.db_path)

        ctx = AttemptContext(
            attempt_id="attempt-1",
            store=None,
            full_path=Path("/some/path"),
            relative_path=Path("tasks/t1/attempts/attempt-1"),
            operator_uuid=None,
        )

        result = get_or_create_store(ctx, handle.db_path)

        assert result is not None

    def test_creates_store_even_when_parent_dir_missing(self, tmp_path):
        """SQLiteStateStore creates missing directories, so get_or_create_store succeeds."""
        ctx = AttemptContext(
            attempt_id="attempt-1",
            store=None,
            full_path=Path("/some/path"),
            relative_path=Path("tasks/t1/attempts/attempt-1"),
            operator_uuid=None,
        )

        # SQLiteStateStore will create the parent directory
        result = get_or_create_store(ctx, tmp_path / "new_dir" / "state.db")

        # Store is created successfully (SQLiteStateStore creates parent dirs)
        assert result is not None
