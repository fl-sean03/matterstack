"""
Unit tests for orphan attempt detection and cleanup.

Tests:
- find_orphaned_attempts returns stuck CREATED attempts
- mark_attempts_failed_init updates status correctly
- FAILED_INIT is treated as terminal state
"""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.state_store import SQLiteStateStore


@pytest.fixture
def temp_store():
    """Create a temporary SQLite store for testing."""
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = SQLiteStateStore(db_path)
        yield store


@pytest.fixture
def store_with_run(temp_store):
    """Create a store with a run and task ready for attempts."""
    run_id = "test_run_001"
    root_path = Path("/tmp/test")

    handle = RunHandle(
        run_id=run_id,
        workspace_slug="test_workspace",
        root_path=root_path,
    )
    temp_store.create_run(handle)

    # Create tasks using workflow
    task1 = Task(task_id="task_001", image="test:latest", command="echo test")
    task2 = Task(task_id="task_002", image="test:latest", command="echo test2")
    workflow = Workflow()
    workflow.add_task(task1)
    workflow.add_task(task2)
    temp_store.add_workflow(workflow, run_id)

    return temp_store, run_id


class TestFailedInitStatus:
    """Test FAILED_INIT status semantics."""

    def test_failed_init_exists_in_enum(self):
        """FAILED_INIT should exist in ExternalRunStatus enum."""
        assert hasattr(ExternalRunStatus, "FAILED_INIT")
        assert ExternalRunStatus.FAILED_INIT.value == "FAILED_INIT"

    def test_failed_init_is_terminal_in_get_active_attempts(self, store_with_run):
        """FAILED_INIT attempts should not appear in get_active_attempts."""
        store, run_id = store_with_run

        # Create an attempt in FAILED_INIT state
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.FAILED_INIT.value,
        )

        # Should not appear in active attempts
        active = store.get_active_attempts(run_id)
        assert len(active) == 0

    def test_failed_init_is_terminal_in_count_active(self, store_with_run):
        """FAILED_INIT should not count as active for concurrency."""
        store, run_id = store_with_run

        # Create a FAILED_INIT attempt
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            operator_key="hpc.default",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.FAILED_INIT.value,
        )

        # Should not count as active
        counts = store.count_active_attempts_by_operator(run_id)
        assert counts.get("hpc.default", 0) == 0


class TestFindOrphanedAttempts:
    """Test find_orphaned_attempts method."""

    def test_no_orphans_when_all_submitted(self, store_with_run):
        """No orphans when all attempts have external_ids."""
        store, run_id = store_with_run

        # Create a submitted attempt (has external_id)
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.SUBMITTED.value,
            external_id="slurm_12345",
        )

        # Should find no orphans
        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=0)
        assert len(orphans) == 0

    def test_no_orphans_when_recently_created(self, store_with_run):
        """No orphans when attempts are too recent."""
        store, run_id = store_with_run

        # Create a fresh CREATED attempt (no external_id)
        store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )

        # With 1 hour timeout, should find no orphans
        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=3600)
        assert len(orphans) == 0

    def test_finds_stuck_created_attempts(self, store_with_run):
        """Find attempts stuck in CREATED without external_id."""
        store, run_id = store_with_run

        # Create a CREATED attempt (will have created_at set)
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )

        # With 0 second timeout, should find it immediately
        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=0)
        assert len(orphans) == 1
        assert orphans[0].attempt_id == attempt_id

    def test_excludes_non_created_attempts(self, store_with_run):
        """Should not find attempts that have progressed beyond CREATED."""
        store, run_id = store_with_run

        # Create and progress an attempt
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.SUBMITTED.value,
            # Note: even without external_id, not CREATED means not orphaned
        )

        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=0)
        assert len(orphans) == 0

    def test_excludes_attempts_with_external_id(self, store_with_run):
        """Should not find CREATED attempts that have external_ids."""
        store, run_id = store_with_run

        # Create and add external_id but keep status CREATED (edge case)
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            external_id="slurm_99999",
        )

        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=0)
        assert len(orphans) == 0


class TestMarkAttemptsFailedInit:
    """Test mark_attempts_failed_init method."""

    def test_marks_single_attempt(self, store_with_run):
        """Mark a single attempt as FAILED_INIT."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )

        count = store.mark_attempts_failed_init([attempt_id], "test cleanup")
        assert count == 1

        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.FAILED_INIT.value
        assert attempt.status_reason == "test cleanup"
        assert attempt.ended_at is not None

    def test_marks_multiple_attempts(self, store_with_run):
        """Mark multiple attempts as FAILED_INIT."""
        store, run_id = store_with_run

        attempt_id_1 = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        attempt_id_2 = store.create_attempt(
            run_id=run_id,
            task_id="task_002",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )

        count = store.mark_attempts_failed_init(
            [attempt_id_1, attempt_id_2],
            "batch cleanup",
        )
        assert count == 2

        for aid in [attempt_id_1, attempt_id_2]:
            attempt = store.get_attempt(aid)
            assert attempt.status == ExternalRunStatus.FAILED_INIT.value

    def test_empty_list_returns_zero(self, store_with_run):
        """Empty list should return 0 without errors."""
        store, run_id = store_with_run

        count = store.mark_attempts_failed_init([], "no attempts")
        assert count == 0


class TestTaskStatusMapping:
    """Test that FAILED_INIT maps to FAILED task status."""

    def test_failed_init_maps_to_failed(self):
        """FAILED_INIT should map to task status FAILED."""
        from matterstack.orchestration.polling import task_status_from_external_status

        result = task_status_from_external_status(ExternalRunStatus.FAILED_INIT)
        assert result == "FAILED"
