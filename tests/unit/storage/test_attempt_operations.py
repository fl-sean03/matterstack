"""Characterization tests for attempt operations.

These tests capture existing behavior of attempt CRUD operations
to prevent regressions during refactoring.
"""

from datetime import datetime, timedelta
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
    """Create a store with a run and tasks ready for attempts."""
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
    task3 = Task(task_id="task_003", image="test:latest", command="echo test3")
    workflow = Workflow()
    workflow.add_task(task1)
    workflow.add_task(task2)
    workflow.add_task(task3)
    temp_store.add_workflow(workflow, run_id)

    return temp_store, run_id


class TestCreateAttempt:
    """Tests for create_attempt()."""

    def test_creates_attempt_with_default_status(self, store_with_run):
        """Should create attempt with CREATED status by default."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt is not None
        assert attempt.status == ExternalRunStatus.CREATED.value

    def test_creates_attempt_with_custom_status(self, store_with_run):
        """Should create attempt with specified status."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.SUBMITTED.value,
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.SUBMITTED.value

    def test_creates_attempt_with_operator_key(self, store_with_run):
        """Should create attempt with operator_key."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
            operator_key="hpc.default",
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt.operator_key == "hpc.default"

    def test_creates_attempt_with_operator_data(self, store_with_run):
        """Should create attempt with operator_data dict."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
            operator_data={"job_name": "test_job", "queue": "batch"},
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt.operator_data == {"job_name": "test_job", "queue": "batch"}

    def test_creates_attempt_with_relative_path(self, store_with_run):
        """Should create attempt with relative_path."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
            relative_path=Path("runs/task_001"),
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt.relative_path == "runs/task_001"

    def test_auto_increments_attempt_index(self, store_with_run):
        """Should auto-increment attempt_index for same task."""
        store, run_id = store_with_run

        # Create first attempt
        attempt_id_1 = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
        )

        # Create second attempt for same task
        attempt_id_2 = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
        )

        attempt_1 = store.get_attempt(attempt_id_1)
        attempt_2 = store.get_attempt(attempt_id_2)

        assert attempt_1.attempt_index == 1
        assert attempt_2.attempt_index == 2

    def test_sets_current_attempt_id_on_task(self, store_with_run):
        """Should set current_attempt_id on task after creation."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
        )

        current = store.get_current_attempt("task_001")
        assert current is not None
        assert current.attempt_id == attempt_id

    def test_sets_created_at_timestamp(self, store_with_run):
        """Should set created_at timestamp on creation."""
        store, run_id = store_with_run

        before = datetime.utcnow()
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="HPC",
        )
        after = datetime.utcnow()

        attempt = store.get_attempt(attempt_id)
        assert attempt.created_at is not None
        assert before <= attempt.created_at <= after

    def test_raises_for_nonexistent_task(self, store_with_run):
        """Should raise ValueError for non-existent task."""
        store, run_id = store_with_run

        with pytest.raises(ValueError, match="not found"):
            store.create_attempt(
                run_id=run_id,
                task_id="nonexistent_task",
                operator_type="HPC",
            )


class TestListAttempts:
    """Tests for list_attempts()."""

    def test_returns_empty_list_for_no_attempts(self, store_with_run):
        """Should return empty list when no attempts exist."""
        store, run_id = store_with_run

        attempts = store.list_attempts("task_001")
        assert attempts == []

    def test_returns_attempts_ordered_by_index(self, store_with_run):
        """Should return attempts ordered by attempt_index."""
        store, run_id = store_with_run

        # Create multiple attempts
        id1 = store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        id2 = store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        id3 = store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")

        attempts = store.list_attempts("task_001")

        assert len(attempts) == 3
        assert [a.attempt_index for a in attempts] == [1, 2, 3]

    def test_returns_only_attempts_for_specified_task(self, store_with_run):
        """Should only return attempts for the specified task."""
        store, run_id = store_with_run

        # Create attempts for different tasks
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_002", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")

        attempts = store.list_attempts("task_001")

        assert len(attempts) == 2
        assert all(a.task_id == "task_001" for a in attempts)


class TestGetAttemptCount:
    """Tests for get_attempt_count()."""

    def test_returns_zero_for_no_attempts(self, store_with_run):
        """Should return 0 when no attempts exist."""
        store, run_id = store_with_run

        count = store.get_attempt_count(run_id, "task_001")
        assert count == 0

    def test_returns_correct_count(self, store_with_run):
        """Should return correct count of attempts."""
        store, run_id = store_with_run

        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")

        count = store.get_attempt_count(run_id, "task_001")
        assert count == 3


class TestGetActiveAttempts:
    """Tests for get_active_attempts()."""

    def test_returns_non_terminal_attempts(self, store_with_run):
        """Should return attempts not in terminal states."""
        store, run_id = store_with_run

        # Create attempts in various states
        store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value
        )
        store.create_attempt(
            run_id=run_id, task_id="task_002", operator_type="HPC",
            status=ExternalRunStatus.SUBMITTED.value
        )
        store.create_attempt(
            run_id=run_id, task_id="task_003", operator_type="HPC",
            status=ExternalRunStatus.WAITING_EXTERNAL.value
        )

        active = store.get_active_attempts(run_id)

        assert len(active) == 3

    def test_excludes_completed_attempts(self, store_with_run):
        """Should exclude COMPLETED attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.COMPLETED.value)

        active = store.get_active_attempts(run_id)

        assert len(active) == 0

    def test_excludes_failed_attempts(self, store_with_run):
        """Should exclude FAILED attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.FAILED.value)

        active = store.get_active_attempts(run_id)

        assert len(active) == 0

    def test_excludes_failed_init_attempts(self, store_with_run):
        """Should exclude FAILED_INIT attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.FAILED_INIT.value)

        active = store.get_active_attempts(run_id)

        assert len(active) == 0

    def test_excludes_cancelled_attempts(self, store_with_run):
        """Should exclude CANCELLED attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.CANCELLED.value)

        active = store.get_active_attempts(run_id)

        assert len(active) == 0


class TestCountActiveAttemptsByOperator:
    """Tests for count_active_attempts_by_operator()."""

    def test_groups_by_operator_key(self, store_with_run):
        """Should group active attempts by operator_key."""
        store, run_id = store_with_run

        # Create attempts with different operator keys
        store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            operator_key="hpc.default", status=ExternalRunStatus.RUNNING.value
        )
        store.create_attempt(
            run_id=run_id, task_id="task_002", operator_type="Human",
            operator_key="human.default", status=ExternalRunStatus.RUNNING.value
        )
        store.create_attempt(
            run_id=run_id, task_id="task_003", operator_type="HPC",
            operator_key="hpc.default", status=ExternalRunStatus.RUNNING.value
        )

        counts = store.count_active_attempts_by_operator(run_id)

        assert counts["hpc.default"] == 2
        assert counts["human.default"] == 1

    def test_maps_none_key_to_empty_string(self, store_with_run):
        """Should map None operator_key to empty string."""
        store, run_id = store_with_run

        store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            operator_key=None, status=ExternalRunStatus.RUNNING.value
        )

        counts = store.count_active_attempts_by_operator(run_id)

        assert counts.get("", 0) == 1

    def test_excludes_terminal_attempts(self, store_with_run):
        """Should not count terminal attempts."""
        store, run_id = store_with_run

        # Create a completed attempt
        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            operator_key="hpc.default", status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.COMPLETED.value)

        counts = store.count_active_attempts_by_operator(run_id)

        assert counts.get("hpc.default", 0) == 0


class TestGetCurrentAttempt:
    """Tests for get_current_attempt()."""

    def test_returns_none_when_no_attempts(self, store_with_run):
        """Should return None when task has no attempts."""
        store, run_id = store_with_run

        current = store.get_current_attempt("task_001")
        assert current is None

    def test_returns_latest_attempt(self, store_with_run):
        """Should return the most recently created attempt."""
        store, run_id = store_with_run

        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        attempt_id_2 = store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")

        current = store.get_current_attempt("task_001")

        assert current.attempt_id == attempt_id_2


class TestGetAttempt:
    """Tests for get_attempt()."""

    def test_returns_none_for_nonexistent_attempt(self, store_with_run):
        """Should return None for non-existent attempt_id."""
        store, run_id = store_with_run

        attempt = store.get_attempt("nonexistent_attempt_id")
        assert attempt is None

    def test_returns_attempt_by_id(self, store_with_run):
        """Should return attempt by attempt_id."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        attempt = store.get_attempt(attempt_id)

        assert attempt is not None
        assert attempt.attempt_id == attempt_id


class TestGetAttemptTaskIds:
    """Tests for get_attempt_task_ids()."""

    def test_returns_empty_set_when_no_attempts(self, store_with_run):
        """Should return empty set when no attempts exist."""
        store, run_id = store_with_run

        task_ids = store.get_attempt_task_ids(run_id)
        assert task_ids == set()

    def test_returns_distinct_task_ids(self, store_with_run):
        """Should return distinct task_ids with attempts."""
        store, run_id = store_with_run

        # Create multiple attempts, some for same task
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        store.create_attempt(run_id=run_id, task_id="task_002", operator_type="HPC")

        task_ids = store.get_attempt_task_ids(run_id)

        assert task_ids == {"task_001", "task_002"}


class TestUpdateAttempt:
    """Tests for update_attempt()."""

    def test_updates_status(self, store_with_run):
        """Should update attempt status."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(attempt_id, status=ExternalRunStatus.RUNNING.value)

        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.RUNNING.value

    def test_updates_external_id(self, store_with_run):
        """Should update external_id."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(attempt_id, external_id="job_12345")

        attempt = store.get_attempt(attempt_id)
        assert attempt.external_id == "job_12345"

    def test_updates_operator_data(self, store_with_run):
        """Should update operator_data dict."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(attempt_id, operator_data={"exit_code": 0})

        attempt = store.get_attempt(attempt_id)
        assert attempt.operator_data == {"exit_code": 0}

    def test_updates_status_reason(self, store_with_run):
        """Should update status_reason."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.FAILED.value,
            status_reason="Out of memory"
        )

        attempt = store.get_attempt(attempt_id)
        assert attempt.status_reason == "Out of memory"

    def test_sets_submitted_at_on_status_change(self, store_with_run):
        """Should set submitted_at when status changes from CREATED."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(attempt_id, status=ExternalRunStatus.SUBMITTED.value)

        attempt = store.get_attempt(attempt_id)
        assert attempt.submitted_at is not None

    def test_sets_ended_at_on_terminal_status(self, store_with_run):
        """Should set ended_at when status changes to terminal."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.update_attempt(attempt_id, status=ExternalRunStatus.COMPLETED.value)

        attempt = store.get_attempt(attempt_id)
        assert attempt.ended_at is not None

    def test_raises_for_nonexistent_attempt(self, store_with_run):
        """Should raise ValueError for non-existent attempt."""
        store, run_id = store_with_run

        with pytest.raises(ValueError, match="not found"):
            store.update_attempt("nonexistent_id", status="RUNNING")


class TestFindOrphanedAttempts:
    """Tests for find_orphaned_attempts()."""

    def test_finds_stuck_created_attempts(self, store_with_run):
        """Should find attempts stuck in CREATED state."""
        store, run_id = store_with_run

        # Create an attempt (will have created_at set to now)
        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )

        # With a very short timeout, the attempt should be found
        # We need to manually update created_at to be old enough
        with store.SessionLocal() as session:
            from matterstack.storage.schema import TaskAttemptModel
            from sqlalchemy import select, update
            old_time = datetime.utcnow() - timedelta(hours=2)
            session.execute(
                update(TaskAttemptModel)
                .where(TaskAttemptModel.attempt_id == attempt_id)
                .values(created_at=old_time)
            )
            session.commit()

        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=3600)

        assert len(orphans) == 1
        assert orphans[0].attempt_id == attempt_id

    def test_excludes_attempts_with_external_id(self, store_with_run):
        """Should not find attempts that have external_id set."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )
        store.update_attempt(attempt_id, external_id="job_123")

        # Make it old
        with store.SessionLocal() as session:
            from matterstack.storage.schema import TaskAttemptModel
            from sqlalchemy import update
            old_time = datetime.utcnow() - timedelta(hours=2)
            session.execute(
                update(TaskAttemptModel)
                .where(TaskAttemptModel.attempt_id == attempt_id)
                .values(created_at=old_time)
            )
            session.commit()

        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=3600)

        assert len(orphans) == 0

    def test_excludes_recent_attempts(self, store_with_run):
        """Should not find recently created attempts."""
        store, run_id = store_with_run

        store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
            status=ExternalRunStatus.CREATED.value
        )

        # Attempt was just created, so timeout_seconds=3600 should not find it
        orphans = store.find_orphaned_attempts(run_id, timeout_seconds=3600)

        assert len(orphans) == 0


class TestMarkAttemptsFailedInit:
    """Tests for mark_attempts_failed_init()."""

    def test_marks_attempts_as_failed_init(self, store_with_run):
        """Should mark specified attempts as FAILED_INIT."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        count = store.mark_attempts_failed_init([attempt_id])

        assert count == 1
        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.FAILED_INIT.value

    def test_sets_status_reason(self, store_with_run):
        """Should set status_reason on marked attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.mark_attempts_failed_init([attempt_id], reason="Custom reason")

        attempt = store.get_attempt(attempt_id)
        assert attempt.status_reason == "Custom reason"

    def test_sets_ended_at(self, store_with_run):
        """Should set ended_at on marked attempts."""
        store, run_id = store_with_run

        attempt_id = store.create_attempt(
            run_id=run_id, task_id="task_001", operator_type="HPC",
        )

        store.mark_attempts_failed_init([attempt_id])

        attempt = store.get_attempt(attempt_id)
        assert attempt.ended_at is not None

    def test_handles_empty_list(self, store_with_run):
        """Should handle empty list without error."""
        store, run_id = store_with_run

        count = store.mark_attempts_failed_init([])

        assert count == 0

    def test_marks_multiple_attempts(self, store_with_run):
        """Should mark multiple attempts at once."""
        store, run_id = store_with_run

        id1 = store.create_attempt(run_id=run_id, task_id="task_001", operator_type="HPC")
        id2 = store.create_attempt(run_id=run_id, task_id="task_002", operator_type="HPC")
        id3 = store.create_attempt(run_id=run_id, task_id="task_003", operator_type="HPC")

        count = store.mark_attempts_failed_init([id1, id2, id3])

        assert count == 3
        for attempt_id in [id1, id2, id3]:
            attempt = store.get_attempt(attempt_id)
            assert attempt.status == ExternalRunStatus.FAILED_INIT.value
