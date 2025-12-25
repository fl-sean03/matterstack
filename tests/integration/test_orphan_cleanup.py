"""
Integration tests for orphan attempt cleanup.

Tests:
- Dispatch failure marks attempt as FAILED_INIT
- Polling detects stuck attempts and marks them FAILED_INIT
- Cleanup CLI command finds and marks orphans
"""
import pytest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from matterstack.core.operators import (
    ExternalRunStatus,
    ExternalRunHandle,
    Operator,
    OperatorResult,
)
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.orchestration.dispatch import submit_task_to_operator
from matterstack.orchestration.polling import poll_active_attempts


class FailingOperator(Operator):
    """Test operator that fails on submit."""
    
    def prepare_run(self, run: RunHandle, task: Task) -> ExternalRunHandle:
        return ExternalRunHandle(
            task_id=task.task_id,
            operator_type="failing",
            status=ExternalRunStatus.CREATED,
        )
    
    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        raise RuntimeError("Simulated submit failure")
    
    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        return handle
    
    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
        )


class SuccessfulOperator(Operator):
    """Test operator that succeeds."""
    
    def prepare_run(self, run: RunHandle, task: Task) -> ExternalRunHandle:
        return ExternalRunHandle(
            task_id=task.task_id,
            operator_type="success",
            status=ExternalRunStatus.CREATED,
        )
    
    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        handle.external_id = "test_job_123"
        handle.status = ExternalRunStatus.SUBMITTED
        return handle
    
    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        return handle
    
    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
        )


@pytest.fixture
def temp_run():
    """Create a temporary run with store and handle."""
    from matterstack.core.workflow import Task as WorkflowTask, Workflow
    
    with TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        run_id = "test_run_001"
        
        run_handle = RunHandle(
            run_id=run_id,
            root_path=root_path,
            workspace_slug="test_workspace",
        )
        
        # Use run_handle.db_path to ensure consistency
        store = SQLiteStateStore(run_handle.db_path)
        
        store.create_run(run_handle)
        
        # Create tasks using workflow
        task1 = WorkflowTask(task_id="task_001", image="test:latest", command="echo test")
        task2 = WorkflowTask(task_id="task_002", image="test:latest", command="echo test2")
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, run_id)
        
        yield store, run_handle, run_id


class TestDispatchFailureMarksFailedInit:
    """Test that dispatch failures mark attempts as FAILED_INIT."""
    
    def test_submit_failure_creates_failed_init_attempt(self, temp_run):
        """When submit() raises, attempt should be FAILED_INIT."""
        store, run_handle, run_id = temp_run
        
        task = Task(
            task_id="task_001",
            image="test:latest",
            command="echo test",
        )
        
        operators = {"failing": FailingOperator()}
        
        result = submit_task_to_operator(
            task=task,
            operator_type="failing",
            run_handle=run_handle,
            store=store,
            operators=operators,
        )
        
        # Should return False (failure)
        assert result is False
        
        # Check the attempt was created and marked FAILED_INIT
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 1
        
        attempt = attempts[0]
        assert attempt.status == ExternalRunStatus.FAILED_INIT.value
        assert "Simulated submit failure" in (attempt.status_reason or "")
    
    def test_failed_init_not_counted_as_active(self, temp_run):
        """FAILED_INIT attempts should not count against concurrency."""
        store, run_handle, run_id = temp_run
        
        task = Task(
            task_id="task_001",
            image="test:latest",
            command="echo test",
        )
        
        operators = {"failing": FailingOperator()}
        
        # Submit and fail
        submit_task_to_operator(
            task=task,
            operator_type="failing",
            run_handle=run_handle,
            store=store,
            operators=operators,
        )
        
        # Should have no active attempts
        active = store.get_active_attempts(run_id)
        assert len(active) == 0


class TestPollingDetectsStuckAttempts:
    """Test that polling detects and marks stuck attempts."""
    
    def test_stuck_attempt_marked_failed_init(self, temp_run):
        """Polling should mark stuck CREATED attempts as FAILED_INIT."""
        store, run_handle, run_id = temp_run
        
        # Create a CREATED attempt directly (simulating stuck state)
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        
        operators = {}  # No operators needed - stuck detection happens first
        
        # Poll with 0 second timeout to immediately detect stuck
        poll_active_attempts(
            run_id=run_id,
            store=store,
            operators=operators,
            stuck_timeout_seconds=0,
        )
        
        # Attempt should now be FAILED_INIT
        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.FAILED_INIT.value
        assert "Stuck in CREATED" in (attempt.status_reason or "")
    
    def test_submitted_attempt_not_marked_stuck(self, temp_run):
        """SUBMITTED attempts should not be marked as stuck."""
        store, run_handle, run_id = temp_run
        
        # Create a SUBMITTED attempt
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(
            attempt_id,
            status=ExternalRunStatus.SUBMITTED.value,
            external_id="job_123",
        )
        
        operators = {}
        
        # Poll with 0 second timeout
        poll_active_attempts(
            run_id=run_id,
            store=store,
            operators=operators,
            stuck_timeout_seconds=0,
        )
        
        # Attempt should still be SUBMITTED
        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.SUBMITTED.value
    
    def test_stuck_detection_fires_lifecycle_hook(self, temp_run):
        """Stuck detection should fire on_fail lifecycle hook."""
        store, run_handle, run_id = temp_run
        
        # Create a stuck attempt
        store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        
        # Create a mock lifecycle hook
        mock_hook = MagicMock()
        
        poll_active_attempts(
            run_id=run_id,
            store=store,
            operators={},
            lifecycle_hooks=mock_hook,
            stuck_timeout_seconds=0,
        )
        
        # on_fail should have been called
        mock_hook.on_fail.assert_called_once()


class TestCleanupOrphansCommand:
    """Test the cleanup-orphans CLI command."""
    
    def test_parse_timeout_hours(self):
        """Should parse hour-based timeout strings."""
        from matterstack.cli.commands.task_management import _parse_timeout
        
        assert _parse_timeout("1h") == 3600
        assert _parse_timeout("2h") == 7200
        assert _parse_timeout("24h") == 86400
    
    def test_parse_timeout_minutes(self):
        """Should parse minute-based timeout strings."""
        from matterstack.cli.commands.task_management import _parse_timeout
        
        assert _parse_timeout("30m") == 1800
        assert _parse_timeout("60m") == 3600
        assert _parse_timeout("5m") == 300
    
    def test_parse_timeout_seconds(self):
        """Should parse second-based timeout strings."""
        from matterstack.cli.commands.task_management import _parse_timeout
        
        assert _parse_timeout("3600s") == 3600
        assert _parse_timeout("3600") == 3600
        assert _parse_timeout("60") == 60
    
    def test_parse_timeout_invalid(self):
        """Should raise on invalid format."""
        from matterstack.cli.commands.task_management import _parse_timeout
        
        with pytest.raises(ValueError):
            _parse_timeout("invalid")
        with pytest.raises(ValueError):
            _parse_timeout("1d")  # days not supported
    
    def test_format_age(self):
        """Should format age as human-readable string."""
        from matterstack.cli.commands.task_management import _format_age
        
        now = datetime.utcnow()
        
        # 30 minutes ago
        result = _format_age(now - timedelta(minutes=30))
        assert "30m" in result
        
        # 2 hours ago
        result = _format_age(now - timedelta(hours=2))
        assert "2h" in result
    
    def test_cleanup_command_lists_orphans(self, temp_run, capsys):
        """cleanup-orphans should list orphaned attempts."""
        from types import SimpleNamespace
        
        store, run_handle, run_id = temp_run
        
        # Create a stuck attempt
        store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        
        # Mock args using SimpleNamespace
        args = SimpleNamespace(run_id=run_id, confirm=False, timeout="0s")
        
        # Patch find_run to return our handle
        with patch("matterstack.cli.commands.task_management.find_run") as mock_find:
            mock_find.return_value = run_handle
            
            from matterstack.cli.commands.task_management import cmd_cleanup_orphans
            cmd_cleanup_orphans(args)
        
        captured = capsys.readouterr()
        assert "Found 1 orphaned attempt" in captured.out
        assert "task_001" in captured.out
        assert "Run with --confirm" in captured.out
    
    def test_cleanup_command_marks_orphans_with_confirm(self, temp_run, capsys):
        """cleanup-orphans --confirm should mark orphans as FAILED_INIT."""
        from types import SimpleNamespace
        
        store, run_handle, run_id = temp_run
        
        # Create a stuck attempt
        attempt_id = store.create_attempt(
            run_id=run_id,
            task_id="task_001",
            operator_type="test",
            status=ExternalRunStatus.CREATED.value,
        )
        
        # Mock args using SimpleNamespace
        args = SimpleNamespace(run_id=run_id, confirm=True, timeout="0s")
        
        with patch("matterstack.cli.commands.task_management.find_run") as mock_find:
            mock_find.return_value = run_handle
            
            from matterstack.cli.commands.task_management import cmd_cleanup_orphans
            cmd_cleanup_orphans(args)
        
        # Check attempt is now FAILED_INIT
        attempt = store.get_attempt(attempt_id)
        assert attempt.status == ExternalRunStatus.FAILED_INIT.value
        
        captured = capsys.readouterr()
        assert "Marked 1 attempt(s) as FAILED_INIT" in captured.out


class TestEndToEndOrphanHandling:
    """End-to-end tests for orphan handling."""
    
    def test_full_orphan_lifecycle(self, temp_run):
        """Test complete orphan lifecycle: create, detect, cleanup."""
        store, run_handle, run_id = temp_run
        
        # 1. Simulate failed dispatch creating orphan
        task = Task(
            task_id="task_001",
            image="test:latest",
            command="echo test",
        )
        
        operators = {"failing": FailingOperator()}
        
        submit_task_to_operator(
            task=task,
            operator_type="failing",
            run_handle=run_handle,
            store=store,
            operators=operators,
        )
        
        # 2. Verify it's in FAILED_INIT state
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 1
        assert attempts[0].status == ExternalRunStatus.FAILED_INIT.value
        
        # 3. Verify it doesn't count as active
        active = store.get_active_attempts(run_id)
        assert len(active) == 0
        
        # 4. Verify rerun can proceed (would create new attempt)
        store.update_task_status("task_001", "PENDING")
        
        # Submit with successful operator
        task2 = Task(
            task_id="task_001",
            image="test:latest",
            command="echo test",
        )
        
        operators_success = {"success": SuccessfulOperator()}
        
        result = submit_task_to_operator(
            task=task2,
            operator_type="success",
            run_handle=run_handle,
            store=store,
            operators=operators_success,
        )
        
        assert result is True
        
        # 5. Verify we now have 2 attempts
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 2
        
        # First attempt is FAILED_INIT, second is SUBMITTED
        assert attempts[0].status == ExternalRunStatus.FAILED_INIT.value
        assert attempts[1].status == ExternalRunStatus.SUBMITTED.value
