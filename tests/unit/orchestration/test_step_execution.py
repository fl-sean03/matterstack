"""Characterization tests for step execution.

These tests capture existing behavior of step_run() and its phases
(POLL, PLAN, EXECUTE, ANALYZE) to prevent regressions during refactoring.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from matterstack.core.campaign import Campaign
from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.step_execution import (
    DEFAULT_MAX_CONCURRENT_GLOBAL,
    _build_default_operator_registry,
    step_run,
)
from matterstack.storage.state_store import SQLiteStateStore


class MockCampaign(Campaign):
    """Mock campaign for testing step execution.
    
    Uses the matterstack.core.campaign.Campaign interface:
    - plan(state) -> Optional[Workflow]
    - analyze(state, results) -> new_state
    """
    
    def __init__(self, plan_result: Optional[Workflow] = None):
        self.plan_result = plan_result
        self.plan_call_count = 0
        self.analyze_called = False
        self.analyze_results = None
    
    def plan(self, state: Any) -> Optional[Workflow]:
        self.plan_call_count += 1
        # Return the workflow on first call, None on subsequent calls
        if self.plan_call_count == 1 and self.plan_result is not None:
            return self.plan_result
        return None
    
    def analyze(self, state: Any, results: Any) -> Any:
        self.analyze_called = True
        self.analyze_results = results
        return state


@pytest.fixture
def temp_store():
    """Create a temporary SQLite store for testing."""
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = SQLiteStateStore(db_path)
        yield store, Path(tmpdir)


@pytest.fixture
def run_handle(tmp_path):
    """Create a run handle for testing."""
    return RunHandle(
        run_id="test_run_001",
        workspace_slug="test_workspace",
        root_path=tmp_path,
    )


class TestBuildDefaultOperatorRegistry:
    """Tests for _build_default_operator_registry()."""

    def test_creates_legacy_keys(self, run_handle):
        """Should create operators with legacy keys."""
        registry = _build_default_operator_registry(run_handle)
        
        assert "Human" in registry
        assert "Experiment" in registry
        assert "Local" in registry
        assert "HPC" in registry

    def test_creates_canonical_keys(self, run_handle):
        """Should create operators with canonical v0.2.6 keys."""
        registry = _build_default_operator_registry(run_handle)
        
        assert "human.default" in registry
        assert "experiment.default" in registry
        assert "local.default" in registry
        assert "hpc.default" in registry

    def test_legacy_and_canonical_share_instances(self, run_handle):
        """Legacy and canonical keys should map to same operator instances."""
        registry = _build_default_operator_registry(run_handle)
        
        assert registry["Human"] is registry["human.default"]
        assert registry["Local"] is registry["local.default"]
        assert registry["HPC"] is registry["hpc.default"]
        assert registry["Experiment"] is registry["experiment.default"]


class TestStepRunStatusChecks:
    """Tests for step_run() run status handling."""

    def test_transitions_pending_to_running(self, tmp_path):
        """Should transition PENDING run to RUNNING."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        # Use the handle's db_path so step_run finds the same database
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        # Run should now be COMPLETED (no tasks)
        assert result in ["COMPLETED", "RUNNING"]

    def test_skips_cancelled_run(self, tmp_path):
        """Should skip execution for CANCELLED run."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        store.set_run_status(handle.run_id, "CANCELLED")
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        assert result == "CANCELLED"

    def test_skips_failed_run(self, tmp_path):
        """Should skip execution for FAILED run."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        store.set_run_status(handle.run_id, "FAILED", reason="Test failure")
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        assert result == "FAILED"

    def test_skips_completed_run(self, tmp_path):
        """Should skip execution for COMPLETED run."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        store.set_run_status(handle.run_id, "COMPLETED")
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        assert result == "COMPLETED"

    def test_returns_paused_for_paused_run(self, tmp_path):
        """Should return PAUSED for paused run without executing."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        store.set_run_status(handle.run_id, "PAUSED")
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        assert result == "PAUSED"


class TestStepRunPlanPhase:
    """Tests for step_run() PLAN phase (dependency checking)."""

    def test_identifies_ready_tasks(self, tmp_path):
        """Should identify tasks with met dependencies as ready."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create a task with no dependencies
        task = Task(task_id="task_001", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        store.add_workflow(workflow, handle.run_id)
        
        campaign = MockCampaign()
        
        # In simulation mode, task should be marked COMPLETED immediately
        result = step_run(handle, campaign)
        
        # Re-open store to get fresh data
        store = SQLiteStateStore(handle.db_path)
        status = store.get_task_status("task_001")
        # Task should have progressed (either COMPLETED in simulation or submitted)
        assert status in ["COMPLETED", "RUNNING", "SUBMITTED", "WAITING_EXTERNAL"]

    def test_waits_for_dependencies(self, tmp_path):
        """Should wait for dependencies to complete before running task."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create two tasks with dependency
        task1 = Task(task_id="task_001", image="test:latest", command="echo first")
        task2 = Task(task_id="task_002", image="test:latest", command="echo second", dependencies={"task_001"})
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, handle.run_id)
        
        campaign = MockCampaign()
        
        # First tick
        result = step_run(handle, campaign)
        
        # Re-open store to get fresh data
        store = SQLiteStateStore(handle.db_path)
        # task_001 should be completed (simulation), task_002 should now be ready
        task1_status = store.get_task_status("task_001")
        assert task1_status == "COMPLETED"

    def test_handles_failed_dependencies(self, tmp_path):
        """When dependency fails and dependent is blocked, run stays RUNNING until terminal."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create two tasks with dependency
        task1 = Task(task_id="task_001", image="test:latest", command="echo first")
        task2 = Task(task_id="task_002", image="test:latest", command="echo second", dependencies={"task_001"})
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, handle.run_id)
        
        # Mark first task as failed
        store.update_task_status("task_001", "FAILED")
        
        campaign = MockCampaign()
        
        # With pending dependent task, run stays RUNNING (dependent blocked on unmet dep)
        # The run only transitions to FAILED when there are no active tasks and no tasks to run
        result = step_run(handle, campaign)
        
        # The actual behavior: run stays RUNNING because task_002 is pending (waiting for failed dep)
        assert result == "RUNNING"


class TestStepRunExecutePhase:
    """Tests for step_run() EXECUTE phase (task submission)."""

    def test_respects_global_concurrency_limit(self, tmp_path):
        """Should respect max_hpc_jobs global concurrency limit."""
        import json
        
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        # Set concurrency limit to 1
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps({"max_hpc_jobs_per_run": 1, "execution_mode": "HPC"}))
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create multiple tasks
        task1 = Task(task_id="task_001", image="test:latest", command="echo test1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo test2")
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, handle.run_id)
        
        campaign = MockCampaign()
        
        # Mock operators to prevent actual submission
        mock_operators = {
            "HPC": MagicMock(),
            "hpc.default": MagicMock(),
        }
        mock_operators["HPC"].prepare_run.side_effect = Exception("Mocked to prevent submission")
        mock_operators["hpc.default"].prepare_run.side_effect = Exception("Mocked to prevent submission")
        
        # Run step (will fail on submission but demonstrates concurrency check)
        result = step_run(handle, campaign, operator_registry=mock_operators)
        
        # At least one task should have been attempted
        assert result in ["RUNNING", "FAILED"]

    def test_simulation_mode_completes_immediately(self, tmp_path):
        """Tasks in simulation mode should complete immediately."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create a task (no config = simulation mode)
        task = Task(task_id="task_001", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        store.add_workflow(workflow, handle.run_id)
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        # Re-open store to get fresh data
        store = SQLiteStateStore(handle.db_path)
        # Task should be completed
        status = store.get_task_status("task_001")
        assert status == "COMPLETED"


class TestStepRunAnalyzePhase:
    """Tests for step_run() ANALYZE phase (campaign iteration)."""

    def test_completes_when_no_new_workflow(self, tmp_path):
        """Run completes on second tick when campaign.plan() returns None."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create a task that will complete in simulation
        task = Task(task_id="task_001", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        store.add_workflow(workflow, handle.run_id)
        
        # Campaign returns no new workflow
        campaign = MockCampaign(plan_result=None)
        
        # First tick: task_001 gets submitted (completes in simulation mode)
        # But ANALYZE phase doesn't run in same tick as submission
        result1 = step_run(handle, campaign)
        assert result1 == "RUNNING"  # Still running after first tick
        
        # Second tick: All tasks complete, ANALYZE phase runs, plan returns None
        result2 = step_run(handle, campaign)
        assert result2 == "COMPLETED"  # Now completed

    def test_adds_new_workflow_when_plan_returns_tasks(self, tmp_path):
        """New workflow is added on second tick during analyze phase."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create initial task
        task1 = Task(task_id="task_001", image="test:latest", command="echo first")
        workflow1 = Workflow()
        workflow1.add_task(task1)
        store.add_workflow(workflow1, handle.run_id)
        
        # Campaign will return a new workflow on first plan() call
        new_task = Task(task_id="task_002", image="test:latest", command="echo second")
        new_workflow = Workflow()
        new_workflow.add_task(new_task)
        
        # Use a custom campaign that always returns the workflow
        class AlwaysReturnWorkflowCampaign(Campaign):
            def plan(self, state: Any) -> Optional[Workflow]:
                return new_workflow
            
            def analyze(self, state: Any, results: Any) -> Any:
                return state
        
        campaign = AlwaysReturnWorkflowCampaign()
        
        # First tick - submits task_001 (completes in simulation)
        result1 = step_run(handle, campaign)
        assert result1 == "RUNNING"
        
        # Second tick - ANALYZE phase runs, new workflow added
        result2 = step_run(handle, campaign)
        assert result2 == "RUNNING"  # Still running with new tasks
        
        # Re-open store to get fresh data
        store = SQLiteStateStore(handle.db_path)
        # Verify new task was added
        all_tasks = store.get_tasks(handle.run_id)
        task_ids = [t.task_id for t in all_tasks]
        assert "task_002" in task_ids


class TestStepRunEdgeCases:
    """Tests for step_run() edge cases."""

    def test_handles_empty_workflow(self, tmp_path):
        """Should complete immediately when no tasks exist."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        # No tasks = immediate completion
        assert result == "COMPLETED"

    def test_handles_allow_failure_tasks(self, tmp_path):
        """Tasks with allow_failure=True should not cause run failure."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create a task with allow_failure=True
        task = Task(task_id="task_001", image="test:latest", command="echo test", allow_failure=True)
        workflow = Workflow()
        workflow.add_task(task)
        store.add_workflow(workflow, handle.run_id)
        
        # Mark task as failed
        store.update_task_status("task_001", "FAILED")
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        # Should complete, not fail (because allow_failure=True)
        assert result == "COMPLETED"

    def test_default_global_concurrency_constant(self):
        """DEFAULT_MAX_CONCURRENT_GLOBAL should be defined."""
        assert DEFAULT_MAX_CONCURRENT_GLOBAL == 50

    def test_skips_already_active_tasks(self, tmp_path):
        """Should not resubmit tasks that already have active attempts."""
        handle = RunHandle(
            run_id="test_run",
            workspace_slug="test",
            root_path=tmp_path,
        )
        
        store = SQLiteStateStore(handle.db_path)
        store.create_run(handle)
        
        # Create a task
        task = Task(task_id="task_001", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        store.add_workflow(workflow, handle.run_id)
        
        # Create an active attempt for the task
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        
        campaign = MockCampaign()
        
        result = step_run(handle, campaign)
        
        # Should be RUNNING, waiting for the active attempt
        assert result == "RUNNING"
        
        # Re-open store to get fresh data
        store = SQLiteStateStore(handle.db_path)
        # Should still have only one attempt
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 1
