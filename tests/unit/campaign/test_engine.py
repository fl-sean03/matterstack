"""Characterization tests for campaign engine.

These tests capture existing behavior of campaign iteration control and stop conditions
to prevent regressions during refactoring.
"""

from typing import Any, Dict, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from matterstack.campaign.engine import Campaign, CampaignState
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.results import WorkflowResult


class MockWorkflowResult:
    """Mock workflow result for testing."""
    
    def __init__(self, status: str = "COMPLETED", task_statuses: Optional[Dict[str, str]] = None):
        self.status = status
        self.task_statuses = task_statuses or {}


class SimpleCampaign(Campaign):
    """Simple campaign implementation for testing."""
    
    def __init__(self, max_iterations: int = 3):
        super().__init__()
        self.max_iterations = max_iterations
        self.plan_calls = 0
        self.analyze_calls = 0
        self.analyze_results_history = []
    
    def plan(self) -> Optional[Workflow]:
        self.plan_calls += 1
        if self.plan_calls > self.max_iterations:
            return None
        
        task = Task(
            task_id=f"task_iter_{self.state.iteration + 1}",
            image="test:latest",
            command="echo test"
        )
        workflow = Workflow()
        workflow.add_task(task)
        return workflow
    
    def analyze(self, result: WorkflowResult) -> None:
        self.analyze_calls += 1
        self.analyze_results_history.append(result)


class StoppingCampaign(Campaign):
    """Campaign that stops after a condition is met."""
    
    def __init__(self, stop_after: int = 2):
        super().__init__()
        self.stop_after = stop_after
        self.plan_calls = 0
    
    def plan(self) -> Optional[Workflow]:
        self.plan_calls += 1
        task = Task(task_id=f"task_{self.plan_calls}", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        return workflow
    
    def analyze(self, result: WorkflowResult) -> None:
        if self.state.iteration + 1 >= self.stop_after:
            self.state.stopped = True


class NeverStopCampaign(Campaign):
    """Campaign that never sets stopped flag (for testing max_iterations)."""
    
    def plan(self) -> Optional[Workflow]:
        task = Task(task_id=f"task_{self.state.iteration}", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        return workflow
    
    def analyze(self, result: WorkflowResult) -> None:
        pass  # Never stops


class ImmediateNullPlanCampaign(Campaign):
    """Campaign that returns None from plan immediately."""
    
    def plan(self) -> Optional[Workflow]:
        return None
    
    def analyze(self, result: WorkflowResult) -> None:
        pass


class DataStoringCampaign(Campaign):
    """Campaign that stores data in state."""
    
    def __init__(self):
        super().__init__()
        self.collected_data = []
    
    def plan(self) -> Optional[Workflow]:
        if len(self.collected_data) >= 3:
            return None
        task = Task(task_id=f"task_{self.state.iteration}", image="test:latest", command="echo test")
        workflow = Workflow()
        workflow.add_task(task)
        return workflow
    
    def analyze(self, result: WorkflowResult) -> None:
        self.collected_data.append(f"iteration_{self.state.iteration}")
        self.state.data = {"history": self.collected_data.copy()}


class TestCampaignState:
    """Tests for CampaignState dataclass."""

    def test_default_values(self):
        """CampaignState should have sensible defaults."""
        state = CampaignState()
        
        assert state.iteration == 0
        assert state.data is None
        assert state.stopped is False

    def test_can_set_iteration(self):
        """Should be able to set iteration value."""
        state = CampaignState(iteration=5)
        assert state.iteration == 5

    def test_can_set_data(self):
        """Should be able to set data value."""
        state = CampaignState(data={"key": "value"})
        assert state.data == {"key": "value"}

    def test_can_set_stopped(self):
        """Should be able to set stopped flag."""
        state = CampaignState(stopped=True)
        assert state.stopped is True


class TestCampaignAbstract:
    """Tests for Campaign abstract base class."""

    def test_initializes_with_default_state(self):
        """Campaign should initialize with default CampaignState."""
        campaign = SimpleCampaign()
        
        assert campaign.state is not None
        assert campaign.state.iteration == 0
        assert campaign.state.stopped is False

    def test_should_stop_returns_stopped_flag(self):
        """should_stop() should return state.stopped value."""
        campaign = SimpleCampaign()
        
        assert campaign.should_stop() is False
        
        campaign.state.stopped = True
        assert campaign.should_stop() is True


class TestCampaignRun:
    """Tests for Campaign.run() method."""

    @patch("matterstack.campaign.engine.run_workflow")
    def test_respects_max_iterations(self, mock_run_workflow):
        """Should stop after max_iterations even if campaign wants to continue."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = NeverStopCampaign()
        
        final_state = campaign.run(max_iterations=5)
        
        assert final_state.iteration == 5
        assert mock_run_workflow.call_count == 5

    @patch("matterstack.campaign.engine.run_workflow")
    def test_stops_when_should_stop_true(self, mock_run_workflow):
        """Should stop when should_stop() returns True."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = StoppingCampaign(stop_after=2)
        
        final_state = campaign.run(max_iterations=10)
        
        # Should stop after 2 iterations
        assert final_state.iteration == 2
        assert mock_run_workflow.call_count == 2

    @patch("matterstack.campaign.engine.run_workflow")
    def test_stops_when_plan_returns_none(self, mock_run_workflow):
        """Should stop when plan() returns None."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = SimpleCampaign(max_iterations=3)
        
        final_state = campaign.run(max_iterations=10)
        
        # Should stop after 3 iterations (plan returns None on 4th call)
        assert final_state.iteration == 3
        assert mock_run_workflow.call_count == 3

    @patch("matterstack.campaign.engine.run_workflow")
    def test_stops_immediately_if_first_plan_is_none(self, mock_run_workflow):
        """Should stop immediately if first plan() returns None."""
        campaign = ImmediateNullPlanCampaign()
        
        final_state = campaign.run(max_iterations=10)
        
        assert final_state.iteration == 0
        assert mock_run_workflow.call_count == 0

    @patch("matterstack.campaign.engine.run_workflow")
    def test_calls_plan_then_execute_then_analyze(self, mock_run_workflow):
        """Should follow plan -> execute -> analyze sequence."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = SimpleCampaign(max_iterations=1)
        
        campaign.run(max_iterations=10)
        
        # Should have called plan once (plus one more that returned None)
        assert campaign.plan_calls == 2  # One successful + one that returned None
        # Should have called analyze once
        assert campaign.analyze_calls == 1
        # Should have executed workflow once
        assert mock_run_workflow.call_count == 1

    @patch("matterstack.campaign.engine.run_workflow")
    def test_increments_iteration_after_analyze(self, mock_run_workflow):
        """Should increment iteration counter after each analyze phase."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = SimpleCampaign(max_iterations=3)
        
        final_state = campaign.run(max_iterations=10)
        
        assert final_state.iteration == 3

    @patch("matterstack.campaign.engine.run_workflow")
    def test_passes_workflow_to_run_workflow(self, mock_run_workflow):
        """Should pass the planned workflow to run_workflow."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = SimpleCampaign(max_iterations=1)
        
        campaign.run(max_iterations=10)
        
        # Verify run_workflow was called with a workflow
        call_args = mock_run_workflow.call_args
        assert call_args is not None
        workflow_arg = call_args[0][0]  # First positional arg
        assert isinstance(workflow_arg, Workflow)

    @patch("matterstack.campaign.engine.run_workflow")
    def test_passes_result_to_analyze(self, mock_run_workflow):
        """Should pass workflow result to analyze method."""
        expected_result = MockWorkflowResult(status="COMPLETED")
        mock_run_workflow.return_value = expected_result
        
        campaign = SimpleCampaign(max_iterations=1)
        
        campaign.run(max_iterations=10)
        
        assert len(campaign.analyze_results_history) == 1
        assert campaign.analyze_results_history[0] is expected_result

    @patch("matterstack.campaign.engine.run_workflow")
    def test_returns_final_state(self, mock_run_workflow):
        """Should return the final CampaignState."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = SimpleCampaign(max_iterations=2)
        
        final_state = campaign.run(max_iterations=10)
        
        assert isinstance(final_state, CampaignState)
        assert final_state is campaign.state

    @patch("matterstack.campaign.engine.run_workflow")
    def test_state_persists_across_iterations(self, mock_run_workflow):
        """State should persist and accumulate across iterations."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = DataStoringCampaign()
        
        final_state = campaign.run(max_iterations=10)
        
        assert final_state.data is not None
        assert "history" in final_state.data
        assert len(final_state.data["history"]) == 3

    @patch("matterstack.campaign.engine.run_workflow")
    def test_backend_passed_to_run_workflow(self, mock_run_workflow):
        """Should pass backend parameter to run_workflow."""
        mock_run_workflow.return_value = MockWorkflowResult()
        mock_backend = MagicMock()
        
        campaign = SimpleCampaign(max_iterations=1)
        
        campaign.run(max_iterations=10, backend=mock_backend)
        
        # Verify backend was passed
        call_kwargs = mock_run_workflow.call_args[1]
        assert "backend" in call_kwargs
        assert call_kwargs["backend"] is mock_backend


class TestCampaignStopConditions:
    """Tests for various stop condition scenarios."""

    @patch("matterstack.campaign.engine.run_workflow")
    def test_zero_max_iterations(self, mock_run_workflow):
        """Should not execute any iterations with max_iterations=0."""
        campaign = SimpleCampaign(max_iterations=10)
        
        final_state = campaign.run(max_iterations=0)
        
        assert final_state.iteration == 0
        assert mock_run_workflow.call_count == 0

    @patch("matterstack.campaign.engine.run_workflow")
    def test_one_max_iteration(self, mock_run_workflow):
        """Should execute exactly one iteration with max_iterations=1."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = NeverStopCampaign()
        
        final_state = campaign.run(max_iterations=1)
        
        assert final_state.iteration == 1
        assert mock_run_workflow.call_count == 1

    @patch("matterstack.campaign.engine.run_workflow")
    def test_stop_condition_checked_before_plan(self, mock_run_workflow):
        """should_stop() should be checked at start of each iteration."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        # Campaign that's already stopped
        campaign = SimpleCampaign(max_iterations=10)
        campaign.state.stopped = True
        
        final_state = campaign.run(max_iterations=10)
        
        assert final_state.iteration == 0
        assert mock_run_workflow.call_count == 0

    @patch("matterstack.campaign.engine.run_workflow")
    def test_analyze_can_trigger_stop(self, mock_run_workflow):
        """analyze() setting stopped=True should stop next iteration."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = StoppingCampaign(stop_after=3)
        
        final_state = campaign.run(max_iterations=10)
        
        # Should run 3 iterations, then stop
        assert final_state.iteration == 3
        assert final_state.stopped is True


class TestCampaignEdgeCases:
    """Edge case tests for Campaign."""

    @patch("matterstack.campaign.engine.run_workflow")
    def test_handles_empty_workflow(self, mock_run_workflow):
        """Should handle workflow with no tasks."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        class EmptyWorkflowCampaign(Campaign):
            def __init__(self):
                super().__init__()
                self.plan_count = 0
            
            def plan(self) -> Optional[Workflow]:
                self.plan_count += 1
                if self.plan_count > 2:
                    return None
                return Workflow()  # Empty workflow
            
            def analyze(self, result: WorkflowResult) -> None:
                pass
        
        campaign = EmptyWorkflowCampaign()
        final_state = campaign.run(max_iterations=10)
        
        assert final_state.iteration == 2

    @patch("matterstack.campaign.engine.run_workflow")
    def test_preserves_state_on_early_stop(self, mock_run_workflow):
        """State should be preserved when campaign stops early."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = DataStoringCampaign()
        campaign.state.data = {"initial": "value"}
        
        # Stop after 1 iteration
        campaign.state.stopped = True
        
        final_state = campaign.run(max_iterations=10)
        
        # Initial data should still be there
        assert final_state.data == {"initial": "value"}

    def test_can_subclass_campaign(self):
        """Should be able to create concrete Campaign subclass."""
        class MyCampaign(Campaign):
            def plan(self) -> Optional[Workflow]:
                return None
            
            def analyze(self, result: WorkflowResult) -> None:
                pass
        
        campaign = MyCampaign()
        assert isinstance(campaign, Campaign)

    def test_must_implement_abstract_methods(self):
        """Subclass must implement plan() and analyze()."""
        # This should raise TypeError at instantiation
        class IncompleteCampaign(Campaign):
            pass
        
        with pytest.raises(TypeError):
            IncompleteCampaign()

    @patch("matterstack.campaign.engine.run_workflow")
    def test_multiple_runs_accumulate_iteration(self, mock_run_workflow):
        """Multiple calls to run() should accumulate iterations if state is shared."""
        mock_run_workflow.return_value = MockWorkflowResult()
        
        campaign = NeverStopCampaign()
        
        # First run
        campaign.run(max_iterations=3)
        assert campaign.state.iteration == 3
        
        # Second run continues from where it left off
        # Need to use a higher max_iterations since loop checks state.iteration < max_iterations
        campaign.run(max_iterations=5)
        assert campaign.state.iteration == 5
