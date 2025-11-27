import pytest
from typing import Optional
from matterstack.campaign.engine import Campaign, CampaignState
from matterstack.core.workflow import Workflow, Task
from matterstack.orchestration.results import WorkflowResult, TaskResult, JobStatus, JobState
from matterstack.orchestration.api import TaskLogs

class DummyTask(Task):
    """Simple task that does nothing."""
    def __init__(self, task_id):
        super().__init__(image="alpine", command="echo hello", task_id=task_id)

class CountingCampaign(Campaign):
    def __init__(self, limit=3):
        super().__init__()
        self.limit = limit
        self.plan_count = 0
        self.analyze_count = 0
        
    def plan(self) -> Optional[Workflow]:
        if self.plan_count >= self.limit:
            return None
        
        self.plan_count += 1
        wf = Workflow()
        # We need a dummy task. Since we mock run_workflow, the task content doesn't matter much.
        wf.add_task(DummyTask(f"task_{self.plan_count}"))
        return wf
        
    def analyze(self, result: WorkflowResult) -> None:
        self.analyze_count += 1
        # Simple stop condition
        if self.analyze_count >= self.limit:
            self.state.stopped = True

# Mock run_workflow to avoid actual execution
import matterstack.campaign.engine as engine_module
from unittest.mock import MagicMock

@pytest.fixture
def mock_run_workflow(monkeypatch):
    mock = MagicMock()
    
    def side_effect(workflow, backend=None):
        # Return a dummy successful result
        results = {}
        for task_id, task in workflow.tasks.items():
            results[task_id] = TaskResult(
                task=task,
                job_id="mock_job",
                status=JobStatus(job_id="mock_job", state=JobState.COMPLETED_OK),
                logs=TaskLogs(stdout="", stderr=""),
                workspace_path="."
            )
        return WorkflowResult(workflow=workflow, tasks=results)
        
    mock.side_effect = side_effect
    monkeypatch.setattr(engine_module, "run_workflow", mock)
    return mock

def test_campaign_loop(mock_run_workflow):
    campaign = CountingCampaign(limit=3)
    final_state = campaign.run(max_iterations=5)
    
    assert final_state.iteration == 3
    assert final_state.stopped == True
    assert campaign.plan_count == 3
    assert campaign.analyze_count == 3
    assert mock_run_workflow.call_count == 3

def test_campaign_max_iterations(mock_run_workflow):
    # Campaign that never stops itself
    class InfiniteCampaign(Campaign):
        def plan(self):
            wf = Workflow()
            wf.add_task(DummyTask("t"))
            return wf
        def analyze(self, result):
            pass
            
    campaign = InfiniteCampaign()
    final_state = campaign.run(max_iterations=2)
    
    assert final_state.iteration == 2
    assert final_state.stopped == False
    assert mock_run_workflow.call_count == 2