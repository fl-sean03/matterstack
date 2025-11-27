import pytest
import shutil
import json
from pathlib import Path
from typing import Generator

from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.core.operators import ExternalRunStatus
from matterstack.runtime.operators.human import HumanOperator
from matterstack.runtime.operators.experiment import ExperimentOperator

@pytest.fixture
def temp_run_handle(tmp_path: Path) -> Generator[RunHandle, None, None]:
    """Create a temporary RunHandle."""
    run_root = tmp_path / "runs" / "test_run_001"
    run_root.mkdir(parents=True)
    
    handle = RunHandle(
        workspace_slug="test_ws",
        run_id="test_run_001",
        root_path=run_root
    )
    yield handle
    # Cleanup is automatic by tmp_path, but good practice to be explicit if needed

def test_human_operator_lifecycle(temp_run_handle: RunHandle):
    """
    Test the full lifecycle of a HumanOperator task.
    Prepare -> Submit -> Check (Waiting) -> Interact -> Check (Completed) -> Collect
    """
    operator = HumanOperator()
    
    # 1. Define Task
    task = Task(
        image="human",
        command="think",
        env={"INSTRUCTIONS": "Please approve this test."},
        task_id="human_task_1"
    )
    
    # 2. Prepare
    handle = operator.prepare_run(temp_run_handle, task)
    assert handle.status == ExternalRunStatus.CREATED
    assert handle.operator_type == "Human"
    
    # Verify instructions.md created
    op_path = Path(handle.operator_data["absolute_path"])
    assert (op_path / "instructions.md").exists()
    assert "Please approve this test" in (op_path / "instructions.md").read_text()
    
    # 3. Submit
    handle = operator.submit(handle)
    assert handle.status == ExternalRunStatus.WAITING_EXTERNAL
    
    # 4. Check Status (Should be Waiting)
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.WAITING_EXTERNAL
    
    # 5. Simulate Human Interaction (Create response.json)
    response_data = {
        "status": "COMPLETED",
        "data": {"decision": "approved", "comment": "looks good"}
    }
    with open(op_path / "response.json", "w") as f:
        json.dump(response_data, f)
        
    # Create an extra file to test collection
    with open(op_path / "notes.txt", "w") as f:
        f.write("Some notes from the human.")

    # 6. Check Status (Should be Completed)
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED
    
    # 7. Collect Results
    result = operator.collect_results(handle)
    assert result.status == ExternalRunStatus.COMPLETED
    assert result.data["decision"] == "approved"
    assert "notes.txt" in result.files

def test_experiment_operator_lifecycle(temp_run_handle: RunHandle):
    """
    Test the lifecycle of an ExperimentOperator task.
    """
    operator = ExperimentOperator()
    
    # 1. Define Task
    task = Task(
        image="robot",
        command="mix",
        env={"EXPERIMENT_CONFIG": '{"temperature": 100, "speed": 50}'},
        task_id="exp_task_1"
    )
    
    # 2. Prepare
    handle = operator.prepare_run(temp_run_handle, task)
    assert handle.status == ExternalRunStatus.CREATED
    
    # Verify experiment_request.json created
    op_path = Path(handle.operator_data["absolute_path"])
    req_file = op_path / "experiment_request.json"
    assert req_file.exists()
    
    with open(req_file) as f:
        req = json.load(f)
        assert req["config"]["temperature"] == 100
        
    # 3. Submit -> Wait
    handle = operator.submit(handle)
    assert handle.status == ExternalRunStatus.WAITING_EXTERNAL
    
    # 4. Simulate Experiment Result
    result_data = {
        "status": "COMPLETED",
        "data": {"yield": 0.95}
    }
    with open(op_path / "experiment_result.json", "w") as f:
        json.dump(result_data, f)
        
    # 5. Check Status & Collect
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED
    
    result = operator.collect_results(handle)
    assert result.data["yield"] == 0.95