import pytest
import shutil
import tempfile
import json
from pathlib import Path

from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.runtime.operators.manual_hpc import ManualHPCOperator
from matterstack.runtime.operators.human import HumanOperator

@pytest.fixture
def run_handle():
    path = Path(tempfile.mkdtemp())
    run_root = path / "workspace" / "runs" / "test_run"
    run_root.mkdir(parents=True)
    handle = RunHandle(
        workspace_slug="workspace",
        run_id="test_run",
        root_path=run_root
    )
    yield handle
    shutil.rmtree(path)

def test_manual_hpc_malformed_status(run_handle):
    op = ManualHPCOperator()
    task = Task(task_id="t1", image="ubuntu", command="echo 1")
    
    # Prepare
    handle = op.prepare_run(run_handle, task)
    op.submit(handle)
    
    # Inject Malformed status.json
    op_path = Path(handle.operator_data["absolute_path"])
    status_file = op_path / "status.json"
    with open(status_file, "w") as f:
        f.write("{ invalid json")
        
    # Check Status - Should handle gracefully (log warning, stay waiting)
    # The implementation logs warning and returns handle as is.
    handle = op.check_status(handle)
    assert handle.status == ExternalRunStatus.FAILED

def test_manual_hpc_output_files_fallback(run_handle):
    op = ManualHPCOperator()
    task = Task(task_id="t2", image="ubuntu", command="echo 1")
    
    # Prepare
    handle = op.prepare_run(run_handle, task)
    op.submit(handle)
    
    # No status.json, but files in output
    op_path = Path(handle.operator_data["absolute_path"])
    output_dir = op_path / "output"
    output_dir.mkdir(exist_ok=True)
    (output_dir / "result.txt").write_text("done")
    
    # Check Status - Should detect files and Complete
    handle = op.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED

def test_human_operator_malformed_response(run_handle):
    op = HumanOperator()
    task = Task(task_id="h1", image="ubuntu", command="echo 1")
    
    # Prepare
    handle = op.prepare_run(run_handle, task)
    op.submit(handle)
    
    # Inject Malformed response.json
    op_path = Path(handle.operator_data["absolute_path"])
    resp_file = op_path / "response.json"
    with open(resp_file, "w") as f:
        f.write("{ invalid json")
        
    # Check Status - Should handle gracefully
    handle = op.check_status(handle)
    assert handle.status == ExternalRunStatus.FAILED

def test_human_operator_valid_response(run_handle):
    op = HumanOperator()
    task = Task(task_id="h2", image="ubuntu", command="echo 1")
    
    # Prepare
    handle = op.prepare_run(run_handle, task)
    op.submit(handle)
    
    # Inject Valid response.json
    op_path = Path(handle.operator_data["absolute_path"])
    resp_file = op_path / "response.json"
    with open(resp_file, "w") as f:
        f.write(json.dumps({"status": "COMPLETED", "data": {"rating": 5}}))
        
    # Check Status
    handle = op.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED
    
    # Collect Results
    result = op.collect_results(handle)
    assert result.status == ExternalRunStatus.COMPLETED
    assert result.data["rating"] == 5
