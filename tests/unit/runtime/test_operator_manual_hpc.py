import json
from pathlib import Path

import pytest

from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.runtime.operators.manual_hpc import ManualHPCOperator


@pytest.fixture
def run_handle(tmp_path):
    """Create a temporary RunHandle."""
    run_dir = tmp_path / "runs" / "test_run_manual"
    run_dir.mkdir(parents=True)
    return RunHandle(
        workspace_slug="test_ws",
        run_id="test_run_manual",
        root_path=run_dir
    )

@pytest.fixture
def manual_operator():
    return ManualHPCOperator()

def test_manual_operator_lifecycle(run_handle, manual_operator):
    # 1. Prepare
    task = Task(
        task_id="task_manual_1",
        image="ubuntu:latest",
        command="echo hello"
    )
    handle = manual_operator.prepare_run(run_handle, task)

    assert handle.status == ExternalRunStatus.CREATED
    assert handle.operator_data["operator_uuid"] is not None

    # Verify directory structure
    op_path = Path(handle.operator_data["absolute_path"])
    assert op_path.exists()
    assert (op_path / "manifest.json").exists()
    assert (op_path / "job_template.sh").exists()
    assert (op_path / "output").exists()

    # 2. Submit (should transition to WAITING_EXTERNAL)
    handle = manual_operator.submit(handle)
    assert handle.status == ExternalRunStatus.WAITING_EXTERNAL

    # 3. Check Status (Should still be WAITING_EXTERNAL initially)
    handle = manual_operator.check_status(handle)
    assert handle.status == ExternalRunStatus.WAITING_EXTERNAL

    # 4. Simulate User Action: Write output file
    output_file = op_path / "output" / "results.csv"
    output_file.write_text("a,b,c\n1,2,3")

    # Check Status (Should detect file and complete)
    handle = manual_operator.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED

    # 5. Collect Results
    result = manual_operator.collect_results(handle)
    assert result.status == ExternalRunStatus.COMPLETED
    assert "results.csv" in result.files
    assert result.files["results.csv"].name == "results.csv"

def test_manual_operator_status_json(run_handle, manual_operator):
    """Test completion via status.json explicitly."""
    task = Task(
        task_id="task_manual_2",
        image="ubuntu:latest",
        command="echo hello"
    )
    handle = manual_operator.prepare_run(run_handle, task)
    handle = manual_operator.submit(handle)

    op_path = Path(handle.operator_data["absolute_path"])

    # Write status.json
    status_file = op_path / "status.json"
    with open(status_file, "w") as f:
        json.dump({"status": "COMPLETED"}, f)

    handle = manual_operator.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED

def test_manual_operator_failure(run_handle, manual_operator):
    """Test failure signaling via status.json."""
    task = Task(
        task_id="task_manual_3",
        image="ubuntu:latest",
        command="echo hello"
    )
    handle = manual_operator.prepare_run(run_handle, task)
    handle = manual_operator.submit(handle)

    op_path = Path(handle.operator_data["absolute_path"])

    # Write status.json with failure
    status_file = op_path / "status.json"
    with open(status_file, "w") as f:
        json.dump({"status": "FAILED", "error": "Something went wrong"}, f)

    handle = manual_operator.check_status(handle)
    assert handle.status == ExternalRunStatus.FAILED
    assert handle.operator_data["error"] == "Something went wrong"
