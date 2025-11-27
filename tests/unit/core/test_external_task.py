import json
import time
import threading
from pathlib import Path
from matterstack.core.workflow import Workflow
from matterstack.core.external import ExternalTask
from matterstack.orchestration.api import run_workflow
from matterstack.core.backend import JobState
from matterstack.runtime.backends.local import LocalBackend

def test_external_task_success(tmp_path):
    """
    Test that ExternalTask writes a request and waits for a response.
    We simulate the external agent using a background thread.
    """
    workspace = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace))
    
    task = ExternalTask(
        task_id="ext_task",
        image="ubuntu",
        command="", # overwritten by __post_init__
        request_data={"command": "move_arm", "x": 10},
        poll_interval=0.5
    )
    
    # Ensure subprocess can import matterstack
    import os
    # We need to point to the project root.
    # Current file: tests/unit/core/test_external_task.py
    # Root: ../../../
    root_dir = str(Path(__file__).resolve().parent.parent.parent.parent)
    task.env["PYTHONPATH"] = root_dir

    wf = Workflow()
    wf.add_task(task)
    
    # Background thread to act as the "Robot"
    def robot_simulator():
        # Wait for request file
        task_dir = workspace / "ext_task"
        req_path = task_dir / "request.json"
        
        # Wait up to 5 seconds for request to appear
        for _ in range(50):
            if req_path.exists():
                break
            time.sleep(0.1)
        
        if not req_path.exists():
            return # Failed to see request
            
        # Read request
        with open(req_path) as f:
            req = json.load(f)
            
        # Simulate work
        time.sleep(1)
        
        # Write response
        resp_path = task_dir / "response.json"
        with open(resp_path, "w") as f:
            json.dump({
                "status": "success", 
                "result": f"Moved to {req['x']}"
            }, f)
            
    robot_thread = threading.Thread(target=robot_simulator)
    robot_thread.start()
    
    # Run Workflow
    result = run_workflow(wf, backend=backend)
    
    robot_thread.join()
    
    assert result.status == JobState.COMPLETED_OK
    assert result.tasks["ext_task"].status.state == JobState.COMPLETED_OK
    
    # Verify logs show success
    logs = result.tasks["ext_task"].logs.stdout + result.tasks["ext_task"].logs.stderr
    assert "Response file found" in logs
    assert "External task completed successfully" in logs


def test_external_task_failure(tmp_path):
    """
    Test that ExternalTask handles failure response correctly.
    """
    workspace = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace))
    
    task = ExternalTask(
        task_id="ext_task_fail",
        image="ubuntu",
        command="", # overwritten by __post_init__
        request_data={"op": "bad_op"},
        poll_interval=0.5
    )
    
    # Ensure subprocess can import matterstack
    import os
    # We need to point to the project root.
    # Current file: tests/unit/core/test_external_task.py
    # Root: ../../../
    root_dir = str(Path(__file__).resolve().parent.parent.parent.parent)
    task.env["PYTHONPATH"] = root_dir

    wf = Workflow()
    wf.add_task(task)
    
    def robot_simulator_fail():
        task_dir = workspace / "ext_task_fail"
        req_path = task_dir / "request.json"
        
        while not req_path.exists():
            time.sleep(0.1)
            
        time.sleep(0.5)
        
        resp_path = task_dir / "response.json"
        with open(resp_path, "w") as f:
            json.dump({
                "status": "failed", 
                "reason": "Invalid Operation"
            }, f)
            
    robot_thread = threading.Thread(target=robot_simulator_fail)
    robot_thread.start()
    
    result = run_workflow(wf, backend=backend, continue_on_error=True)
    robot_thread.join()
    
    assert result.status == JobState.COMPLETED_ERROR
    assert result.tasks["ext_task_fail"].status.state == JobState.COMPLETED_ERROR
    assert "Invalid Operation" in result.tasks["ext_task_fail"].logs.stderr