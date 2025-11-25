import pytest
import threading
import time
import os
import shutil
from pathlib import Path
from matterstack.core.gate import GateTask
from matterstack.core.workflow import Workflow
from matterstack.orchestration.api import run_workflow
from matterstack.core.backend import JobState
from matterstack.runtime.backends.local import LocalBackend

def cleanup_files(paths):
    for p in paths:
        if os.path.exists(p):
            if os.path.isdir(p):
                shutil.rmtree(p)
            else:
                os.remove(p)

def test_gate_task_approval():
    """Test that GateTask succeeds when approved.txt is created."""
    
    # 1. Setup
    workspace = Path("test_gate_workspace_approve")
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir()
    
    # Add current directory to PYTHONPATH for the subprocess
    env = {"PYTHONPATH": os.getcwd()}
    
    gate_task = GateTask(
        task_id="gate_task_1",
        image="python:3.9",
        poll_interval=0.5,
        env=env
    )
    
    workflow = Workflow()
    workflow.add_task(gate_task)
    
    # 2. Simulate User Action in a separate thread
    def simulate_user():
        time.sleep(2) # Wait for task to start and write info file
        # Check if info file exists (it should be created by the wrapper)
        # Note: LocalBackend runs in a subdirectory typically, but for simple LocalBackend 
        # without profile it might be current dir or job_id dir.
        # Let's inspect where the wrapper writes. The wrapper writes to relative paths.
        # So we need to find where the task is running.
        
        # HACK: In this test environment, we need to know where the task is running.
        # Since we can't easily peek into the running process's CWD from here without complex logic,
        # we'll assume the task runs in the CWD or we look for the file recursively.
        
        # However, run_workflow with LocalBackend runs subprocesses.
        # If we use LocalBackend(workspace_root=...), we know where it is.
        pass

    # Better approach: We run the workflow, and WE are the external agent.
    # But run_workflow is blocking. So we need threading.
    
    backend = LocalBackend(workspace_root=workspace)
    
    def approver_thread():
        # Poll for gate_info.json in the workspace subdirectories
        found = False
        start = time.time()
        target_file = None
        
        while time.time() - start < 10:
            # Recursively search for gate_info.json
            for root, dirs, files in os.walk(workspace):
                if "gate_info.json" in files:
                    target_dir = Path(root)
                    target_file = target_dir / "approved.txt"
                    found = True
                    break
            if found:
                break
            time.sleep(0.5)
            
        if found and target_file:
            time.sleep(1) # simulate think time
            target_file.touch()
            
    t = threading.Thread(target=approver_thread)
    t.start()
    
    # 3. Run Workflow
    result = run_workflow(workflow, backend=backend, poll_interval=0.5)
    
    t.join()
    
    # 4. Assertions
    task_res = result.tasks["gate_task_1"]
    assert task_res.status.state == JobState.COMPLETED
    
    # Cleanup
    shutil.rmtree(workspace)


def test_gate_task_rejection():
    """Test that GateTask fails when rejected.txt is created."""
    
    workspace = Path("test_gate_workspace_reject")
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir()
    
    env = {"PYTHONPATH": os.getcwd()}
    
    gate_task = GateTask(
        task_id="gate_task_2",
        image="python:3.9",
        poll_interval=0.5,
        env=env
    )
    
    workflow = Workflow()
    workflow.add_task(gate_task)
    
    backend = LocalBackend(workspace_root=workspace)
    
    def rejector_thread():
        found = False
        start = time.time()
        target_file = None
        
        while time.time() - start < 10:
            for root, dirs, files in os.walk(workspace):
                if "gate_info.json" in files:
                    target_dir = Path(root)
                    target_file = target_dir / "rejected.txt"
                    found = True
                    break
            if found:
                break
            time.sleep(0.5)
            
        if found and target_file:
            time.sleep(1)
            target_file.touch()
            
    t = threading.Thread(target=rejector_thread)
    t.start()
    
    result = run_workflow(workflow, backend=backend, poll_interval=0.5, continue_on_error=True)
    
    t.join()
    
    task_res = result.tasks["gate_task_2"]
    assert task_res.status.state == JobState.FAILED
    
    shutil.rmtree(workspace)