import pytest
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Any
from unittest.mock import patch, MagicMock

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.core.external import ExternalTask
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.core.operators import ExternalRunStatus

class MockExternalCampaign(Campaign):
    def plan(self, state: Any) -> Optional[Workflow]:
        if state is None:
            wf = Workflow()
            t = ExternalTask(
                task_id="ext_1", 
                image="ubuntu:latest", 
                command="echo 'hello'"
            )
            wf.add_task(t)
            return wf
        return None

    def analyze(self, state: Any, results: Any) -> Any:
        return {"done": True}

@pytest.fixture
def workspace_path():
    path = Path(tempfile.mkdtemp())
    yield path
    shutil.rmtree(path)

def test_idempotency_external_task(workspace_path):
    """
    Verify that calling step_run multiple times on a waiting task
    does not cause re-submission or errors.
    """
    campaign = MockExternalCampaign()
    run_handle = initialize_run("test_ws", campaign, base_path=workspace_path)
    
    # Initial State: Task Created (PENDING)
    store = SQLiteStateStore(run_handle.db_path)
    assert store.get_task_status("ext_1") == "PENDING"
    
    # Step 1: Submit Task
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"
    
    # Verify Submission
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"
    ext_run = store.get_external_run("ext_1")
    assert ext_run is not None
    assert ext_run.status == ExternalRunStatus.WAITING_EXTERNAL
    
    # Capture timestamp or ID to verify it doesn't change
    initial_ext_id = ext_run.external_id
    
    # Step 2: Call step_run again (Idempotency)
    # Task is WAITING_EXTERNAL, so it should be skipped in EXECUTE phase
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"
    
    # Verify no changes
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"
    ext_run_2 = store.get_external_run("ext_1")
    # Since we didn't change it, it should be same
    assert ext_run_2.status == ExternalRunStatus.WAITING_EXTERNAL

def test_crash_recovery_partial_submission(workspace_path):
    """
    Simulate a crash between registering the external run and updating the task status.
    Verify that the next step_run recovers and completes the submission.
    """
    campaign = MockExternalCampaign()
    run_handle = initialize_run("crash_ws", campaign, base_path=workspace_path)
    
    store = SQLiteStateStore(run_handle.db_path)
    
    # Mock update_task_status to fail ONCE
    original_update = store.update_task_status
    
    # Alternative: Subclass for testing
    class FlakyStore(SQLiteStateStore):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            print(f"FlakyStore initialized. should_fail={getattr(self, 'should_fail', 'unset')}")

        def update_task_status(self, task_id, status):
            print(f"FlakyStore.update_task_status({task_id}, {status})")
            # Only fail for the specific transition we care about
            if status == "WAITING_EXTERNAL" and getattr(self, "should_fail", False):
                print("TRIGGERING CRASH")
                self.should_fail = False # Reset
                raise RuntimeError("Simulated Crash during DB Update")
            super().update_task_status(task_id, status)

    # Patch the class in the module
    with patch('matterstack.orchestration.run_lifecycle.SQLiteStateStore', side_effect=FlakyStore):
        # We need to set the flag on the instance. 
        # But `step_run` creates a new instance.
        # We can use `side_effect` on __init__ to set the flag?
        # Or just use a class attribute for the test
        FlakyStore.should_fail = True
        
        # Step 1: Should Crash
        with pytest.raises(RuntimeError, match="Simulated Crash"):
            step_run(run_handle, campaign)
            
    # Check State:
    # 1. External Run registered? (Yes, happens before update_task_status)
    ext_run = store.get_external_run("ext_1")
    assert ext_run is not None
    assert ext_run.status == ExternalRunStatus.WAITING_EXTERNAL
    
    # 2. Task Status? (Should still be None/Pending because update failed)
    assert store.get_task_status("ext_1") == "PENDING"
    
    # Step 2: Recovery
    # Run normally (unpatched)
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"
    
    # Verify consistency
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"
    ext_run = store.get_external_run("ext_1")
    assert ext_run.status == ExternalRunStatus.WAITING_EXTERNAL
    
    print("Crash recovery successful: System healed inconsistent state.")
