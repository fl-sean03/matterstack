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
from matterstack.core.operators import ExternalRunStatus, ExternalRunHandle

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

    # Verify Submission (task status remains stable for users)
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"

    # v2 primary: attempt record exists (no legacy external_run needed)
    attempts = store.list_attempts("ext_1")
    assert len(attempts) == 1
    assert attempts[0].status == ExternalRunStatus.WAITING_EXTERNAL.value

    # Ensure we didn't create a legacy external_run row for this new run path
    assert store.get_external_run("ext_1") is None

    first_attempt_id = attempts[0].attempt_id

    # Step 2: Call step_run again (Idempotency)
    # Task is WAITING_EXTERNAL, so it should be skipped in EXECUTE phase
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"

    # Verify no changes: still only one attempt
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"
    attempts_2 = store.list_attempts("ext_1")
    assert len(attempts_2) == 1
    assert attempts_2[0].attempt_id == first_attempt_id
    assert attempts_2[0].status == ExternalRunStatus.WAITING_EXTERNAL.value

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
    # 1. Attempt created? (Yes, happens before update_task_status)
    attempts = store.list_attempts("ext_1")
    assert len(attempts) == 1
    assert attempts[0].status == ExternalRunStatus.WAITING_EXTERNAL.value

    # 2. Task Status? (Should still be PENDING because update failed)
    assert store.get_task_status("ext_1") == "PENDING"

    # Step 2: Recovery
    # Run normally (unpatched)
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"

    # Verify consistency (orchestrator heals from active attempt)
    assert store.get_task_status("ext_1") == "WAITING_EXTERNAL"

    attempts_after = store.list_attempts("ext_1")
    assert len(attempts_after) == 1
    assert attempts_after[0].status == ExternalRunStatus.WAITING_EXTERNAL.value

    print("Crash recovery successful: System healed inconsistent state from active attempt.")


def test_second_attempt_preserves_first_attempt_record(workspace_path):
    """
    Integration test (local-only): ensure a "rerun-like" second attempt can exist
    without overwriting the first attempt record.

    Approach:
    - Use a custom operator registry ("HPC") that fails the first submission/poll and
      succeeds the second time.
    - After first failure, reset task status to PENDING (simulating CLI rerun/reset).
    - Next tick should create a second attempt (attempt_index increments).
    """
    class OneFailThenSucceedOperator:
        def __init__(self):
            self.submit_count = 0

        def prepare_run(self, run, task):
            # Minimal handle; orchestrator persists operator_data to attempt
            return ExternalRunHandle(
                task_id=task.task_id,
                operator_type="HPC",
                status=ExternalRunStatus.CREATED,
                operator_data={"prepared": True},
                relative_path=None,
            )

        def submit(self, handle):
            self.submit_count += 1
            if self.submit_count == 1:
                handle.status = ExternalRunStatus.SUBMITTED
                handle.external_id = "job-1"
            else:
                handle.status = ExternalRunStatus.SUBMITTED
                handle.external_id = "job-2"
            return handle

        def check_status(self, handle):
            # First attempt fails, second completes
            if handle.external_id == "job-1":
                handle.status = ExternalRunStatus.FAILED
            elif handle.external_id == "job-2":
                handle.status = ExternalRunStatus.COMPLETED
            return handle

        def collect_results(self, handle):
            # No artifacts needed for this test
            return MagicMock(files={}, data={})

    class OneTaskCampaign(Campaign):
        def plan(self, state: Any) -> Optional[Workflow]:
            if state is None:
                wf = Workflow()
                t = ExternalTask(
                    task_id="ext_rerun_1",
                    image="ubuntu:latest",
                    command="echo 'hello'",
                    env={"MATTERSTACK_OPERATOR": "HPC"},
                )
                wf.add_task(t)

                # Add a dependent "blocker" task so the run doesn't auto-transition to FAILED/COMPLETED
                # when ext_rerun_1 fails; this keeps the run in RUNNING while we simulate a rerun.
                blocker = Task(
                    task_id="blocker_after_ext_rerun_1",
                    image="ubuntu:latest",
                    command="echo blocker",
                    dependencies={"ext_rerun_1"},
                )
                wf.add_task(blocker)
                return wf
            return None

        def analyze(self, state: Any, results: Any) -> Any:
            return {"done": True}

    campaign = OneTaskCampaign()
    run_handle = initialize_run("rerun_ws", campaign, base_path=workspace_path)
    store = SQLiteStateStore(run_handle.db_path)

    operators = {"HPC": OneFailThenSucceedOperator()}

    # Tick 1: submit attempt 1
    status = step_run(run_handle, campaign, operator_registry=operators)
    assert status == "RUNNING"
    assert store.get_task_status("ext_rerun_1") == "WAITING_EXTERNAL"
    attempts = store.list_attempts("ext_rerun_1")
    assert len(attempts) == 1
    assert attempts[0].attempt_index == 1

    # Tick 2: poll -> fail attempt 1
    status = step_run(run_handle, campaign, operator_registry=operators)
    assert status == "RUNNING"
    assert store.get_task_status("ext_rerun_1") == "FAILED"
    attempts = store.list_attempts("ext_rerun_1")
    assert len(attempts) == 1
    first_attempt_id = attempts[0].attempt_id
    first_attempt_status = attempts[0].status
    assert first_attempt_status == ExternalRunStatus.FAILED.value

    # Simulate rerun/reset (CLI behavior happens in later subtasks)
    store.update_task_status("ext_rerun_1", "PENDING")

    # Tick 3: create attempt 2 and submit
    status = step_run(run_handle, campaign, operator_registry=operators)
    assert status == "RUNNING"
    assert store.get_task_status("ext_rerun_1") == "WAITING_EXTERNAL"

    attempts = store.list_attempts("ext_rerun_1")
    assert len(attempts) == 2
    assert attempts[0].attempt_id == first_attempt_id
    assert attempts[0].status == first_attempt_status
    assert attempts[1].attempt_index == 2

    # Tick 4: poll -> complete attempt 2
    status = step_run(run_handle, campaign, operator_registry=operators)
    assert status == "RUNNING"
    assert store.get_task_status("ext_rerun_1") == "COMPLETED"

    attempts = store.list_attempts("ext_rerun_1")
    assert len(attempts) == 2
    assert attempts[0].attempt_id == first_attempt_id
    assert attempts[0].status == ExternalRunStatus.FAILED.value
    assert attempts[1].status == ExternalRunStatus.COMPLETED.value

    current = store.get_current_attempt("ext_rerun_1")
    assert current is not None
    assert current.attempt_id == attempts[1].attempt_id
