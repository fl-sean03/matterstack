from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore


class MockCampaign(Campaign):
    def __init__(self):
        self.step_count = 0

    def plan(self, state):
        # First call (init)
        if state is None:
            wf = Workflow()
            t1 = Task(image="ubuntu", command="echo 1", task_id="task_1")
            wf.add_task(t1)
            return wf

        # Second call (after task_1 done)
        if self.step_count == 1:
            wf = Workflow()
            t2 = Task(image="ubuntu", command="echo 2", task_id="task_2")
            wf.add_task(t2)
            return wf

        # Third call (after task_2 done) - Finish
        return None

    def analyze(self, state, results):
        self.step_count += 1
        return {"count": self.step_count}

def test_run_lifecycle_complete_flow(tmp_path):
    """
    Test the full lifecycle: Init -> Step (Submit) -> Mock Complete -> Step (Analyze/Plan) -> ... -> Complete
    """
    workspace = "test_ws"
    campaign = MockCampaign()

    # 1. Initialize
    run_handle = initialize_run(workspace, campaign, base_path=tmp_path)

    assert run_handle.db_path.exists()

    store = SQLiteStateStore(run_handle.db_path)
    tasks = store.get_tasks(run_handle.run_id)
    assert len(tasks) == 1
    assert tasks[0].task_id == "task_1"

    # 2. Step 1: Submit task_1
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"

    # Verify task_1 is COMPLETED (Simulation Mode)
    assert store.get_task_status("task_1") == "COMPLETED"

    # 3. Simulate task_1 completion (Already done in step_run for local tasks)
    # store.update_task_status("task_1", "COMPLETED")

    # 4. Step 2: Analyze task_1 -> Plan task_2
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"

    # Verify task_2 exists and is created (or SUBMITTED if loop ran far enough?)
    # step_run does: Poll -> Plan (find ready) -> Execute.
    # When we called step_run above, it found task_1 COMPLETED.
    # It then saw no active tasks.
    # It ran Analyze -> Plan -> Added task_2.
    # Then it returned "active".
    # It did NOT execute task_2 in the same tick because the tasks list was fetched at start of function!
    # This is correct behavior (tick based). New work is picked up next tick.

    tasks = store.get_tasks(run_handle.run_id)
    assert len(tasks) == 2 # task_1 and task_2
    assert store.get_task_status("task_2") == "PENDING" # Not yet submitted

    # 5. Step 3: Submit task_2
    status = step_run(run_handle, campaign)
    assert status == "RUNNING"
    assert store.get_task_status("task_2") == "COMPLETED"

    # 6. Simulate task_2 completion
    # store.update_task_status("task_2", "COMPLETED")

    # 7. Step 4: Analyze task_2 -> Plan (None) -> Complete
    status = step_run(run_handle, campaign)
    assert status == "COMPLETED"
