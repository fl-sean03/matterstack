from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.run_lifecycle import initialize_run, list_active_runs, step_run
from matterstack.storage.state_store import SQLiteStateStore


class MockCampaign(Campaign):
    def plan(self, state):
        if state is None:
            wf = Workflow()
            t1 = Task(image="ubuntu", command="echo 1", task_id="task_1")
            wf.add_task(t1)
            return wf
        return None # Done after 1 task

    def analyze(self, state, results):
        return {"done": True}

def test_scheduler_multi_run_progress(tmp_path):
    """
    Verify that multiple runs progress to completion when ticked.
    """
    campaign = MockCampaign()
    runs = []

    # Initialize 3 runs in different workspaces
    for i in range(3):
        # We use subdirectory structure compliant with list_active_runs
        # tmp_path/ws_{i}/runs/{run_id}
        ws_path = tmp_path / f"ws_{i}"
        ws_path.mkdir(parents=True)
        h = initialize_run(f"ws_{i}", campaign, base_path=tmp_path)
        runs.append(h)

    # Verify discovery finds them
    active = list_active_runs(tmp_path)
    assert len(active) == 3

    # Simulate Scheduler Loop
    # We expect:
    # Tick 1: Submit task
    # Tick 2: Detect completion -> Analyze -> Plan(None) -> Complete

    max_ticks = 10

    for _ in range(max_ticks):
        active = list_active_runs(tmp_path)
        if not active:
            break

        for handle in active:
            # We assume campaign is same for all in this test
            step_run(handle, campaign)

    # Verification
    for h in runs:
        store = SQLiteStateStore(h.db_path)
        status = store.get_run_status(h.run_id)
        assert status == "COMPLETED", f"Run {h.run_id} did not complete. Status: {status}"

def test_scheduler_skips_locked_run(tmp_path):
    """
    Verify that if a run is locked, the scheduler skips it and processes others.
    """
    campaign = MockCampaign()

    # Setup runs
    ws1 = tmp_path / "ws_1"
    ws1.mkdir()
    h1 = initialize_run("ws_1", campaign, base_path=tmp_path)

    ws2 = tmp_path / "ws_2"
    ws2.mkdir()
    h2 = initialize_run("ws_2", campaign, base_path=tmp_path)

    # Manually lock Run 1
    store1 = SQLiteStateStore(h1.db_path)

    with store1.lock():
        # Run 1 is now locked by this test process.

        # Simulate Scheduler Logic
        active = list_active_runs(tmp_path)
        assert len(active) == 2

        processed = []
        skipped = []

        for handle in active:
            # Scheduler logic: Call step_run directly. It handles locking.
            # If locked, it raises RuntimeError.
            try:
                step_run(handle, campaign)
                processed.append(handle.run_id)
            except RuntimeError as e:
                if "Could not acquire lock" in str(e):
                    skipped.append(handle.run_id)
                else:
                    raise e

        # Run 1 should be skipped
        assert h1.run_id in skipped
        # Run 2 should be processed
        assert h2.run_id in processed

    # After releasing lock, Run 1 should be processable
    step_run(h1, campaign)

    store = SQLiteStateStore(h1.db_path)
    status = store.get_run_status(h1.run_id)
    # It should have moved from PENDING to RUNNING or active
    # (One step: PENDING -> RUNNING -> Submit Tasks)
    assert status == "RUNNING"
