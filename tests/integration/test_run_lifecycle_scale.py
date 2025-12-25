import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import pytest

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore


class ScaleCampaign(Campaign):
    def __init__(self, num_tasks=100):
        self.num_tasks = num_tasks

    def plan(self, state: Any) -> Optional[Workflow]:
        # If state is None, generate the huge workflow
        if state is None:
            wf = Workflow()

            # Simple chain: 0 -> 1 -> 2 ...
            # Or massive parallel: 0 independent tasks

            # Let's do massive parallel to stress DB inserts
            for i in range(self.num_tasks):
                t = Task(
                    task_id=f"scale_task_{i}",
                    image="ubuntu:latest",
                    command=f"echo 'Task {i}'"
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

def test_scale_100_tasks(workspace_path):
    """
    Generate and execute 100 tasks.
    """
    num_tasks = 100
    campaign = ScaleCampaign(num_tasks=num_tasks)

    start_time = time.time()
    run_handle = initialize_run("scale_ws", campaign, base_path=workspace_path)
    init_duration = time.time() - start_time
    print(f"Initialization with {num_tasks} tasks took {init_duration:.2f}s")

    store = SQLiteStateStore(run_handle.db_path)

    # 1. Step to Submit (All 100 should move to COMPLETED immediately because they are local simulation)
    # The current run_lifecycle implementation marks local tasks as COMPLETED immediately in the submit phase.

    step_start = time.time()
    status = step_run(run_handle, campaign)
    step_duration = time.time() - step_start
    print(f"Step run took {step_duration:.2f}s")

    # Check that it's still active (because it submitted them, but analyze phase happens next tick?
    # Wait, looking at run_lifecycle logic:
    # 1. Poll (no ext)
    # 2. Plan (finds ready tasks)
    # 3. Execute (marks local as COMPLETED)
    # 4. Analyze (checks if all complete. If local ones were marked complete, they are now complete.)
    # The analyze logic re-checks if there are active tasks.
    # If all were local, has_active_tasks might be False if we update them.
    # Logic trace:
    # - tasks_to_run populated.
    # - Execute loop: marks COMPLETED. has_active_tasks not set to true for them.
    # - if not has_active_tasks and not tasks_to_run:
    #   Wait, tasks_to_run IS NOT EMPTY.
    #   So it skips Analyze phase in the same tick.
    #   It returns "active".

    assert status == "RUNNING"

    # 2. Next Step: Should see all completed and Analyze
    analyze_start = time.time()
    status = step_run(run_handle, campaign)
    analyze_duration = time.time() - analyze_start

    # Now tasks_to_run should be empty. has_active_tasks should be False (all completed).
    # Should enter Analyze -> Plan (returns None) -> Complete.
    assert status == "COMPLETED"
    print(f"Analyze step took {analyze_duration:.2f}s")

    # Verify all 100 tasks are completed
    tasks = store.get_tasks(run_handle.run_id)
    assert len(tasks) == num_tasks
    for t in tasks:
        s = store.get_task_status(t.task_id)
        assert s == "COMPLETED"

    print(f"Total time for {num_tasks} tasks: {time.time() - start_time:.2f}s")
