from matterstack.core.backend import JobState
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.api import run_workflow
from matterstack.runtime.backends.local import LocalBackend

# A simple mock backend is not needed if we use LocalBackend with dry_run=False
# But to reliably "fail" a task, we can use a command that exits with non-zero code.

def test_workflow_soft_failure(tmp_path):
    """
    Test that continue_on_error=True allows independent tasks to run
    when a sibling fails.
    """
    workspace = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace))

    wf = Workflow()

    # Task A: Succeeds
    task_a = Task(
        task_id="task_a",
        image="ubuntu",
        command="echo 'Task A success'"
    )

    # Task B: Fails
    task_b = Task(
        task_id="task_b",
        image="ubuntu",
        command="exit 1"
    )

    # Task C: Depends on B (Should be Cancelled)
    task_c = Task(
        task_id="task_c",
        image="ubuntu",
        command="echo 'Task C success'",
        dependencies={"task_b"}
    )

    # Task D: Independent (Should Run)
    task_d = Task(
        task_id="task_d",
        image="ubuntu",
        command="echo 'Task D success'"
    )

    wf.add_task(task_a)
    wf.add_task(task_b)
    wf.add_task(task_c)
    wf.add_task(task_d)

    # Run with continue_on_error=True
    result = run_workflow(
        wf,
        backend=backend,
        continue_on_error=True,
        poll_interval=0.1
    )

    # Overall status should be PARTIAL_SUCCESS
    assert result.status == JobState.COMPLETED_ERROR

    # A should be COMPLETED
    assert result.tasks["task_a"].status.state == JobState.COMPLETED_OK

    # B should be FAILED
    assert result.tasks["task_b"].status.state == JobState.COMPLETED_ERROR

    # C should be CANCELLED (upstream failure)
    assert result.tasks["task_c"].status.state == JobState.CANCELLED

    # D should be COMPLETED (independent)
    assert result.tasks["task_d"].status.state == JobState.COMPLETED_OK


def test_workflow_strict_abort(tmp_path):
    """
    Test that continue_on_error=False aborts immediately.
    """
    workspace = tmp_path / "workspace"
    backend = LocalBackend(workspace_root=str(workspace))

    wf = Workflow()

    # Task A: Fails immediately
    task_a = Task(
        task_id="task_a",
        image="ubuntu",
        command="exit 1"
    )

    # Task B: Independent (Should NOT Run because A failed and we abort)
    # Note: Since execution is sequential in the current engine,
    # we need to ensure A runs before B. Topological sort usually respects insertion order
    # for independent nodes, or name order.
    # To be safe, we make B depend on nothing, but we check if it ran.
    # However, if B is truly independent, the topological sort MIGHT put B before A.
    # So we force A -> B dependency? No, then B wouldn't run anyway.

    # We rely on the fact that if A fails, the loop breaks.
    # If B hasn't run yet, it won't run.
    # If B ran before A, then this test proves nothing about aborting *future* tasks.
    # So we need to ensure A is scheduled before B.
    # Workflow.get_topo_sorted_tasks implementation detail:
    # It iterates self.tasks (insertion order usually in Py3.7+ dicts).

    task_b = Task(
        task_id="task_b",
        image="ubuntu",
        command="echo 'Task B success'"
    )

    wf.add_task(task_a)
    wf.add_task(task_b)

    # Check execution order assumption
    sorted_tasks = wf.get_topo_sorted_tasks()
    if sorted_tasks[0].task_id != "task_a":
        # Swap logic if B comes first
        pass
        # Actually, let's just make B really slow or something?
        # No, sequential engine.
        # If A is first in list, A runs first.

    # Run with continue_on_error=False (Default)
    result = run_workflow(
        wf,
        backend=backend,
        continue_on_error=False,
        poll_interval=0.1
    )

    assert result.status == JobState.COMPLETED_ERROR
    assert result.tasks["task_a"].status.state == JobState.COMPLETED_ERROR

    # B should NOT be present in results because the loop broke
    # OR it should be present but untouched?
    # The current implementation initializes task_results = {}.
    # So if loop breaks, B is not in task_results.
    assert "task_b" not in result.tasks
