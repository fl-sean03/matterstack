import pytest
from pathlib import Path
from matterstack.core.run import RunHandle
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.core.workflow import Task
from matterstack.orchestration.diagnostics import get_run_frontier
from matterstack.runtime.operators.manual_hpc import ManualHPCOperator

def test_explain_waiting_external(tmp_path):
    """
    Test that explain identifies a task waiting for external operator.
    """
    # Setup
    run_id = "test_run_explain"
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    # Create Run
    handle = RunHandle(workspace_slug="ws", run_id=run_id, root_path=tmp_path)
    store.create_run(handle)
    
    # Create Task
    task = Task(task_id="task_1", image="img", command="cmd")
    
    # Manually insert task into store
    # We can't use store.add_workflow directly because it takes a Workflow object
    # but that's fine.
    from matterstack.core.workflow import Workflow
    wf = Workflow(tasks={"task_1": task})
    store.add_workflow(wf, run_id)
    
    # Register External Run (ManualHPC)
    ext_handle = ExternalRunHandle(
        task_id="task_1",
        operator_type="ManualHPC",
        status=ExternalRunStatus.WAITING_EXTERNAL,
        operator_data={"absolute_path": str(tmp_path / "operators" / "manual" / "uuid")}
    )
    store.register_external_run(ext_handle, run_id)
    store.update_task_status("task_1", "WAITING_EXTERNAL")
    
    # Call explain
    frontier = get_run_frontier(store, run_id, tmp_path)
    
    assert len(frontier) == 1
    item = frontier[0]
    assert item.task_id == "task_1"
    assert item.status == "WAITING_EXTERNAL"
    assert "ManualHPC" in item.operator_type
    assert "Waiting for status.json" in item.hint
    assert "operators/manual/uuid" in item.hint # check relative path in hint

def test_explain_pending_ready(tmp_path):
    """
    Test that explain identifies a pending task that is ready to run.
    """
    run_id = "test_run_pending"
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    handle = RunHandle(workspace_slug="ws", run_id=run_id, root_path=tmp_path)
    store.create_run(handle)
    
    from matterstack.core.workflow import Workflow
    task = Task(task_id="task_ready", image="img", command="cmd")
    wf = Workflow(tasks={"task_ready": task})
    store.add_workflow(wf, run_id)
    
    # Status is None/PENDING by default
    
    frontier = get_run_frontier(store, run_id, tmp_path)
    
    assert len(frontier) == 1
    assert frontier[0].task_id == "task_ready"
    assert frontier[0].status == "READY"

def test_explain_pending_blocked(tmp_path):
    """
    Test that explain does NOT show a task blocked by unmet dependencies as the primary blocker.
    """
    run_id = "test_run_blocked"
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    handle = RunHandle(workspace_slug="ws", run_id=run_id, root_path=tmp_path)
    store.create_run(handle)
    
    from matterstack.core.workflow import Workflow
    
    # Task 1: Running (Frontier)
    t1 = Task(task_id="t1", image="img", command="cmd")
    
    # Task 2: Depends on T1 (Not Frontier yet)
    t2 = Task(task_id="t2", image="img", command="cmd", dependencies={"t1"})
    
    wf = Workflow(tasks={"t1": t1, "t2": t2})
    store.add_workflow(wf, run_id)
    
    store.update_task_status("t1", "RUNNING")
    
    frontier = get_run_frontier(store, run_id, tmp_path)
    
    # Frontier should contain t1 (RUNNING) but not t2 (Blocked)
    assert len(frontier) == 1
    assert frontier[0].task_id == "t1"
    assert frontier[0].status == "RUNNING"