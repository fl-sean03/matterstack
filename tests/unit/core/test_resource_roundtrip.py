from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.state_store import SQLiteStateStore


def test_task_resource_defaults_none():
    """Verify that a Task initialized without resources has None values."""
    task = Task(image="ubuntu", command="ls")
    assert task.cores is None
    assert task.memory_gb is None
    assert task.gpus is None
    assert task.time_limit_minutes is None

def test_task_resource_roundtrip_persistence(tmp_path):
    """
    Verify that a Task with None resources is serialized/deserialized correctly
    without gaining default values when passed through SQLiteStateStore.
    """
    # 1. Setup StateStore
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)

    # 2. Create Run
    run_id = "test_run_123"
    handle = RunHandle(
        run_id=run_id,
        workspace_slug="test_ws",
        root_path=tmp_path
    )
    store.create_run(handle)

    # 3. Create Task with minimal args (resources should be None)
    task_none = Task(
        task_id="task_none",
        image="ubuntu",
        command="echo hello"
    )
    # Ensure they start as None
    assert task_none.cores is None
    assert task_none.memory_gb is None
    assert task_none.gpus is None
    assert task_none.time_limit_minutes is None

    # 4. Create Task with specified resources
    task_specified = Task(
        task_id="task_specified",
        image="ubuntu",
        command="echo hello",
        cores=4,
        memory_gb=16,
        gpus=1,
        time_limit_minutes=120
    )

    # 5. Add to Workflow and persist
    workflow = Workflow()
    workflow.add_task(task_none)
    workflow.add_task(task_specified)

    store.add_workflow(workflow, run_id)

    # 6. Retrieve and Verify
    retrieved_tasks = store.get_tasks(run_id)
    task_map = {t.task_id: t for t in retrieved_tasks}

    # Verify task_none
    r_task_none = task_map["task_none"]
    assert r_task_none.cores is None
    assert r_task_none.memory_gb is None
    assert r_task_none.gpus is None
    assert r_task_none.time_limit_minutes is None

    # Verify task_specified
    r_task_specified = task_map["task_specified"]
    assert r_task_specified.cores == 4
    assert r_task_specified.memory_gb == 16
    assert r_task_specified.gpus == 1
    assert r_task_specified.time_limit_minutes == 120
