from pathlib import Path

import pytest

from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.state_store import SQLiteStateStore


@pytest.fixture
def temp_run_dir(tmp_path):
    run_dir = tmp_path / "test_run"
    run_dir.mkdir()
    return run_dir

@pytest.fixture
def run_handle(temp_run_dir):
    return RunHandle(
        workspace_slug="test_workspace",
        run_id="run_123",
        root_path=temp_run_dir
    )

@pytest.fixture
def store(run_handle):
    return SQLiteStateStore(run_handle.db_path)

def test_initialize_store(store, run_handle):
    """Test that the database file is created and tables are initialized."""
    assert run_handle.db_path.exists()

    # Re-opening should work
    new_store = SQLiteStateStore(run_handle.db_path)
    assert new_store

def test_create_and_get_run(store, run_handle):
    """Test creating and retrieving a run."""
    metadata = RunMetadata(
        description="Test Run",
        tags={"project": "matterstack"}
    )
    store.create_run(run_handle, metadata)

    # Verify retrieval
    retrieved_handle = store.get_run(run_handle.run_id)
    assert retrieved_handle is not None
    assert retrieved_handle.run_id == run_handle.run_id
    assert retrieved_handle.workspace_slug == run_handle.workspace_slug
    assert retrieved_handle.root_path == run_handle.root_path

def test_workflow_persistence(store, run_handle):
    """Test persisting and retrieving a workflow with tasks."""
    # Create run first (FK constraint)
    store.create_run(run_handle)

    wf = Workflow()
    t1 = Task(image="ubuntu", command="echo 1", task_id="t1")
    t2 = Task(image="ubuntu", command="echo 2", task_id="t2", dependencies={"t1"})

    wf.add_task(t1)
    wf.add_task(t2)

    store.add_workflow(wf, run_handle.run_id)

    # Verify tasks
    tasks = store.get_tasks(run_handle.run_id)
    assert len(tasks) == 2

    t1_retrieved = next(t for t in tasks if t.task_id == "t1")
    t2_retrieved = next(t for t in tasks if t.task_id == "t2")

    assert t1_retrieved.command == "echo 1"
    assert t2_retrieved.dependencies == {"t1"}

def test_task_status_update(store, run_handle):
    """Test updating task internal status."""
    store.create_run(run_handle)
    wf = Workflow()
    t1 = Task(image="u", command="c", task_id="t1")
    wf.add_task(t1)
    store.add_workflow(wf, run_handle.run_id)

    assert store.get_task_status("t1") == "PENDING"

    store.update_task_status("t1", "COMPLETED")
    assert store.get_task_status("t1") == "COMPLETED"

def test_external_run_lifecycle(store, run_handle):
    """Test creating, updating, and querying external runs."""
    store.create_run(run_handle)
    wf = Workflow()
    t1 = Task(image="u", command="c", task_id="t1")
    wf.add_task(t1)
    store.add_workflow(wf, run_handle.run_id)

    # Register External Run
    ext_handle = ExternalRunHandle(
        task_id="t1",
        operator_type="slurm",
        status=ExternalRunStatus.CREATED,
        relative_path=Path("operators/slurm/123")
    )
    store.register_external_run(ext_handle, run_handle.run_id)

    # Verify creation
    retrieved = store.get_external_run("t1")
    assert retrieved.operator_type == "slurm"
    assert retrieved.status == ExternalRunStatus.CREATED
    assert retrieved.relative_path == Path("operators/slurm/123")

    # Update Status
    ext_handle.status = ExternalRunStatus.RUNNING
    ext_handle.external_id = "job_999"
    store.update_external_run(ext_handle)

    retrieved_updated = store.get_external_run("t1")
    assert retrieved_updated.status == ExternalRunStatus.RUNNING
    assert retrieved_updated.external_id == "job_999"

    # Test Active Runs Query
    active = store.get_active_external_runs(run_handle.run_id)
    assert len(active) == 1
    assert active[0].task_id == "t1"

    # Complete it
    ext_handle.status = ExternalRunStatus.COMPLETED
    store.update_external_run(ext_handle)

    active_now = store.get_active_external_runs(run_handle.run_id)
    assert len(active_now) == 0

def test_persistence_across_instances(temp_run_dir, run_handle):
    """Test that data persists when closing and reopening the store."""
    store1 = SQLiteStateStore(run_handle.db_path)
    store1.create_run(run_handle)

    wf = Workflow()
    t1 = Task(image="u", command="c", task_id="persistent_task")
    wf.add_task(t1)
    store1.add_workflow(wf, run_handle.run_id)

    # Simulate process exit/restart by creating new store instance
    store2 = SQLiteStateStore(run_handle.db_path)
    tasks = store2.get_tasks(run_handle.run_id)

    assert len(tasks) == 1
    assert tasks[0].task_id == "persistent_task"
