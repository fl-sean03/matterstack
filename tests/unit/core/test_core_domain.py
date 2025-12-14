import json
import uuid
from pathlib import Path
from datetime import datetime

import pytest
from pydantic import ValidationError

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.operators import ExternalRunHandle, OperatorResult, ExternalRunStatus
from matterstack.core.evidence import EvidenceBundle
from matterstack.core.workflow import Task, Workflow

def test_run_handle_creation_and_paths():
    handle = RunHandle(
        workspace_slug="test_ws",
        run_id="run_123",
        root_path=Path("/tmp/runs/run_123")
    )
    
    assert handle.workspace_slug == "test_ws"
    assert handle.run_id == "run_123"
    assert handle.db_path == Path("/tmp/runs/run_123/state.sqlite")
    assert handle.config_path == Path("/tmp/runs/run_123/config.json")
    
    # Test JSON serialization
    json_str = handle.model_dump_json()
    loaded = RunHandle.model_validate_json(json_str)
    assert loaded == handle

def test_run_metadata_defaults():
    meta = RunMetadata()
    assert meta.created_at is not None
    assert meta.status == "active"
    assert meta.tags == {}
    
    meta_custom = RunMetadata(
        status="completed",
        tags={"experiment": "A"},
        description="Test run"
    )
    assert meta_custom.status == "completed"
    assert meta_custom.tags["experiment"] == "A"

def test_external_run_handle():
    handle = ExternalRunHandle(
        task_id="task_abc",
        operator_type="hpc"
    )
    assert handle.status == ExternalRunStatus.CREATED
    assert handle.external_id is None
    
    # Test update
    handle.external_id = "job_999"
    handle.status = ExternalRunStatus.RUNNING
    
    # Serialization
    json_str = handle.model_dump_json()
    loaded = ExternalRunHandle.model_validate_json(json_str)
    assert loaded.external_id == "job_999"
    assert loaded.status == ExternalRunStatus.RUNNING

def test_evidence_bundle():
    bundle = EvidenceBundle(
        run_id="run_123",
        workspace_slug="ws_1"
    )
    assert bundle.generated_at is not None
    assert bundle.data == {}
    
    bundle.data["f1_score"] = 0.95
    bundle.artifacts["plot"] = Path("plots/f1.png")
    
    # Serialization
    json_str = bundle.model_dump_json()
    loaded = EvidenceBundle.model_validate_json(json_str)
    assert loaded.data["f1_score"] == 0.95
    assert loaded.artifacts["plot"] == Path("plots/f1.png")

def test_workflow_task_models():
    # 1. Create Task
    task1 = Task(
        image="ubuntu:latest",
        command="echo hello"
    )
    assert task1.task_id is not None  # UUID generated
    assert task1.cores is None
    
    task2 = Task(
        image="python:3.9",
        command="python script.py",
        dependencies={task1.task_id},
        files={"script.py": "print('hello')"}
    )
    
    # 2. Create Workflow
    wf = Workflow()
    wf.add_task(task1)
    wf.add_task(task2)
    
    assert len(wf.tasks) == 2
    
    # 3. Test Sorting
    sorted_tasks = wf.get_topo_sorted_tasks()
    assert len(sorted_tasks) == 2
    assert sorted_tasks[0].task_id == task1.task_id
    assert sorted_tasks[1].task_id == task2.task_id
    
    # 4. Test Serialization
    json_str = wf.model_dump_json()
    loaded_wf = Workflow.model_validate_json(json_str)
    assert len(loaded_wf.tasks) == 2
    assert loaded_wf.tasks[task2.task_id].command == "python script.py"

def test_workflow_cycle_detection():
    wf = Workflow()
    t1 = Task(image="a", command="a", task_id="1")
    t2 = Task(image="b", command="b", task_id="2", dependencies={"1"})
    t1.dependencies.add("2") # Cycle
    
    wf.tasks["1"] = t1
    wf.tasks["2"] = t2
    
    with pytest.raises(ValueError, match="Graph has cycles"):
        wf.get_topo_sorted_tasks()