import json
import pytest
from pathlib import Path
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Workflow, Task
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle

@pytest.fixture
def temp_run_env(tmp_path):
    """Setup a temporary run environment with a DB."""
    run_root = tmp_path / "runs" / "test_run_123"
    run_root.mkdir(parents=True)
    db_path = run_root / "state.sqlite"
    
    store = SQLiteStateStore(db_path)
    
    handle = RunHandle(
        workspace_slug="test_ws",
        run_id="test_run_123",
        root_path=run_root
    )
    
    return handle, store

def test_build_evidence_bundle(temp_run_env):
    handle, store = temp_run_env
    
    # 1. Setup Data in Store
    store.create_run(handle)
    
    # Create tasks
    task1 = Task(task_id="t1", image="img", command="cmd", files={})
    task2 = Task(task_id="t2", image="img", command="cmd", files={})
    wf = Workflow(tasks={"t1": task1, "t2": task2})
    store.add_workflow(wf, handle.run_id)
    
    # Register External Runs (simulating completion)
    ext1 = ExternalRunHandle(
        task_id="t1",
        operator_type="hpc",
        external_id="job_1",
        status=ExternalRunStatus.COMPLETED,
        operator_data={"energy": -100.5}
    )
    store.register_external_run(ext1, handle.run_id)
    
    ext2 = ExternalRunHandle(
        task_id="t2",
        operator_type="manual",
        external_id="kit_1",
        status=ExternalRunStatus.FAILED,
        operator_data={"error": "User timeout"},
        relative_path=Path("operators/manual/kit_1")
    )
    store.register_external_run(ext2, handle.run_id)

    # Ensure artifact exists on disk
    artifact_path = handle.root_path / "operators/manual/kit_1"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.touch()
    
    # 2. Build Bundle
    bundle = build_evidence_bundle(handle, store)
    
    # 3. Verify
    assert bundle.run_id == "test_run_123"
    assert "t1" in bundle.data["tasks"]
    assert "t2" in bundle.data["tasks"]
    
    t1_data = bundle.data["tasks"]["t1"]
    assert t1_data["status"] == "COMPLETED"
    assert t1_data["results"]["energy"] == -100.5
    
    t2_data = bundle.data["tasks"]["t2"]
    assert t2_data["status"] == "FAILED"
    
    # Check artifact path resolution
    assert "t2" in bundle.artifacts
    assert bundle.artifacts["t2"] == handle.root_path / "operators/manual/kit_1"

def test_export_evidence_bundle(temp_run_env):
    handle, store = temp_run_env
    
    # Create a dummy bundle directly
    from matterstack.core.evidence import EvidenceBundle
    
    bundle = EvidenceBundle(
        run_id=handle.run_id,
        workspace_slug=handle.workspace_slug,
        data={
            "tasks": {
                "t1": {"status": "COMPLETED", "results": {"score": 99}},
                "t2": {"status": "PENDING"}
            }
        },
        artifacts={"t1": Path("/some/path")}
    )
    
    # Export
    export_evidence_bundle(bundle, handle.root_path)
    
    # Verify JSON
    json_path = handle.evidence_path / "bundle.json"
    assert json_path.exists()
    content = json.loads(json_path.read_text())
    assert content["run_id"] == handle.run_id
    assert content["data"]["tasks"]["t1"]["results"]["score"] == 99
    
    # Verify Markdown
    md_path = handle.evidence_path / "report.md"
    assert md_path.exists()
    md_content = md_path.read_text()
    
    assert "# Evidence Report: Run test_run_123" in md_content
    assert "| t1 | COMPLETED |" in md_content
    assert "score=99" in md_content
    assert "| t2 | PENDING |" in md_content