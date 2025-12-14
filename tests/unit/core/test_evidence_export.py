import json
import pytest
from pathlib import Path
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Workflow, Task
from matterstack.core.operators import ExternalRunStatus
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

    # Register Attempts (schema v2): attempt-first behavior
    a1 = store.create_attempt(
        run_id=handle.run_id,
        task_id="t1",
        operator_type="hpc",
        relative_path=None,
    )
    store.update_attempt(
        a1,
        status=ExternalRunStatus.COMPLETED.value,
        operator_type="hpc",
        external_id="job_1",
        operator_data={"energy": -100.5},
    )

    # Task t2: two attempts with separate evidence directories
    t2_attempt1_rel = Path("operators/manual/kit_1/attempt_1")
    a2_1 = store.create_attempt(
        run_id=handle.run_id,
        task_id="t2",
        operator_type="manual",
        relative_path=t2_attempt1_rel,
    )
    store.update_attempt(
        a2_1,
        status=ExternalRunStatus.FAILED.value,
        operator_type="manual",
        external_id="kit_1a",
        operator_data={"error": "User timeout"},
        relative_path=t2_attempt1_rel,
    )

    t2_attempt2_rel = Path("operators/manual/kit_1/attempt_2")
    a2_2 = store.create_attempt(
        run_id=handle.run_id,
        task_id="t2",
        operator_type="manual",
        relative_path=t2_attempt2_rel,
    )
    store.update_attempt(
        a2_2,
        status=ExternalRunStatus.FAILED.value,
        operator_type="manual",
        external_id="kit_1b",
        operator_data={"error": "Second attempt failed"},
        relative_path=t2_attempt2_rel,
    )

    # Ensure artifact dirs exist on disk (attempt evidence directories)
    (handle.root_path / t2_attempt1_rel).mkdir(parents=True, exist_ok=True)
    (handle.root_path / t2_attempt2_rel).mkdir(parents=True, exist_ok=True)

    # 2. Build Bundle
    bundle = build_evidence_bundle(handle, store)

    # 3. Verify
    assert bundle.run_id == "test_run_123"
    assert "t1" in bundle.data["tasks"]
    assert "t2" in bundle.data["tasks"]

    t1_data = bundle.data["tasks"]["t1"]
    assert t1_data["status"] == "COMPLETED"
    assert t1_data["results"]["energy"] == -100.5
    assert t1_data["legacy_external_run"] is None
    assert len(t1_data["attempts"]) == 1
    assert t1_data["current_attempt"]["attempt_index"] == 1

    t2_data = bundle.data["tasks"]["t2"]
    # Summary status is derived from current attempt (attempt_2)
    assert t2_data["status"] == "FAILED"
    assert t2_data["legacy_external_run"] is None
    assert [a["attempt_index"] for a in t2_data["attempts"]] == [1, 2]
    assert t2_data["current_attempt"]["attempt_index"] == 2

    # Check attempt-specific artifact path export
    assert t2_data["attempts"][0]["artifact_path"] == str(handle.root_path / t2_attempt1_rel)
    assert t2_data["attempts"][1]["artifact_path"] == str(handle.root_path / t2_attempt2_rel)

    # Compatibility shim: bundle.artifacts points at CURRENT attempt evidence dir
    assert "t2" in bundle.artifacts
    assert bundle.artifacts["t2"] == handle.root_path / t2_attempt2_rel

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


def test_evidence_export_includes_multiple_attempts_for_single_task(temp_run_env):
    handle, store = temp_run_env

    store.create_run(handle)

    task = Task(task_id="t1", image="img", command="cmd", files={})
    wf = Workflow(tasks={"t1": task})
    store.add_workflow(wf, handle.run_id)

    rel1 = Path("operators/hpc/job_1/attempt_1")
    attempt1 = store.create_attempt(
        run_id=handle.run_id,
        task_id="t1",
        operator_type="hpc",
        relative_path=rel1,
    )
    store.update_attempt(
        attempt1,
        status=ExternalRunStatus.FAILED.value,
        operator_type="hpc",
        external_id="job_1a",
        operator_data={"error": "first fail"},
        relative_path=rel1,
    )
    (handle.root_path / rel1).mkdir(parents=True, exist_ok=True)

    rel2 = Path("operators/hpc/job_1/attempt_2")
    attempt2 = store.create_attempt(
        run_id=handle.run_id,
        task_id="t1",
        operator_type="hpc",
        relative_path=rel2,
    )
    store.update_attempt(
        attempt2,
        status=ExternalRunStatus.COMPLETED.value,
        operator_type="hpc",
        external_id="job_1b",
        operator_data={"ok": True},
        relative_path=rel2,
    )
    (handle.root_path / rel2).mkdir(parents=True, exist_ok=True)

    bundle = build_evidence_bundle(handle, store)
    t1 = bundle.data["tasks"]["t1"]

    assert [a["attempt_index"] for a in t1["attempts"]] == [1, 2]
    assert t1["current_attempt"]["attempt_index"] == 2
    assert t1["status"] == "COMPLETED"
    assert t1["attempts"][0]["artifact_path"] == str(handle.root_path / rel1)
    assert t1["attempts"][1]["artifact_path"] == str(handle.root_path / rel2)
    assert bundle.artifacts["t1"] == handle.root_path / rel2