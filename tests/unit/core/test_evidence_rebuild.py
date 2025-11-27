import shutil
import tempfile
import json
from pathlib import Path
from unittest.mock import MagicMock

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle
from matterstack.core.workflow import Task

def test_evidence_rebuild_idempotency():
    """
    Verify that evidence export is idempotent and rebuilds from store.
    """
    # Setup temp workspace
    temp_dir = Path(tempfile.mkdtemp())
    try:
        run_root = temp_dir / "runs" / "test_run_1"
        run_root.mkdir(parents=True)
        db_path = run_root / "state.sqlite"
        
        handle = RunHandle(workspace_slug="test_ws", run_id="test_run_1", root_path=run_root)
        store = SQLiteStateStore(db_path)
        
        # 1. Create a Run in "COMPLETED" state
        meta = RunMetadata(status="COMPLETED")
        store.create_run(handle, meta)
        
        # 2. Add some tasks
        task1 = Task(task_id="t1", image="img", command="cmd")
        store.add_workflow(MagicMock(tasks={"t1": task1}), handle.run_id)
        
        # Mock external run execution
        from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
        store.register_external_run(ExternalRunHandle(
            task_id="t1",
            operator_type="test_op",
            status=ExternalRunStatus.COMPLETED,
            operator_data={"result": 42},
            relative_path=Path("artifacts/t1.dat")
        ), handle.run_id)
        
        # Create dummy artifact
        (run_root / "artifacts").mkdir()
        (run_root / "artifacts" / "t1.dat").write_text("DATA")
        
        # 3. First Export
        bundle1 = build_evidence_bundle(handle, store)
        export_evidence_bundle(bundle1, handle.root_path)
        
        assert bundle1.is_complete is True
        assert bundle1.run_status == "COMPLETED"
        assert bundle1.task_counts["completed"] == 1
        assert "t1" in bundle1.artifacts
        
        json_path = run_root / "evidence" / "bundle.json"
        assert json_path.exists()
        content1 = json_path.read_text()
        
        # 4. Delete evidence directory
        shutil.rmtree(run_root / "evidence")
        
        # 5. Second Export (Rebuild)
        bundle2 = build_evidence_bundle(handle, store)
        export_evidence_bundle(bundle2, handle.root_path)
        
        content2 = json_path.read_text()
        
        # Timestamps will differ, but core data should match
        data1 = json.loads(content1)
        data2 = json.loads(content2)
        
        assert data1["run_id"] == data2["run_id"]
        assert data1["task_counts"] == data2["task_counts"]
        assert data1["artifacts"] == data2["artifacts"]
        
    finally:
        shutil.rmtree(temp_dir)

def test_evidence_partial_run():
    """
    Verify that a FAILED run produces valid partial evidence.
    """
    temp_dir = Path(tempfile.mkdtemp())
    try:
        run_root = temp_dir / "runs" / "test_run_fail"
        run_root.mkdir(parents=True)
        db_path = run_root / "state.sqlite"
        
        handle = RunHandle(workspace_slug="test_ws", run_id="test_run_fail", root_path=run_root)
        store = SQLiteStateStore(db_path)
        
        # 1. Create a Run in "FAILED" state
        meta = RunMetadata(status="FAILED")
        store.create_run(handle, meta)
        store.set_run_status(handle.run_id, "FAILED", reason="Critical failure")
        
        # 2. Add tasks: 1 completed, 1 failed
        task1 = Task(task_id="t1", image="img", command="cmd")
        task2 = Task(task_id="t2", image="img", command="cmd")
        store.add_workflow(MagicMock(tasks={"t1": task1, "t2": task2}), handle.run_id)
        
        from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
        
        # Task 1 Completed
        store.register_external_run(ExternalRunHandle(
            task_id="t1",
            operator_type="test_op",
            status=ExternalRunStatus.COMPLETED,
            operator_data={"res": 1},
            relative_path=None
        ), handle.run_id)
        
        # Task 2 Failed
        store.register_external_run(ExternalRunHandle(
            task_id="t2",
            operator_type="test_op",
            status=ExternalRunStatus.FAILED,
            operator_data={"error": "bad inputs"},
            relative_path=None
        ), handle.run_id)
        
        # 3. Export
        bundle = build_evidence_bundle(handle, store)
        export_evidence_bundle(bundle, handle.root_path)
        
        assert bundle.is_complete is False
        assert bundle.run_status == "FAILED"
        assert bundle.status_reason == "Critical failure"
        
        counts = bundle.task_counts
        assert counts["total"] == 2
        assert counts["completed"] == 1
        assert counts["failed"] == 1
        
        # Check Report
        report_path = run_root / "evidence" / "report.md"
        report_content = report_path.read_text()
        
        assert "❌ FAILED" in report_content
        assert "**Reason:** Critical failure" in report_content
        assert "1/2 Tasks Completed" in report_content
        
    finally:
        shutil.rmtree(temp_dir)