import pytest
import asyncio
from pathlib import Path
import json
import uuid
from typing import Dict, Any, Optional, List, Tuple

from matterstack.core.workflow import Task, Workflow
from matterstack.core.run import RunHandle
from matterstack.core.operators import ExternalRunStatus, OperatorResult
from matterstack.core.backend import ComputeBackend, JobStatus, JobState
from matterstack.runtime.operators.hpc import ComputeOperator as DirectHPCOperator
from matterstack.storage.state_store import SQLiteStateStore

# --- Mock Backend ---

class MockComputeBackend(ComputeBackend):
    def __init__(self):
        self.jobs: Dict[str, JobStatus] = {}
        self.files: Dict[str, Dict[str, str]] = {}  # job_id -> {filename: content}
        self.counter = 0

        # Mimic SlurmBackend: presence of workspace_root enables ComputeOperator to construct remote_workdir
        self.workspace_root = "/remote_root"

        # Track submissions so tests can assert attempt-scoped workdir overrides
        self.submissions: List[Tuple[str, Optional[str], Optional[Path]]] = []  # (job_id, workdir_override, local_debug_dir)

    async def submit(
        self,
        task: Task,
        workdir_override: Optional[str] = None,
        local_debug_dir: Optional[Path] = None,
    ) -> str:
        self.counter += 1
        job_id = str(self.counter)
        self.jobs[job_id] = JobStatus(job_id=job_id, state=JobState.QUEUED)

        self.submissions.append((job_id, workdir_override, local_debug_dir))

        # Simulate initial files (like stdout/stderr placeholders)
        self.files[job_id] = {
            "stdout.log": "",
            "stderr.log": "",
            "submit.sh": "# mock script",
        }

        # If a local debug dir is provided, write a submit.sh there to emulate SlurmBackend behavior
        if local_debug_dir is not None:
            local_debug_dir.mkdir(parents=True, exist_ok=True)
            (local_debug_dir / "submit.sh").write_text(f"# mock script for job {job_id}\n")

        return job_id

    async def poll(self, job_id: str) -> JobStatus:
        if job_id not in self.jobs:
            return JobStatus(job_id=job_id, state=JobState.UNKNOWN)
        return self.jobs[job_id]

    async def download(
        self,
        job_id: str,
        remote_path: str,
        local_path: str,
        include_patterns: Optional[Any] = None,
        exclude_patterns: Optional[Any] = None,
        workdir_override: Optional[str] = None,
    ) -> None:
        if job_id not in self.files:
            raise RuntimeError(f"Job {job_id} not found")

        local_dir = Path(local_path)
        local_dir.mkdir(parents=True, exist_ok=True)

        # Simulate downloading all files if remote_path is "."
        if remote_path == ".":
            for name, content in self.files[job_id].items():
                (local_dir / name).write_text(content)

    async def cancel(self, job_id: str) -> None:
        if job_id in self.jobs:
            self.jobs[job_id].state = JobState.CANCELLED

    async def get_logs(self, job_id: str) -> Dict[str, str]:
        return {"stdout": "", "stderr": ""}

    # Helper to simulate state change from test
    def set_status(self, job_id: str, state: JobState):
        if job_id in self.jobs:
            self.jobs[job_id].state = state

    def add_output_file(self, job_id: str, filename: str, content: str):
        if job_id in self.files:
            self.files[job_id][filename] = content


def _seed_store_with_run_and_task(run_handle: RunHandle, task: Task) -> SQLiteStateStore:
    """
    Create a minimal v2 schema store with:
    - run row
    - task row
    so create_attempt() can be used in attempt-aware tests.
    """
    store = SQLiteStateStore(run_handle.db_path)
    store.create_run(run_handle)
    wf = Workflow()
    wf.add_task(task)
    store.add_workflow(wf, run_handle.run_id)
    return store

# --- Tests ---

@pytest.fixture
def mock_backend():
    return MockComputeBackend()

@pytest.fixture
def run_handle(tmp_path):
    root = tmp_path / "run_root"
    root.mkdir()
    return RunHandle(
        workspace_slug="test_ws",
        run_id="test_run",
        root_path=root
    )

@pytest.fixture
def operator(mock_backend):
    return DirectHPCOperator(backend=mock_backend)

@pytest.fixture
def simple_task():
    return Task(
        task_id="task_1",
        image="ubuntu",
        command="echo hello"
    )

def test_prepare_run(operator, run_handle, simple_task):
    handle = operator.prepare_run(run_handle, simple_task)

    assert handle.task_id == simple_task.task_id
    assert handle.status == ExternalRunStatus.CREATED
    assert handle.operator_data["task_dump"] == simple_task.model_dump(mode="json")
    assert handle.relative_path is not None

    # Check directory creation
    full_path = run_handle.root_path / handle.relative_path
    assert full_path.exists()
    assert (full_path / "manifest.json").exists()

    # Check manifest content
    manifest = json.loads((full_path / "manifest.json").read_text())
    assert manifest["task_id"] == "task_1"

    # Check absolute_path was stored
    assert handle.operator_data.get("absolute_path") == str(full_path)

def test_submit(operator, run_handle, simple_task):
    handle = operator.prepare_run(run_handle, simple_task)

    handle = operator.submit(handle)

    assert handle.status == ExternalRunStatus.SUBMITTED
    assert handle.external_id == "1"

    # Verify backend received it
    assert "1" in operator.backend.jobs
    assert operator.backend.jobs["1"].state == JobState.QUEUED

def test_check_status(operator, run_handle, simple_task):
    handle = operator.prepare_run(run_handle, simple_task)
    handle = operator.submit(handle)

    # 1. Check initial status (QUEUED -> SUBMITTED)
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.SUBMITTED

    # 2. Simulate RUNNING
    operator.backend.set_status(handle.external_id, JobState.RUNNING)
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.RUNNING

    # 3. Simulate COMPLETED
    operator.backend.set_status(handle.external_id, JobState.COMPLETED_OK)
    handle = operator.check_status(handle)
    assert handle.status == ExternalRunStatus.COMPLETED

def test_collect_results(operator, run_handle, simple_task):
    handle = operator.prepare_run(run_handle, simple_task)
    handle = operator.submit(handle)

    # Add some output files to backend
    job_id = handle.external_id
    operator.backend.add_output_file(job_id, "results.csv", "a,b,c")

    # Run collection
    result = operator.collect_results(handle)

    assert result.task_id == simple_task.task_id
    assert "results.csv" in result.files

    # Verify file content on disk
    full_path = run_handle.root_path / handle.relative_path
    assert (full_path / "results.csv").read_text() == "a,b,c"

def test_idempotent_submit(operator, run_handle, simple_task):
    handle = operator.prepare_run(run_handle, simple_task)
    handle = operator.submit(handle)
    first_job_id = handle.external_id

    # Call submit again
    handle = operator.submit(handle)
    assert handle.external_id == first_job_id
    # Check counter didn't increase
    assert operator.backend.counter == 1

def test_attempt_scoped_evidence_dirs_and_workdirs_do_not_overwrite(operator, run_handle, simple_task):
    store = _seed_store_with_run_and_task(run_handle, simple_task)

    # Attempt 1
    attempt_id_1 = store.create_attempt(
        run_id=run_handle.run_id,
        task_id=simple_task.task_id,
        operator_type="HPC",
    )
    h1 = operator.prepare_run(run_handle, simple_task)
    p1 = run_handle.root_path / h1.relative_path
    assert str(p1).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_1}")
    assert (p1 / "manifest.json").exists()

    h1 = operator.submit(h1)
    assert (p1 / "submit.sh").exists()

    # Attempt 2 (same task_id, new attempt_id, must not overwrite attempt 1)
    attempt_id_2 = store.create_attempt(
        run_id=run_handle.run_id,
        task_id=simple_task.task_id,
        operator_type="HPC",
    )
    h2 = operator.prepare_run(run_handle, simple_task)
    p2 = run_handle.root_path / h2.relative_path
    assert str(p2).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_2}")
    assert p1 != p2
    assert (p2 / "manifest.json").exists()

    h2 = operator.submit(h2)
    assert (p2 / "submit.sh").exists()

    # Evidence for attempt 1 is still present (no overwrite)
    assert (p1 / "manifest.json").exists()
    assert (p1 / "submit.sh").exists()

    # Backend got distinct workdir overrides per attempt
    # submissions: (job_id, workdir_override, local_debug_dir)
    (_, wd1, ld1), (_, wd2, ld2) = operator.backend.submissions[:2]
    assert wd1 is not None and wd2 is not None
    assert wd1.endswith(f"/{simple_task.task_id}/{attempt_id_1}")
    assert wd2.endswith(f"/{simple_task.task_id}/{attempt_id_2}")
    assert wd1 != wd2
    assert ld1 is not None and ld2 is not None
    assert str(ld1).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_1}")
    assert str(ld2).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_2}")


def test_attempt_config_snapshot_hash_is_populated_and_stable(operator, run_handle, simple_task):
    store = _seed_store_with_run_and_task(run_handle, simple_task)

    # Provide deterministic run-root inputs for snapshot.
    (run_handle.root_path / "config.json").write_text('{"max_hpc_jobs_per_run": 1}\n')
    (run_handle.root_path / "campaign_state.json").write_text('{"state": "x"}\n')

    # Attempt 1
    attempt_id_1 = store.create_attempt(
        run_id=run_handle.run_id,
        task_id=simple_task.task_id,
        operator_type="HPC",
    )
    h1 = operator.prepare_run(run_handle, simple_task)
    p1 = run_handle.root_path / h1.relative_path

    assert str(p1).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_1}")
    assert h1.operator_data.get("config_hash"), "config_hash should be present in operator_data"
    assert isinstance(h1.operator_data["config_hash"], str)
    assert len(h1.operator_data["config_hash"]) >= 64

    snap1 = p1 / "config_snapshot"
    assert snap1.exists()
    assert (snap1 / "config.json").exists()
    assert (snap1 / "campaign_state.json").exists()
    assert (snap1 / "task_manifest.json").exists()
    assert (snap1 / "manifest.json").exists()

    m1 = json.loads((snap1 / "manifest.json").read_text())
    assert m1["combined_hash"] == h1.operator_data["config_hash"]

    # Attempt 2 (identical snapshot inputs -> stable hash)
    attempt_id_2 = store.create_attempt(
        run_id=run_handle.run_id,
        task_id=simple_task.task_id,
        operator_type="HPC",
    )
    h2 = operator.prepare_run(run_handle, simple_task)
    p2 = run_handle.root_path / h2.relative_path

    assert str(p2).endswith(f"tasks/{simple_task.task_id}/attempts/{attempt_id_2}")
    assert h2.operator_data.get("config_hash")
    assert h2.operator_data["config_hash"] == h1.operator_data["config_hash"]

    snap2 = p2 / "config_snapshot"
    m2 = json.loads((snap2 / "manifest.json").read_text())
    assert m2["combined_hash"] == h2.operator_data["config_hash"]