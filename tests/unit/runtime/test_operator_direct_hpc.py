import pytest
import asyncio
from pathlib import Path
import json
import uuid
from typing import Dict, Any, Optional

from matterstack.core.workflow import Task
from matterstack.core.run import RunHandle
from matterstack.core.operators import ExternalRunStatus, OperatorResult
from matterstack.core.backend import ComputeBackend, JobStatus, JobState
from matterstack.runtime.operators.hpc import ComputeOperator as DirectHPCOperator

# --- Mock Backend ---

class MockComputeBackend(ComputeBackend):
    def __init__(self):
        self.jobs: Dict[str, JobStatus] = {}
        self.files: Dict[str, Dict[str, str]] = {} # job_id -> {filename: content}
        self.counter = 0

    async def submit(self, task: Task) -> str:
        self.counter += 1
        job_id = str(self.counter)
        self.jobs[job_id] = JobStatus(job_id=job_id, state=JobState.QUEUED)
        
        # Simulate initial files (like stdout/stderr placeholders)
        self.files[job_id] = {
            "stdout.log": "",
            "stderr.log": "",
            "submit.sh": "# mock script"
        }
        return job_id

    async def poll(self, job_id: str) -> JobStatus:
        if job_id not in self.jobs:
            return JobStatus(job_id=job_id, state=JobState.UNKNOWN)
        return self.jobs[job_id]

    async def download(self, job_id: str, remote_path: str, local_path: str) -> None:
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
    assert handle.operator_data["task_dump"] == simple_task.model_dump(mode='json')
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
    
    # 1. Check initial status (PENDING -> SUBMITTED)
    # The map_status maps PENDING -> SUBMITTED
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