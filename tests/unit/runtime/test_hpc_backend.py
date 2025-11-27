import pytest
import asyncio
from unittest.mock import patch
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.hpc.ssh import SSHConfig
from matterstack.core.workflow import Task
from matterstack.core.backend import JobState
from tests.unit.runtime.hpc_mocks import MockSSHClient

@pytest.fixture
def mock_ssh():
    client = MockSSHClient()
    # Patch the connect method to return our pre-configured mock instance
    with patch("matterstack.runtime.backends.hpc.ssh.SSHClient.connect", return_value=client) as p:
        yield client

@pytest.fixture
def backend(mock_ssh):
    config = SSHConfig(host="test", user="user")
    b = SlurmBackend(ssh_config=config, workspace_root="/scratch/test")
    return b

@pytest.mark.asyncio
async def test_submit_job(backend, mock_ssh):
    task = Task(
        task_id="job1",
        command="echo hello",
        image="",
        cores=1,
        memory_gb=1,
        time_limit_minutes=10,
        files={"input.txt": "data"}
    )

    job_id = await backend.submit(task)
    
    assert job_id == "1000"
    assert "/scratch/test/job1/submit.sh" in mock_ssh.files
    assert "/scratch/test/job1/input.txt" in mock_ssh.files
    assert mock_ssh.files["/scratch/test/job1/input.txt"] == b"data"
    
    # Verify script content (basic check)
    script = mock_ssh.files["/scratch/test/job1/submit.sh"].decode()
    assert "#SBATCH --job-name=job1" in script
    assert "echo hello" in script

@pytest.mark.asyncio
async def test_poll_job(backend, mock_ssh):
    # Setup job in mock
    mock_ssh.jobs["123"] = mock_ssh.jobs.get("123") or type("Info", (), {"job_id": "123", "state": "RUNNING", "exit_code": "0:0", "reason": "None"})()
    
    status = await backend.poll("123")
    assert status.state == JobState.RUNNING

    # Update state
    mock_ssh.jobs["123"].state = "COMPLETED"
    status = await backend.poll("123")
    assert status.state == JobState.COMPLETED_OK

@pytest.mark.asyncio
async def test_get_logs(backend, mock_ssh):
    # Setup log files
    mock_ssh.files["/scratch/test/job1/stdout.txt"] = b"Hello World\n"
    mock_ssh.files["/scratch/test/job1/stderr.txt"] = b"No errors\n"
    
    # We need to ensure scontrol/sacct points to these. 
    # The current MockSSHClient.run implementation for scontrol returns fixed paths:
    # StdOut=stdout.txt StdErr=stderr.txt WorkDir=/tmp/work
    # So we need to match those paths in self.files or update the mock logic.
    # Let's override the file paths in mock_ssh to match what the mock returns for scontrol
    
    mock_ssh.files["/tmp/work/stdout.txt"] = b"Hello World\n"
    mock_ssh.files["/tmp/work/stderr.txt"] = b"No errors\n"

    logs = await backend.get_logs("1000")
    
    assert logs["stdout"] == "Hello World\n"
    assert logs["stderr"] == "No errors\n"

@pytest.mark.asyncio
async def test_download(backend, mock_ssh, tmp_path):
    # Setup remote file
    mock_ssh.files["/scratch/test/job1/output.dat"] = b"result data"
    
    local_dest = tmp_path / "output.dat"
    
    # backend.download uses full remote path relative to workspace/task_id
    await backend.download("job1", "output.dat", str(local_dest))
    
    assert local_dest.exists()
    assert local_dest.read_text() == "result data"