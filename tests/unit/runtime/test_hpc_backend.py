from unittest.mock import patch

import pytest

from matterstack.core.backend import JobState
from matterstack.core.workflow import Task
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.hpc.ssh import SSHConfig
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
async def test_submit_job_workdir_override_and_local_debug_dir(backend, mock_ssh, tmp_path):
    task = Task(
        task_id="job1",
        command="echo hello",
        image="",
        cores=1,
        memory_gb=1,
        time_limit_minutes=10,
        files={"input.txt": "data"},
    )

    attempt_workdir_1 = "/scratch/test/job1/attempt-001"
    local_debug_dir_1 = tmp_path / "attempt-001"

    job_id_1 = await backend.submit(
        task,
        workdir_override=attempt_workdir_1,
        local_debug_dir=local_debug_dir_1,
    )
    assert job_id_1 == "1000"

    # Remote paths should be rooted at the attempt-scoped workdir_override.
    assert f"{attempt_workdir_1}/submit.sh" in mock_ssh.files
    assert f"{attempt_workdir_1}/input.txt" in mock_ssh.files

    # sbatch should be run with cwd set to the attempt-scoped workspace.
    assert f"[cwd={attempt_workdir_1}] sbatch submit.sh" in mock_ssh.cmds_executed

    # submit.sh should be persisted locally into the attempt-scoped local_debug_dir.
    local_submit_1 = local_debug_dir_1 / "submit.sh"
    assert local_submit_1.exists()
    assert local_submit_1.read_text() == mock_ssh.files[f"{attempt_workdir_1}/submit.sh"].decode()

    # A second attempt must not overwrite the first attempt's local evidence (distinct dirs).
    attempt_workdir_2 = "/scratch/test/job1/attempt-002"
    local_debug_dir_2 = tmp_path / "attempt-002"

    job_id_2 = await backend.submit(
        task,
        workdir_override=attempt_workdir_2,
        local_debug_dir=local_debug_dir_2,
    )
    assert job_id_2 == "1001"

    local_submit_2 = local_debug_dir_2 / "submit.sh"
    assert local_submit_2.exists()
    assert local_submit_2.read_text() == mock_ssh.files[f"{attempt_workdir_2}/submit.sh"].decode()

    # Ensure attempt-001 file is still present and unchanged.
    assert local_submit_1.exists()
    assert local_submit_1.read_text() == mock_ssh.files[f"{attempt_workdir_1}/submit.sh"].decode()


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

    # Ensure backend.download accepts the interface keyword `job_id=`.
    await backend.download(job_id="job1", remote_path="output.dat", local_path=str(local_dest))

    assert local_dest.exists()
    assert local_dest.read_text() == "result data"


@pytest.mark.asyncio
async def test_download_workdir_override_and_filtering(backend, mock_ssh, tmp_path):
    attempt_workdir = "/scratch/test/job1/attempt-001"

    # Remote tree under attempt-scoped workdir.
    mock_ssh.files[f"{attempt_workdir}/results/a.json"] = b'{"a": 1}'
    mock_ssh.files[f"{attempt_workdir}/results/b.txt"] = b"exclude me"
    mock_ssh.files[f"{attempt_workdir}/logs/run.log"] = b"not included"

    local_dest = tmp_path / "dl"
    await backend.download(
        job_id="job1",
        remote_path=".",
        local_path=str(local_dest),
        include_patterns=["results/*"],
        exclude_patterns=["results/*.txt"],
        workdir_override=attempt_workdir,
    )

    assert (local_dest / "results" / "a.json").exists()
    assert (local_dest / "results" / "a.json").read_text() == '{"a": 1}'
    assert not (local_dest / "results" / "b.txt").exists()
    assert not (local_dest / "logs" / "run.log").exists()
