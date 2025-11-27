import pytest
import asyncio
from unittest.mock import patch
from matterstack.config.profiles import load_profiles, ExecutionProfile
from matterstack.orchestration.api import run_task_async
from matterstack.core.workflow import Task
from matterstack.core.backend import JobState
from tests.unit.runtime.hpc_mocks import MockSSHClient

@pytest.fixture
def mock_ssh():
    client = MockSSHClient()
    with patch("matterstack.runtime.backends.hpc.ssh.SSHClient.connect", return_value=client):
        yield client

@pytest.mark.asyncio
async def test_slurm_profile_execution(mock_ssh, tmp_path):
    # 1. Create a config file defining a slurm profile
    config_yaml = """
    profiles:
      test_slurm:
        type: slurm
        workspace_root: /scratch/users/test
        ssh:
          host: login.hpc.edu
          user: testuser
          key_path: ~/.ssh/id_rsa
        slurm:
          account: myacc
          partition: debug
    """
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_yaml)

    # 2. Load profile
    profiles = load_profiles(str(config_file))
    assert "test_slurm" in profiles
    profile = profiles["test_slurm"]
    assert profile.type == "slurm"

    # 3. Create backend explicitly to verify config transfer
    backend = profile.create_backend()
    assert backend.workspace_root == "/scratch/users/test"
    assert backend.ssh_config.host == "login.hpc.edu"

    # 4. Run a task via orchestration (this uses profile.create_backend internally if we passed the profile name,
    # but run_task_async takes a backend instance or constructs one.
    # Let's verify run_task_async with an explicit backend first, or check if we can pass a profile.
    # Looking at api.py, run_task_async takes (task, profile_name or backend).
    
    task = Task(
        task_id="integration_job",
        command="echo 'running integration'",
        image="",
        files={"config.ini": "mode=test"}
    )
    
    # We need to ensure the orchestration layer picks up our mocked backend.
    # Since run_task_async(..., profile_name="test_slurm") will call load_profile -> create_backend -> SSHClient.connect,
    # and we have patched SSHClient.connect, this should work seamlessly.
    
    # However, run_task_async might need a context or similar. 
    # Let's import it and check signature briefly (I recall reading it).
    # It returns a result object.
    
    # Note: run_task_async is a high level helper. 
    # If it polls, we need the job to complete. 
    # Our MockSSHClient submits jobs as PENDING.
    # We need a way to advance the job state during the orchestration run?
    # Or we just test submission if run_task_async returns early?
    # Usually run_task_async waits for completion.
    
    # To handle the waiting loop, we can start a background task that updates the job state
    # after a short delay.
    
    async def simulate_cluster():
        await asyncio.sleep(0.5)
        # Find the job and mark completed
        # We don't know the ID easily here unless we peek at mock_ssh
        for jid in mock_ssh.jobs:
            if mock_ssh.jobs[jid].state == "PENDING":
                mock_ssh.jobs[jid].state = "RUNNING"
        await asyncio.sleep(0.5)
        for jid in mock_ssh.jobs:
             if mock_ssh.jobs[jid].state == "RUNNING":
                mock_ssh.jobs[jid].state = "COMPLETED"

    simulator = asyncio.create_task(simulate_cluster())
    
    # Run the task
    # We must patch load_profiles or pass the config path if supported by api?
    # api.run_task_async signature: (task: Task, profile: Union[str, ExecutionProfile] = None, ...)
    # If we pass the profile object we loaded, it skips reloading config from disk default locations.
    
    result = await run_task_async(task, profile=profile)
    
    await simulator

    # result.status is a JobStatus object
    assert result.status.state == JobState.COMPLETED_OK
    assert result.status.exit_code == 0
    # Check if files were uploaded
    assert "/scratch/users/test/integration_job/config.ini" in mock_ssh.files