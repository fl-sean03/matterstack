
import pytest

from matterstack.core.workflow import Task
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.hpc.ssh import SSHConfig
from matterstack.runtime.backends.local import LocalBackend


@pytest.fixture
def slurm_backend():
    config = SSHConfig(host="test", user="user")
    # Provide some global defaults to test fallback logic
    slurm_config = {
        "partition": "standard",
        "time": 60,
        "mem": 4,
        "cpus-per-task": 1
    }
    return SlurmBackend(ssh_config=config, workspace_root="/scratch/test", slurm_config=slurm_config)

@pytest.fixture
def local_backend(tmp_path):
    return LocalBackend(workspace_root=str(tmp_path), dry_run=True)

class TestSlurmResourceLogic:
    def test_generate_script_none_resources(self, slurm_backend):
        """Test generation when Task has None for resources."""
        task = Task(
            task_id="job_none",
            command="echo hello",
            image="",
            cores=None,
            memory_gb=None,
            time_limit_minutes=None,
            gpus=None
        )

        script = slurm_backend._generate_batch_script(task, "/path/to/task")

        # Should not have specific resource directives from Task (should be None or skipped)
        # But wait, current implementation might put "None" in the script which is invalid.

        # Check time
        # Current impl: lines.append(f"#SBATCH --time={task.time_limit_minutes}")
        # If None, it becomes "--time=None", which is invalid Slurm.
        # We want it to be skipped so it falls back to slurm_config["time"] or system default.

        assert "#SBATCH --time=None" not in script
        assert "#SBATCH --time=60" in script # Should fallback to global default

        # Check cores
        assert "#SBATCH --cpus-per-task=None" not in script
        assert "#SBATCH --cpus-per-task=1" in script # Fallback

        # Check memory
        assert "#SBATCH --mem=NoneG" not in script
        assert "#SBATCH --mem=4" in script # Fallback

        # Check GPUs
        # Current impl: if task.gpus > 0: -> raises TypeError if None
        # We want it to handle None safely (treat as 0/None)
        # We expect this test to crash if not handled.

    def test_generate_script_explicit_resources(self, slurm_backend):
        """Test generation when Task has explicit resources (should override defaults)."""
        task = Task(
            task_id="job_explicit",
            command="echo hello",
            image="",
            cores=4,
            memory_gb=16,
            time_limit_minutes=120,
            gpus=1
        )

        script = slurm_backend._generate_batch_script(task, "/path/to/task")

        assert "#SBATCH --time=120" in script
        assert "#SBATCH --time=60" not in script

        assert "#SBATCH --cpus-per-task=4" in script
        assert "#SBATCH --cpus-per-task=1" not in script

        assert "#SBATCH --mem=16G" in script
        assert "#SBATCH --mem=4" not in script

        assert "#SBATCH --gres=gpu:1" in script

    def test_generate_script_zero_values(self, slurm_backend):
        """Test edge case where 0 might be meaningful or mean None."""
        # For GPUs, 0 means no GPUs.
        # For cores/memory, 0 is invalid usually, but let's see how it behaves.
        # Logic should probably treat 0 as "do not set" or "set to 0" depending on context?
        # Typically we just care about None vs Value.
        pass

class TestLocalResourceLogic:
    @pytest.mark.asyncio
    async def test_submit_none_resources(self, local_backend):
        """LocalBackend should safely ignore None resources."""
        task = Task(
            task_id="local_none",
            command="echo hello",
            image="",
            cores=None,
            memory_gb=None,
            time_limit_minutes=None,
            gpus=None
        )

        # This shouldn't crash
        await local_backend.submit(task)

        assert local_backend._jobs["local_none"].state.name == "COMPLETED_OK"
