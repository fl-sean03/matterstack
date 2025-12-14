import pytest
from matterstack.runtime.backends.hpc.backend import SlurmBackend, SSHConfig
from matterstack.core.workflow import Task

@pytest.fixture
def mock_ssh_config():
    # Minimal config for testing
    return SSHConfig(host="localhost", user="test")

def test_slurm_precedence_conflict_time(mock_ssh_config):
    """
    Verify that Task.time_limit_minutes takes precedence over global 'time' config.
    """
    # Task specific setting: 60 minutes
    task = Task(image="ubuntu", command="echo hello", time_limit_minutes=60)
    
    # Global config that conflicts: 24 hours
    slurm_config = {"time": "24:00:00"}
    
    backend = SlurmBackend(ssh_config=mock_ssh_config, workspace_root="/tmp", slurm_config=slurm_config)
    
    script = backend._generate_batch_script(task, "/tmp/task_1")
    
    # We expect the Task's value (60) to be present
    assert "#SBATCH --time=60" in script
    
    # We expect the Global value to be ABSENT because Task overrides it.
    # If the logic is "append global after task", this assertion would fail in the old code (if looking for unique lines)
    # or fail if we check that only one exists. 
    # The requirement is that the generated script uses the task value. 
    # Since Slurm uses the last value, if both are present, the last one wins.
    # Our fix will ensure the global one is NOT written if the task one is present.
    assert "#SBATCH --time=24:00:00" not in script

def test_slurm_precedence_conflict_cores(mock_ssh_config):
    """
    Verify that Task.cores takes precedence over global 'cpus-per-task' config.
    """
    # Task specific setting: 4 cores
    task = Task(image="ubuntu", command="echo hello", cores=4)
    
    # Global config that conflicts: 8 cores
    slurm_config = {"cpus-per-task": "8"}
    
    backend = SlurmBackend(ssh_config=mock_ssh_config, workspace_root="/tmp", slurm_config=slurm_config)
    
    script = backend._generate_batch_script(task, "/tmp/task_1")
    
    assert "#SBATCH --cpus-per-task=4" in script
    assert "#SBATCH --cpus-per-task=8" not in script

def test_slurm_precedence_conflict_memory(mock_ssh_config):
    """
    Verify that Task.memory_gb takes precedence over global 'mem' config.
    """
    # Task: 16 GB
    task = Task(image="ubuntu", command="echo hello", memory_gb=16)
    
    # Global: 32 GB (syntax might be 32G or just 32000)
    slurm_config = {"mem": "32G"}
    
    backend = SlurmBackend(ssh_config=mock_ssh_config, workspace_root="/tmp", slurm_config=slurm_config)
    
    script = backend._generate_batch_script(task, "/tmp/task_1")
    
    assert "#SBATCH --mem=16G" in script
    assert "#SBATCH --mem=32G" not in script

def test_slurm_global_only_partition(mock_ssh_config):
    """
    Verify that global config is used when Task has no opinion (e.g. partition).
    """
    # Task has no opinion on partition
    task = Task(image="ubuntu", command="echo hello")
    
    slurm_config = {"partition": "debug"}
    
    backend = SlurmBackend(ssh_config=mock_ssh_config, workspace_root="/tmp", slurm_config=slurm_config)
    
    script = backend._generate_batch_script(task, "/tmp/task_1")
    
    assert "#SBATCH --partition=debug" in script

def test_slurm_global_defaults_respected_if_task_missing(mock_ssh_config):
    """
    Verify that if Task attribute is None (if possible), global is used.
    Note: Task.time_limit_minutes has a default, but cores/memory are Optional[int].
    """
    # Task with None cores/memory
    task = Task(image="ubuntu", command="echo hello")
    task.cores = None
    task.memory_gb = None # Explicitly None to test fallback
    
    slurm_config = {"cpus-per-task": "2", "mem": "4G"}
    
    backend = SlurmBackend(ssh_config=mock_ssh_config, workspace_root="/tmp", slurm_config=slurm_config)
    
    script = backend._generate_batch_script(task, "/tmp/task_1")
    
    assert "#SBATCH --cpus-per-task=2" in script
    assert "#SBATCH --mem=4G" in script