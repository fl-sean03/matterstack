# Execution Backends

MatterStack uses a "Write Once, Run Anywhere" philosophy. The `ComputeBackend` interface abstracts the differences between your laptop and a supercomputer.

## The `ComputeBackend` Interface

Every backend implements these core methods:

*   `submit(task: Task) -> str`: Dispatches a task and returns a Job ID.
*   `poll(job_id: str) -> JobStatus`: Checks if the task is PENDING, RUNNING, COMPLETED, or FAILED.
*   `get_logs(job_id: str) -> Dict`: Retrieves stdout and stderr.
*   `cancel(job_id: str)`: Stops the execution.

## Available Backends

### 1. LocalBackend (`matterstack.runtime.backends.local`)

This backend runs tasks as subprocesses on the local machine.

*   **Use Case**: Development, debugging, small-scale simulations, running on single-node workstations.
*   **Behavior**:
    *   Creates a directory for each task.
    *   Executes the command in a shell.
    *   Supports `max_concurrent_tasks` to limit CPU usage.
    *   Ignores `image` (runs in the host environment).

**Configuration**:
```python
from matterstack.runtime.backends.local import LocalBackend

backend = LocalBackend(workspace_root="./results")
```

### 2. SlurmBackend (`matterstack.runtime.backends.hpc`)

This backend submits tasks as jobs to a Slurm workload manager via SSH.

*   **Use Case**: Production runs on HPC clusters (e.g., Perlmutter, Summit, generic university clusters).
*   **Behavior**:
    1.  **Connect**: Establishes an SSH connection to the login node.
    2.  **Stage**: Uploads input files and scripts to a remote workspace directory.
    3.  **Script Generation**: Automatically generates a `#SBATCH` script based on Task requirements (cores, memory, time).
    4.  **Submit**: Runs `sbatch submit.sh`.
    5.  **Monitor**: Polls `squeue` or `sacct` to track status.
    6.  **Retrieve**: Downloads results upon completion (optional, depending on workflow).

**Configuration**:
```python
from matterstack.runtime.backends.hpc import SlurmBackend, SSHConfig

ssh_config = SSHConfig(
    hostname="perlmutter.nersc.gov",
    username="user123",
    key_filename="/home/user/.ssh/id_rsa"
)

slurm_config = {
    "account": "m1234",
    "partition": "regular",
    "modules": ["module load python/3.9", "module load vasp/6.3"]
}

backend = SlurmBackend(
    ssh_config=ssh_config,
    workspace_root="/global/cscratch1/sd/user123/matterstack_runs",
    slurm_config=slurm_config
)
```

## Profiles

To avoid hardcoding backend details in your `main.py`, MatterStack uses **Execution Profiles**. You can define profiles in `config/profiles.py` or a YAML file, and select them at runtime.

```python
# Run with the 'hpc_prod' profile defined in config
result = run_workflow(workflow, profile="hpc_prod")