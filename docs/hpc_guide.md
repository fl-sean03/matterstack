# High Performance Computing (HPC) Guide

MatterStack provides robust integration with HPC environments, allowing you to scale your computational workflows seamlessly from local development to large-scale clusters. This guide details how to configure and use these capabilities.

## 1. Backends

The core of HPC integration is the `ComputeBackend`. MatterStack supports:

*   **LocalBackend**: Executes tasks on the local machine using subprocesses. Ideal for development, testing, and lightweight tasks.
*   **SlurmBackend**: Submits tasks to a Slurm workload manager on a remote cluster via SSH.

### 1.1. LocalBackend

No special configuration is needed. It is the default for many examples.

```python
from matterstack.runtime.backends.local import LocalBackend

backend = LocalBackend(workspace_root="./local_workspace")
```

### 1.2. SlurmBackend

Requires SSH access to the cluster.

```python
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.hpc.ssh import SSHConfig

ssh_cfg = SSHConfig(
    host="login.cluster.edu",
    user="myuser",
    key_path="~/.ssh/id_rsa"
)

backend = SlurmBackend(
    ssh_config=ssh_cfg,
    workspace_root="/scratch/myuser/matterstack_workspace",
    slurm_config={
        "account": "myaccount",
        "partition": "compute",
        "modules": ["lammps/2023"]
    }
)
```

## 2. Defining Tasks

A `Task` represents a unit of work.

```python
from matterstack.core.workflow import Task

task = Task(
    task_id="sim_001",
    command="lmp -in run.in",
    
    # Files to upload to the task directory
    files={
        "run.in": Path("inputs/run.in"),
        "data.file": Path("inputs/structure.data")
    },
    
    # Resource Requirements
    cores=32,
    memory_gb=64,
    time_limit_minutes=120,
    gpus=1,
    
    # Environment Variables
    env={"OMP_NUM_THREADS": "1"}
)
```

## 3. Selective Download

By default, MatterStack downloads the entire task workspace when a job completes. For data-intensive simulations, you can filter which files are retrieved to save bandwidth and storage.

### Configuration

Add `download_patterns` to your `Task` definition:

```python
task = Task(
    ...,
    download_patterns={
        "include": ["results/*.json", "*.restart"],
        "exclude": ["*.tmp", "*.huge_log"]
    }
)
```

*   **include**: Only download files matching these glob patterns.
*   **exclude**: Skip files matching these patterns.

### How it Works
*   **Local**: Filters files during the copy operation.
*   **Slurm**: Filters files *before* transfer over SSH/SFTP, ensuring efficient data movement.

## 4. The Compute Operator

The `ComputeOperator` (often aliased as `DirectHPC`) manages the lifecycle of a task on a backend.

1.  **Prepare**: Creates a local tracking directory (`runs/<id>/operators/...`) and a manifest.
2.  **Submit**: Uploads files to the backend and submits the job.
3.  **Poll**: Checks job status.
4.  **Collect**: Downloads results (respecting selective download patterns) to the local tracking directory.

## 5. Workflow Integration

In a MatterStack workflow, you typically don't instantiate backends manually in every script. Instead, they are configured via `Profiles` or dependency injection in the Orchestrator.

(See `matterstack/config/profiles.py` for advanced configuration patterns).