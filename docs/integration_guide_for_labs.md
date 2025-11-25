# Integration Guide for Laboratories

This guide is intended for Research Software Engineers (RSEs) and Lab Managers integrating MatterStack into their facility's infrastructure.

## 1. HPC Integration

To deploy MatterStack on a new cluster (e.g., Slurm-based), follow these steps:

### A. Environment Setup
Create a standardized software environment on the cluster. We recommend using `uv` or `conda` to manage Python dependencies, or Singularity for full containerization.

```bash
# On the cluster login node
module load python/3.9
uv venv matterstack-env
source matterstack-env/bin/activate
uv pip install matterstack
```

### B. SSH Configuration
Ensure the machine running the Campaign Engine (e.g., a local workstation or a cloud VM) has password-less SSH access to the cluster's login node.

```bash
# Verify connection
ssh user@cluster.institution.edu "sbatch --version"
```

### C. Backend Configuration
Define a profile for your cluster in your workspace configuration or `profiles.py`.

```python
cluster_profile = ExecutionProfile(
    name="my_cluster",
    backend_type="slurm",
    backend_config={
        "ssh_config": {
            "hostname": "cluster.institution.edu",
            "username": "user",
            "key_filename": "~/.ssh/id_rsa"
        },
        "slurm_config": {
            "account": "project_id",
            "partition": "compute"
        },
        "workspace_root": "/scratch/user/matterstack_work"
    }
)
```

## 2. Robotic Lab Integration

Integrating a robot requires establishing a shared data channel.

### A. Network Architecture
Do **not** attempt to run MatterStack directly on the robot controller if it runs a real-time OS or legacy Windows. Instead, use a **File Transfer Gateway**.

1.  **Mount a Network Share**: Mount a NAS drive (SMB/NFS) accessible by both the MatterStack host and the robot controller.
2.  **Cloud Sync**: Alternatively, use a synced folder (Dropbox/OneDrive) or an S3 bucket with a local syncing daemon.

### B. Developing the Agent
Write a simple "Watchdog" script in the robot's native language (Python, C#, LabVIEW) that monitors the `request_path`.

**Pseudocode for Robot Watchdog:**
```python
while True:
    if exists("incoming/request.json"):
        # 1. Lock file
        rename("incoming/request.json", "incoming/processing.json")
        
        # 2. Parse instructions
        params = read_json("incoming/processing.json")
        
        # 3. Execute Hardware Driver
        result = robot_driver.run_experiment(params)
        
        # 4. Write output
        write_json("outgoing/response.json", result)
        
        # 5. Cleanup
        delete("incoming/processing.json")
        
    sleep(5)
```

### C. Testing
Use the `LocalBackend` with an `ExternalTask` pointing to local directories to test the handshake logic before connecting to the live robot.