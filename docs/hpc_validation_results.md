# HPC Validation Results: CURC `atesting` Partition

**Date:** 2025-12-04
**Status:** Resolved
**Issue:** Segmentation Fault (Signal 11) during LAMMPS execution on `atesting` partition.

## 1. Executive Summary

A persistent Segmentation Fault (Signal 11) was observed when running LAMMPS simulations for the MXene shear campaign on the `atesting` partition of the Alpine cluster. After a comprehensive debugging campaign, the issue was identified as an **Out-Of-Memory (OOM)** condition, not a software incompatibility or data corruption issue.

The default memory allocation on the `atesting` partition (often 1GB/core) was insufficient for the `mxene_water_050` system combined with the Python wrapper overhead. 

**Solution:** Explicitly requesting **4GB of memory** per task resolved the segmentation faults across all pipeline stages.

## 2. Debugging Methodology

We employed a layered "5-level" verification process to isolate the failure point, moving from the lowest level (binary execution) to the highest (full orchestration).

### Level 1: Binary & Input Verification
*   **Script:** `workspaces/hpc_debug/02_test_mxene_input.py`
*   **Objective:** Verify that `mpirun lmp_mpi` can successfully run the `minimize.inp` simulation with `mxene_water_050.data` directly via SSH/sbatch, bypassing the orchestration layer.
*   **Finding:** The job initially failed with Segfault. Increasing memory to 4GB in the generated `submit.sh` allowed it to pass. This proved the physics and binary were compatible with the hardware.

### Level 2: Matterstack Pipeline Verification
*   **Script:** `workspaces/hpc_debug/03_test_matterstack_task.py`
*   **Objective:** Verify that the `Matterstack` `SlurmBackend` correctly constructs the sbatch script and manages job lifecycle.
*   **Finding:** Confirmed that `SlurmBackend` was correctly passing parameters. With `memory_gb=4` configured in the `Task` object, the job completed successfully.

### Level 3: Python Wrapper Verification
*   **Script:** `workspaces/hpc_debug/04_test_python_wrapper.py`
*   **Objective:** Verify that the `run_equilibration.py` Python wrapper (which adds overhead) executes correctly under the resource constraints.
*   **Finding:** The wrapper script adds memory overhead compared to raw LAMMPS. With 4GB allocated, the wrapper successfully launched LAMMPS and completed the equilibration cycle.

### Level 4: Full Campaign Verification
*   **Script:** `workspaces/hpc_debug/05_test_full_campaign.py`
*   **Objective:** Verify the full `TaskBuilder` and orchestration logic works end-to-end.
*   **Finding:** Using `sim_config_overrides` to force `memory_gb: 4` for all stages (equilibration, calibration, sweep) resulted in a successful campaign run.

## 3. Root Cause Analysis

The root cause was resource starvation.

1.  **Symptoms:** Immediate Segmentation Faults (Signal 11) or glibc memory corruption errors upon LAMMPS startup or shortly into the minimization phase.
2.  **Environment:** The `atesting` partition is a resource-constrained environment intended for short, small debug jobs. Its default memory-per-cpu configuration is likely low (approx. 1GB).
3.  **Workload:**
    *   **System:** `mxene_water_050` (approx. 4000-5000 atoms).
    *   **Software:** LAMMPS (MPI version) wrapped in a Python process (`run_equilibration.py`).
4.  **Mechanism:** The combined memory footprint of the Python interpreter, necessary libraries (numpy, etc.), and the LAMMPS MPI process exceeded the default allocation. The OS terminated the process with a Segfault (standard OOM behavior in some HPC configurations) rather than a clear "Out of Memory" error message.

## 4. Verified Configuration

To run successfully on `atesting` (and likely `amilan` for larger systems), the following resource configurations must be used.

### Slurm Configuration (`HPC_atesting_config.yaml` snippet)
```yaml
cluster:
  slurm:
    partition: "atesting"
    qos: "testing"
    ntasks: 1
    # Note: Time limits on atesting are strict (usually max 1 hour)
```

### Resource Request (Matterstack/Validation Code)
When constructing the campaign, explicit resource overrides are required:

```python
sim_config_overrides={
    "execution_mode": "real",
    "resources": {
        "equilibration": {"time_hours": 0.05, "memory_gb": 4},
        "calibration":   {"time_hours": 0.05, "memory_gb": 4},
        "sweep":         {"time_hours": 0.05, "memory_gb": 4}
    }
}
```

## 5. Artifacts

The following debug scripts were created and preserved in `workspaces/hpc_debug/` for future reference:

| File | Purpose |
|------|---------|
| `02_test_mxene_input.py` | Direct SSH/Paramiko test of LAMMPS binary + Input data. |
| `03_test_matterstack_task.py` | Unit test for `SlurmBackend` submission logic. |
| `04_test_python_wrapper.py` | Unit test for `run_equilibration.py` wrapper execution. |
| `05_test_full_campaign.py` | End-to-end validation of the full campaign pipeline. |

These scripts can be re-run to verify environment health if issues recur.