# Running LAMMPS on CURC Alpine

This guide explains how to correctly load modules and run LAMMPS on the CURC Alpine cluster, addressing common issues related to module visibility and dependencies.

## Key Concept: The Alpine Software Stack

The CURC Alpine cluster organizes its software stack in a hierarchy that is NOT fully visible by default when using `sbatch` on certain partitions (like `atesting`). To access the main software stack (compilers, MPIs, libraries), you must explicitly add the module paths.

### 1. Missing Modules ("module unknown")

If you encounter errors like `Lmod has detected the following error: The following module(s) are unknown: "intel/2022.1.2"`, it is likely because `MODULEPATH` is missing the Alpine module roots.

**Solution:**
You must run `module use` commands to add the following paths:
*   `/curc/sw/alpine-modules-21/compilers` (for Intel, GCC, NVHPC)
*   `/curc/sw/alpine-modules-21/idep` (for Independent dependencies like Python)

### 2. The `loadPkgDefaults` Error

When loading `intel/2022.1.2`, you may see a Lua error: `attempt to call a nil value (global 'loadPkgDefaults')`. This occurs because the Lmod environment on the compute node hasn't sourced the site-specific package definitions.

**Solution:**
Explicitly set the `LMOD_PACKAGE_PATH` before loading modules:
```bash
export LMOD_PACKAGE_PATH=/curc/sw/alpine-modules-21/.site
```

### 3. Loading LAMMPS and Dependencies

LAMMPS modules on Alpine are often nested deep within the hierarchy (Compiler -> MPI -> Application). Furthermore, the `intel` module might fail to automatically add downstream paths if the environment isn't perfect.

You must manually load the hierarchy layer by layer to ensure success.

## Recommended SLURM Script Structure

Here is a robust template for a SLURM submission script that correctly sets up the environment for LAMMPS (specifically `lammps/2Aug23` with Intel OneAPI 2022.1.2).

```bash
#!/bin/bash
#SBATCH --job-name=lammps_production
#SBATCH --partition=amilan  # or atesting, amilan, etc.
#SBATCH --nodes=1
#SBATCH --ntasks=64
#SBATCH --time=24:00:00
#SBATCH --output=slurm-%j.out

# ---- 1. Initialize Lmod Environment ----
module purge
# Fix for 'loadPkgDefaults' error in Intel modules
export LMOD_PACKAGE_PATH=/curc/sw/alpine-modules-21/.site

# ---- 2. Add Module Paths ----
# Enable Independent modules (Python, etc.)
module use /curc/sw/alpine-modules-21/idep
# Enable Compilers (Intel, GCC)
module use /curc/sw/alpine-modules-21/compilers

# ---- 3. Load Compiler ----
# This makes 'mpis/intel/...' available
module load intel/2022.1.2

# ---- 4. Load MPI ----
# Explicitly add path if auto-load fails
module use /curc/sw/alpine-modules-21/mpis/intel/2022.1.2
module load impi/2021.5.0

# ---- 5. Load Dependencies ----
# LAMMPS requires zlib and jpeg, found in cdep (Compiler Dependencies)
module use /curc/sw/alpine-modules-21/cdep/intel/2022.1.2
module load zlib/1.2.11
module load jpeg/9e

# ---- 6. Load LAMMPS ----
# Found in mdep (Module Dependencies)
module use /curc/sw/alpine-modules-21/mdep/impi/2021.5.0/intel/2022.1.2
module load lammps/2Aug23

# ---- 7. Verification ----
module list

# ---- 8. Run ----
# Use 'lmp_mpi' executable provided by the module
mpirun lmp_mpi -in input.inp -log log.lammps
```

## Troubleshooting

*   **mpirun: command not found**: Ensure `impi` module is loaded.
*   **execvp error on file lmp_mpi**: The `lmp_mpi` executable is not in your PATH. Verify `module load lammps` succeeded.
*   **Unknown module "python/..."**: Ensure you added `module use /curc/sw/alpine-modules-21/idep`.
*   **Unknown module "zlib/..."**: Ensure you added `module use /curc/sw/alpine-modules-21/cdep/intel/2022.1.2`.

This manual approach is more verbose but guarantees that the exact dependency tree is reconstructed regardless of the default environment state on the compute node.