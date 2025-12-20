# CURC HPC Development Guidelines

This document outlines key considerations, constraints, and best practices for developing software intended to run on the CU Research Computing (CURC) Alpine cluster. These insights are based on practical experience debugging execution environments on the cluster.

## 1. Python Environment Constraints

The default system Python environment on Alpine compute nodes is **Python 3.6.8**. This is significantly older than modern development standards (3.10+), leading to several common compatibility pitfalls.

### 1.1 Core Language Features
*   **Dataclasses**: The `dataclasses` module (introduced in 3.7) is **NOT** available.
    *   *Solution*: Use `typing.NamedTuple` for simple data containers, or standard classes.
*   **Type Annotations**: The `from __future__ import annotations` feature (introduced in 3.7) is **NOT** supported and will cause `SyntaxError`.
    *   *Solution*: Use string forward references (e.g., `'MyClass'`) if needed, and import `List`, `Dict`, `Tuple`, etc., from `typing`. Do not use the generic alias syntax like `list[int]` (introduced in 3.9); use `List[int]`.
*   **Subprocess**: `subprocess.run(..., capture_output=True)` (introduced in 3.7) is not available.
    *   *Solution*: Use `stdout=subprocess.PIPE, stderr=subprocess.PIPE`.

### 1.2 Libraries
*   **NumPy**: The system Python includes `numpy` (version ~1.19.5), but features like `numpy.random.Generator` (the new random API) may behave inconsistently or be unavailable in the specific execution context.
    *   *Recommendation*: Stick to the legacy `numpy.random.RandomState` API for maximum reliability on this platform.
*   **SciPy**: `scipy` (version ~1.5.4) is available.
*   **Newer Python Versions**: While `/usr/bin/python3.11` exists on some nodes, it may **not** have `numpy` or `scipy` installed system-wide. Unless you explicitly manage a virtual environment (conda/venv) that installs these, relying on the system `python3` (3.6) is often the path of least resistance for simple scripts.

## 2. Module Management

*   **System Python vs Modules**: The `module load python` command may not always be necessary or available depending on the user's `.bashrc` or the node configuration. The system binary `/usr/bin/python3` is reliable but old.
*   **Module Purge**: Be careful with `module purge` in your job scripts. While it ensures a clean environment, it might remove access to expected default paths. Ensure you explicitly load `slurm` if needed, though it is typically auto-loaded on compute nodes.

## 3. Best Practices for Remote Execution

### 3.1 Code Portability
Write your simulation scripts (the code running on the HPC) to be "conservative" in its Python usage.
*   Avoid bleeding-edge language features.
*   Test your code locally with an older Python version if possible (e.g., using `pyenv` or `conda` to create a 3.6 environment) to catch syntax errors early.

### 3.2 Debugging Remote Jobs
*   **Logs**: Always ensure `stdout` and `stderr` are captured. The `SlurmBackend` typically handles this, but if a job fails instantly (e.g., due to a `SyntaxError` in imports), the error will be in the log file on the remote system.
*   **Dependency Check**: Before running a complex campaign, create a simple task that runs `python3 -c "import numpy; import scipy; print('OK')"` to verify the environment is what you expect.

## 4. Checklist for New Workspaces

When creating a new workspace intended for HPC execution:
1.  [ ] **Downgrade Syntax**: Check scripts for `dataclasses`, `f-strings` (f-strings are OK in 3.6, but check other 3.7+ features), and modern type hints.
2.  [ ] **Check Imports**: Ensure all imported libraries are available on the standard CURC stack or provided in your `library/` folder.
3.  [ ] **Verify Config**: Ensure `HPC_config.yaml` points to the correct partition (e.g., `atesting` for tests) and account.
4.  [ ] **Legacy Random**: If using stochastic methods, ensure `numpy` usage is compatible with v1.19.
