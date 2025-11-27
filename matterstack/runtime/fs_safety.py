from __future__ import annotations
import os
from pathlib import Path

class PathSafetyError(Exception):
    """Raised when a path safety check fails."""
    pass

def ensure_under_run_root(run_root: Path, target: Path) -> Path:
    """
    Ensure that the target path is contained within the run_root.
    Resolves both paths to their absolute form before checking.
    
    Args:
        run_root: The root directory of the run.
        target: The target path to check.
        
    Returns:
        The resolved absolute target path.
        
    Raises:
        PathSafetyError: If the target is not within the run_root.
    """
    try:
        abs_root = run_root.resolve()
        abs_target = target.resolve()
    except OSError as e:
        # Handling cases where the path might not exist yet but we want to check its potential location
        # If resolve fails (e.g. on Windows sometimes), we try abspath
        abs_root = Path(os.path.abspath(run_root))
        abs_target = Path(os.path.abspath(target))

    # If the target doesn't exist, resolve() might still work if the parent exists, 
    # but strictly speaking resolve() usually follows symlinks. 
    # For safety, we want the physical path.
    
    # Check if abs_target starts with abs_root
    # We use os.path.commonpath to safely check containment
    try:
        common = os.path.commonpath([abs_root, abs_target])
    except ValueError:
        # Paths are on different drives
        raise PathSafetyError(f"Target path {target} is on a different drive than run root {run_root}")

    if Path(common) != abs_root:
        raise PathSafetyError(f"Target path {target} escapes run root {run_root}")
        
    return abs_target

def operator_run_dir(run_root: Path, operator_type: str, uuid: str) -> Path:
    """
    Construct and validate a safe directory path for an operator instance.
    
    Args:
        run_root: The root directory of the run.
        operator_type: The type of operator (e.g., "human", "hpc").
        uuid: The unique identifier for this operator instance.
        
    Returns:
        The absolute path to the operator's directory.
    """
    # Sanitize inputs
    op_type_clean = "".join(c for c in operator_type.lower() if c.isalnum() or c in "_-")
    uuid_clean = "".join(c for c in uuid if c.isalnum() or c in "-")
    
    # Construct relative path
    relative_path = Path("operators") / op_type_clean / uuid_clean
    
    # Construct full path
    full_path = run_root / relative_path
    
    # Verify safety (though construction is safe, we double check)
    return ensure_under_run_root(run_root, full_path)