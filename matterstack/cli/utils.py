import importlib.util
import os
import sys
from pathlib import Path
from typing import Optional, Any
from matterstack.core.run import RunHandle

# Environment variable for workspaces root path
_ENV_WORKSPACES_ROOT = "MATTERSTACK_WORKSPACES_ROOT"


def _find_project_root() -> Optional[Path]:
    """
    Find project root by walking up from CWD to find pyproject.toml.
    
    Returns:
        Path to project root if found, None otherwise.
    """
    current = Path.cwd().resolve()
    
    # Walk up directory tree
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent
    
    return None


def _resolve_workspaces_root() -> Optional[Path]:
    """
    Resolve the workspaces root directory using multi-level strategy.
    
    Resolution order:
    1. Environment variable MATTERSTACK_WORKSPACES_ROOT (explicit, highest priority)
    2. Project root detection via pyproject.toml, then <project_root>/workspaces
    3. Hardcoded fallback: Path("workspaces") if it exists (backward compat)
    
    Returns:
        Path to workspaces root if found, None otherwise.
    """
    # 1. Check environment variable (highest priority)
    env_path = os.environ.get(_ENV_WORKSPACES_ROOT)
    if env_path:
        path = Path(env_path)
        # Return env var path even if it doesn't exist - let caller handle
        return path
    
    # 2. Project root detection via pyproject.toml
    project_root = _find_project_root()
    if project_root:
        workspaces_path = project_root / "workspaces"
        if workspaces_path.exists():
            return workspaces_path
    
    # 3. Hardcoded fallback for backward compatibility
    fallback = Path("workspaces")
    if fallback.exists():
        return fallback
    
    return None

def load_workspace_context(workspace_slug: str, base_path: Optional[Path] = None) -> Any:
    """
    Dynamically load the workspace module and retrieve the campaign.
    
    Expects '{base_path}/{workspace_slug}/main.py' to exist.
    Supports nested slugs like 'demos/battery_screening' which resolve to
    '{base_path}/demos/battery_screening/main.py'.
    
    It looks for a 'get_campaign()' function in the module.
    
    Args:
        workspace_slug: Workspace identifier, may contain '/' for nested paths.
                       e.g., 'battery_screening' or 'demos/battery_screening'
        base_path: Base workspaces directory. If None, uses multi-level resolution:
                   1. MATTERSTACK_WORKSPACES_ROOT env var
                   2. Project root detection via pyproject.toml
                   3. Hardcoded "workspaces" fallback
    
    Returns:
        The result of calling get_campaign() from the workspace module.
    
    Raises:
        RuntimeError: If workspaces root cannot be resolved.
        FileNotFoundError: If the workspace main.py doesn't exist.
        ImportError: If the module cannot be loaded.
        AttributeError: If the module doesn't export get_campaign().
    """
    if base_path is None:
        base_path = _resolve_workspaces_root()
        if base_path is None:
            raise RuntimeError(
                "Cannot find workspaces directory. Either:\n"
                f"  1. Set {_ENV_WORKSPACES_ROOT} environment variable\n"
                "  2. Run from project root (containing pyproject.toml)\n"
                "  3. Ensure 'workspaces' directory exists in current directory"
            )
    
    workspace_path = base_path / workspace_slug
    main_py = workspace_path / "main.py"
    
    if not main_py.exists():
        raise FileNotFoundError(f"Workspace main file not found: {main_py}")
    
    # Create valid Python module name: demos/battery_screening -> workspace.demos.battery_screening
    module_name = f"workspace.{workspace_slug.replace('/', '.')}"
    
    spec = importlib.util.spec_from_file_location(module_name, main_py)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {main_py}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    
    if hasattr(module, "get_campaign"):
        return module.get_campaign()
    else:
        raise AttributeError(f"Workspace module {main_py} does not export 'get_campaign()'.")


def find_run(run_id: str, base_path: Optional[Path] = None) -> Optional[RunHandle]:
    """
    Locate a run directory by searching all workspaces, including nested ones.
    
    Searches recursively for any workspace that contains runs/{run_id}.
    For nested workspaces like demos/battery_screening, returns the full
    relative path as workspace_slug.
    
    Args:
        run_id: The unique identifier of the run to find.
        base_path: The base directory to search. If None, uses multi-level resolution:
                   1. MATTERSTACK_WORKSPACES_ROOT env var
                   2. Project root detection via pyproject.toml
                   3. Hardcoded "workspaces" fallback
    
    Returns:
        RunHandle if found, None otherwise. For nested workspaces,
        workspace_slug will contain the full path (e.g., 'demos/battery_screening').
    """
    if base_path is None:
        base_path = _resolve_workspaces_root()
        if base_path is None:
            return None
    
    if not base_path.exists():
        return None
    
    # Search for any 'runs' directory that contains the run_id
    for runs_dir in base_path.rglob("runs"):
        if runs_dir.is_dir():
            run_dir = runs_dir / run_id
            if run_dir.exists():
                # Calculate workspace_slug relative to base_path
                # runs_dir.parent is the workspace directory
                workspace_path = runs_dir.parent
                workspace_slug = str(workspace_path.relative_to(base_path))
                return RunHandle(
                    workspace_slug=workspace_slug,
                    run_id=run_id,
                    root_path=run_dir
                )
    return None