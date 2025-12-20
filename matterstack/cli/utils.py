import importlib.util
import sys
from pathlib import Path
from typing import Optional, Any
from matterstack.core.run import RunHandle

def load_workspace_context(workspace_slug: str) -> Any:
    """
    Dynamically load the workspace module and retrieve the campaign.
    
    Expects 'workspaces/{workspace_slug}/main.py' to exist.
    Supports nested slugs like 'demos/battery_screening' which resolve to
    'workspaces/demos/battery_screening/main.py'.
    
    It looks for a 'get_campaign()' function in the module.
    
    Args:
        workspace_slug: Workspace identifier, may contain '/' for nested paths.
                       e.g., 'battery_screening' or 'demos/battery_screening'
    
    Returns:
        The result of calling get_campaign() from the workspace module.
    
    Raises:
        FileNotFoundError: If the workspace main.py doesn't exist.
        ImportError: If the module cannot be loaded.
        AttributeError: If the module doesn't export get_campaign().
    """
    workspace_path = Path("workspaces") / workspace_slug
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

def find_run(run_id: str, base_path: Path = Path("workspaces")) -> Optional[RunHandle]:
    """
    Locate a run directory by searching all workspaces, including nested ones.
    
    Searches recursively for any workspace that contains runs/{run_id}.
    For nested workspaces like demos/battery_screening, returns the full
    relative path as workspace_slug.
    
    Args:
        run_id: The unique identifier of the run to find.
        base_path: The base directory to search (default: 'workspaces').
    
    Returns:
        RunHandle if found, None otherwise. For nested workspaces,
        workspace_slug will contain the full path (e.g., 'demos/battery_screening').
    """
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