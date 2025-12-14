import importlib.util
import sys
from pathlib import Path
from typing import Optional, Any
from matterstack.core.run import RunHandle

def load_workspace_context(workspace_slug: str) -> Any:
    """
    Dynamically load the workspace module and retrieve the campaign.
    Expects 'workspaces/{workspace_slug}/main.py' to exist.
    It looks for a 'get_campaign()' function.
    """
    workspace_path = Path("workspaces") / workspace_slug
    main_py = workspace_path / "main.py"
    
    if not main_py.exists():
        raise FileNotFoundError(f"Workspace main file not found: {main_py}")
        
    spec = importlib.util.spec_from_file_location(f"workspace.{workspace_slug}", main_py)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {main_py}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"workspace.{workspace_slug}"] = module
    spec.loader.exec_module(module)
    
    if hasattr(module, "get_campaign"):
        return module.get_campaign()
    else:
        raise AttributeError(f"Workspace module {main_py} does not export 'get_campaign()'.")

def find_run(run_id: str, base_path: Path = Path("workspaces")) -> Optional[RunHandle]:
    """
    Locate a run directory by searching all workspaces.
    """
    if not base_path.exists():
        return None
        
    for ws_dir in base_path.iterdir():
        if ws_dir.is_dir():
            run_dir = ws_dir / "runs" / run_id
            if run_dir.exists():
                return RunHandle(
                    workspace_slug=ws_dir.name,
                    run_id=run_id,
                    root_path=run_dir
                )
    return None