"""
Run initialization and resumption logic.

This module contains functions for initializing new runs and resuming existing ones.
"""
from __future__ import annotations
import logging
import uuid
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.campaign import Campaign
from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger(__name__)


class RunLifecycleError(Exception):
    """Exception raised for run lifecycle errors."""
    pass


def initialize_run(
    workspace_slug: str,
    campaign: Campaign,
    base_path: Path = Path("workspaces"),
    run_id: Optional[str] = None
) -> RunHandle:
    """
    Initialize a new run environment.
    
    1. Resolve run ID and paths.
    2. Create directory structure.
    3. Initialize SQLite DB.
    4. Execute initial plan() and store workflow.
    
    Args:
        workspace_slug: The workspace identifier.
        campaign: The campaign to run.
        base_path: Root path for workspaces.
        run_id: Optional explicit run ID.
    
    Returns:
        RunHandle for the new run.
    """
    if run_id is None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]

    # workspace/runs/run_id/
    root_path = base_path / workspace_slug / "runs" / run_id
    
    logger.info(f"Initializing run {run_id} at {root_path}")
    
    handle = RunHandle(
        workspace_slug=workspace_slug,
        run_id=run_id,
        root_path=root_path
    )
    
    # Create directories
    handle.root_path.mkdir(parents=True, exist_ok=True)
    # handle.operators_path.mkdir(exist_ok=True) # Lazy creation preferred
    # handle.evidence_path.mkdir(exist_ok=True) # Lazy creation preferred
    
    # Initialize State Store
    store = SQLiteStateStore(handle.db_path)
    
    with store.lock():
        store.create_run(handle, RunMetadata(status="PENDING"))
        
        # Initial Plan
        # We pass None as state for the first plan, or we could pass an empty dict
        # depending on Campaign contract. Assuming None is fine for "fresh start".
        workflow = campaign.plan(state=None)
        
        if workflow:
            store.add_workflow(workflow, run_id=handle.run_id)
            logger.info(f"Initialized run {run_id} with {len(workflow.tasks)} tasks.")
        else:
            logger.info(f"Initialized run {run_id} with no initial workflow (done?).")
            # store.update_run_status(run_id, "completed") # Method not yet available
        
    return handle


def initialize_or_resume_run(
    workspace_slug: str,
    campaign: Campaign,
    base_path: Path = Path("workspaces"),
    resume_run_id: Optional[str] = None,
    resume_always: bool = False
) -> RunHandle:
    """
    Initialize a new run or resume an existing one.

    Logic:
    1. If resume_run_id is provided, try to resume that specific run.
       If it doesn't exist, initialize it (specific ID creation).
    2. If no ID provided:
       - Scan workspace/runs/ for existing runs.
       - Sort by ID (timestamp based) desc.
       - Check status of latest run.
       - If Active (PENDING, RUNNING, PAUSED) -> Resume.
       - If Terminal (COMPLETED, FAILED, CANCELLED) -> Start New (unless resume_always=True).
    
    Args:
        workspace_slug: The workspace identifier.
        campaign: The campaign to run.
        base_path: Root path for workspaces.
        resume_run_id: Explicit run ID to resume.
        resume_always: If True, will resume the latest run even if it is terminal.
    
    Returns:
        RunHandle for the active run.
    """
    runs_dir = base_path / workspace_slug / "runs"
    
    # Case 1: Explicit Run ID
    if resume_run_id:
        target_path = runs_dir / resume_run_id
        if target_path.exists():
            logger.info(f"Resuming explicit run: {resume_run_id}")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=resume_run_id,
                root_path=target_path
            )
        else:
            logger.info(f"Run {resume_run_id} not found. Creating new run with this ID.")
            return initialize_run(workspace_slug, campaign, base_path, run_id=resume_run_id)

    # Case 2: Auto-Resume Logic
    if not runs_dir.exists():
        logger.info("No runs directory found. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)
    
    # List all subdirectories
    # Assuming run_ids are sortable (e.g. YYYYMMDD_HHMMSS_uuid)
    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not run_dirs:
        logger.info("No existing runs found. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)
        
    # Sort by name descending (latest first)
    run_dirs.sort(key=lambda p: p.name, reverse=True)
    latest_run_dir = run_dirs[0]
    latest_run_id = latest_run_dir.name
    
    logger.info(f"Found latest run: {latest_run_id}")
    
    # Check Status
    try:
        store = SQLiteStateStore(latest_run_dir / "state.sqlite")
        # We need a handle to query status via helper methods, or just use direct SQL if we didn't have helper.
        # But SQLiteStateStore takes db_path.
        status = store.get_run_status(latest_run_id)
        
        if status is None:
             # Maybe DB init failed or empty? Treat as terminal/broken -> Start new?
             # Or maybe it's fresh? status defaults to None if not found.
             # If DB exists but run record missing, it's corrupted.
             logger.warning(f"Could not determine status for {latest_run_id}. Starting new run.")
             return initialize_run(workspace_slug, campaign, base_path)
             
        logger.info(f"Latest run status: {status}")
        
        active_statuses = ["PENDING", "RUNNING", "PAUSED"]
        
        if status in active_statuses:
            logger.info(f"Resuming active run {latest_run_id}")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=latest_run_id,
                root_path=latest_run_dir
            )
        elif resume_always:
            logger.info(f"Resuming terminal run {latest_run_id} (resume_always=True)")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=latest_run_id,
                root_path=latest_run_dir
            )
        else:
            logger.info(f"Latest run is terminal ({status}). Starting new run.")
            return initialize_run(workspace_slug, campaign, base_path)
            
    except Exception as e:
        logger.warning(f"Error checking run {latest_run_id}: {e}. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)


__all__ = [
    "RunLifecycleError",
    "initialize_run",
    "initialize_or_resume_run",
]
