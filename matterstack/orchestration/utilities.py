"""
Run loop and discovery utilities.

This module contains helper functions for running campaigns to completion
and discovering active runs.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.run import RunHandle
from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger(__name__)


def run_until_completion(
    run_handle: RunHandle,
    campaign: Campaign,
    poll_interval: float = 1.0,
    *,
    operator_registry: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Execute the campaign loop locally until the run is completed, failed, or cancelled.

    This function blocks until the run reaches a terminal state.
    It handles:
    - Calling step_run() repeatedly.
    - Waiting if the run is PAUSED.
    - Retrying if the run is locked by another process (graceful contention).

    Args:
        run_handle: The handle to the run.
        campaign: The campaign instance.
        poll_interval: Time to wait between ticks (seconds).
        operator_registry: Optional operator registry passed through to step_run().

    Returns:
        The final status of the run.
    """
    # Import here to avoid circular imports
    from matterstack.orchestration.step_execution import step_run

    logger.info(f"Starting local execution loop for run {run_handle.run_id}")

    while True:
        try:
            status = step_run(run_handle, campaign, operator_registry=operator_registry)

            if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                logger.info(f"Run {run_handle.run_id} finished with status: {status}")
                return status

            if status == "PAUSED":
                logger.info(f"Run {run_handle.run_id} is PAUSED. Waiting...")
                time.sleep(5)
                continue

        except RuntimeError as re:
            if "Could not acquire lock" in str(re):
                logger.warning(f"Run {run_handle.run_id} is locked by another process. Retrying...")
                time.sleep(1)
                continue
            else:
                raise re
        except Exception as e:
            logger.error(f"Error in execution loop: {e}")
            raise

        time.sleep(poll_interval)


def list_active_runs(base_path: Path = Path("workspaces")) -> List[RunHandle]:
    """
    Scan for runs that are active (PENDING, RUNNING, PAUSED).

    Iterates through all workspaces in base_path and their runs.

    Args:
        base_path: Root path for workspaces.

    Returns:
        List of RunHandle objects for active runs.
    """
    active_runs: List[RunHandle] = []

    if not base_path.exists():
        return active_runs

    # Iterate workspaces
    for ws_dir in base_path.iterdir():
        if not ws_dir.is_dir():
            continue

        runs_dir = ws_dir / "runs"
        if not runs_dir.exists():
            continue

        # Iterate runs
        for run_dir in runs_dir.iterdir():
            if not run_dir.is_dir():
                continue

            db_path = run_dir / "state.sqlite"
            if not db_path.exists():
                continue

            try:
                # Construct RunHandle
                run_id = run_dir.name
                handle = RunHandle(workspace_slug=ws_dir.name, run_id=run_id, root_path=run_dir)

                # Check status
                # Note: We create a new store instance for each check.
                # This is lightweight enough for discovery.
                store = SQLiteStateStore(handle.db_path)
                status = store.get_run_status(run_id)

                if status in ["PENDING", "RUNNING", "PAUSED"]:
                    active_runs.append(handle)

            except Exception as e:
                logger.warning(f"Failed to inspect run at {run_dir}: {e}")
                continue

    return active_runs


__all__ = [
    "run_until_completion",
    "list_active_runs",
]
