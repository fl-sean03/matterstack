from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict
from pathlib import Path

from matterstack.storage.state_store import SQLiteStateStore
from matterstack.core.operators import ExternalRunStatus

logger = logging.getLogger(__name__)

@dataclass
class BlockingItem:
    task_id: str
    status: str  # PENDING, RUNNING, WAITING_EXTERNAL
    reason: str
    operator_type: Optional[str] = None
    hint: Optional[str] = None
    path: Optional[str] = None

def get_status_hint(operator_type: str, operator_data: Dict, run_root: Path) -> str:
    """
    Generate a helpful hint for the user based on the operator type.
    """
    path_str = operator_data.get("absolute_path")
    if not path_str:
        return "Check operator logs."

    op_path = Path(path_str)
    
    # Try to make path relative to run root for cleaner display
    try:
        display_path = op_path.relative_to(run_root)
    except ValueError:
        display_path = op_path

    if operator_type == "Human":
        return f"Waiting for response.json in {display_path}. See instructions.md."
    elif operator_type == "ManualHPC":
        return f"Waiting for status.json or output files in {display_path}/output."
    elif operator_type == "Experiment":
        return f"Waiting for result.json in {display_path}."
    
    return f"External operator active at {display_path}."

def get_run_frontier(store: SQLiteStateStore, run_id: str, run_root: Path) -> List[BlockingItem]:
    """
    Identify tasks that are blocking the progress of the run.
    
    The frontier consists of:
    1. Tasks waiting for external action (WAITING_EXTERNAL).
    2. Tasks currently running (RUNNING).
    3. Tasks ready to run (PENDING with satisfied dependencies).
    """
    tasks = store.get_tasks(run_id)
    if not tasks:
        return []

    task_status_map = {t.task_id: store.get_task_status(t.task_id) for t in tasks}
    
    blocking_items = []
    
    for task in tasks:
        status = task_status_map.get(task.task_id)
        
        # Case 1: Waiting for External Action
        if status == "WAITING_EXTERNAL":
            ext_handle = store.get_external_run(task.task_id)
            if ext_handle:
                hint = get_status_hint(ext_handle.operator_type, ext_handle.operator_data, run_root)
                blocking_items.append(BlockingItem(
                    task_id=task.task_id,
                    status=status,
                    reason="Waiting for operator completion",
                    operator_type=ext_handle.operator_type,
                    hint=hint,
                    path=ext_handle.operator_data.get("absolute_path")
                ))
            else:
                blocking_items.append(BlockingItem(
                    task_id=task.task_id,
                    status=status,
                    reason="Marked as external but no handle found."
                ))
                
        # Case 2: Currently Running
        elif status == "RUNNING":
             blocking_items.append(BlockingItem(
                task_id=task.task_id,
                status=status,
                reason="System is executing this task."
            ))

        # Case 3: Pending
        elif status == "PENDING" or status is None:
            # Check dependencies
            deps_met = True
            missing_deps = []
            for dep_id in task.dependencies:
                dep_status = task_status_map.get(dep_id)
                if dep_status != "COMPLETED":
                    deps_met = False
                    missing_deps.append(dep_id)
            
            if deps_met:
                blocking_items.append(BlockingItem(
                    task_id=task.task_id,
                    status="READY", # It's pending but ready
                    reason="Ready to run. Waiting for scheduler or concurrency slot."
                ))
            else:
                # If dependencies are not met, this task is blocked by upstream tasks.
                # We don't list it as a *primary* blocker unless all upstream tasks are completed/failed?
                # No, if dependencies are missing, the *missing dependencies* are the ones in the frontier (or their deps).
                # So we skip this task, as it's not the "frontier".
                pass
                
    return blocking_items