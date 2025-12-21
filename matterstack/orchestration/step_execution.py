"""
Main step execution coordinator.

This module contains the step_run function that orchestrates one tick
of the run lifecycle, coordinating POLL, PLAN, EXECUTE, and ANALYZE phases.
"""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

from matterstack.core.run import RunHandle
from matterstack.core.campaign import Campaign
from matterstack.core.operators import ExternalRunStatus
from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.storage.state_store import SQLiteStateStore

from matterstack.orchestration.polling import (
    task_status_from_external_status,
    poll_active_attempts,
    poll_legacy_external_runs,
)
from matterstack.orchestration.dispatch import (
    calculate_concurrency_slots,
    get_max_hpc_jobs,
    determine_operator_type,
    submit_task_to_operator,
    submit_external_task_stub,
    submit_local_simulation,
)
from matterstack.orchestration.analyze import execute_analyze_phase

logger = logging.getLogger(__name__)


def _build_default_operator_registry(run_handle: RunHandle) -> Dict[str, Any]:
    """
    Build the default operator registry.
    
    This creates operators with both legacy keys and canonical v0.2.6 keys
    for backward compatibility.
    
    Args:
        run_handle: The run handle.
    
    Returns:
        Dict mapping operator keys to operator instances.
    """
    from matterstack.runtime.operators.human import HumanOperator
    from matterstack.runtime.operators.experiment import ExperimentOperator
    from matterstack.runtime.operators.hpc import ComputeOperator
    from matterstack.runtime.backends.local import LocalBackend

    local_backend = LocalBackend(workspace_root=run_handle.root_path)
    human_op = HumanOperator()
    experiment_op = ExperimentOperator()
    local_op = ComputeOperator(backend=local_backend, slug="local", operator_name="Local")
    hpc_op = ComputeOperator(backend=local_backend, slug="hpc", operator_name="HPC")
    
    # Register with both legacy keys and canonical v0.2.6 keys for compatibility
    return {
        # Legacy keys (v0.2.5 and earlier)
        "Human": human_op,
        "Experiment": experiment_op,
        "Local": local_op,
        "HPC": hpc_op,
        # Canonical keys (v0.2.6+)
        "human.default": human_op,
        "experiment.default": experiment_op,
        "local.default": local_op,
        "hpc.default": hpc_op,
    }


def step_run(
    run_handle: RunHandle,
    campaign: Campaign,
    operator_registry: Optional[Dict[str, Any]] = None
) -> str:
    """
    Execute one 'tick' of the run lifecycle.
    
    This function orchestrates:
    1. POLL Phase: Check status of active attempts and external runs
    2. PLAN Phase: Check dependencies and find ready tasks
    3. EXECUTE Phase: Submit ready tasks to operators
    4. ANALYZE Phase: If workflow complete, analyze and replan
    
    Args:
        run_handle: The handle to the run.
        campaign: The campaign instance.
        operator_registry: Optional operator registry. If None, a default is built.
    
    Returns:
        Current status of the run ("active", "completed", "failed", "PAUSED", etc.)
    """
    store = SQLiteStateStore(run_handle.db_path)
    
    with store.lock():
        # 0. Check Run Status
        run_status = store.get_run_status(run_handle.run_id)
        if run_status == "PENDING":
             logger.info(f"Run {run_handle.run_id} started (transition from PENDING to RUNNING)")
             store.set_run_status(run_handle.run_id, "RUNNING")
             run_status = "RUNNING"
        
        if run_status in ["CANCELLED", "FAILED", "COMPLETED"]:
            logger.info(f"Run {run_handle.run_id} is {run_status}. Skipping execution.")
            return run_status
            
        if run_status == "PAUSED":
             logger.info(f"Run {run_handle.run_id} is PAUSED. Skipping EXECUTE phase.")
             # We might still want to POLL external tasks, but for now we skip everything.
             # This prevents new tasks from being submitted.
             return "PAUSED"

        # Build operator registry if not provided
        if operator_registry:
            operators = operator_registry
        else:
            operators = _build_default_operator_registry(run_handle)

        # 1. POLL Phase: attempt-aware polling (schema v2 primary path)
        attempt_task_ids = store.get_attempt_task_ids(run_handle.run_id)
        
        # Poll active attempts (v2)
        poll_active_attempts(run_handle.run_id, store, operators)
        
        # Poll legacy external runs (v1 fallback) ONLY for tasks that have no attempts
        poll_legacy_external_runs(run_handle.run_id, store, operators, attempt_task_ids)

        # 2. PLAN Phase: Check dependencies and find ready tasks
        tasks = store.get_tasks(run_handle.run_id)
    
        # Map task_id -> status
        task_status_map = {t.task_id: store.get_task_status(t.task_id) for t in tasks}
        
        # Calculate stats for logging
        stats = {
            "total": len(tasks),
            "completed": 0,
            "failed": 0,
            "active": 0,
            "ready": 0,
            "submitted": 0
        }

        # Identify tasks that are ready to run
        # Ready = Created (None/PENDING) AND All dependencies are COMPLETED
        tasks_to_run: List[Any] = []
        
        has_active_tasks = False
        has_failed_tasks = False
        
        for task in tasks:
            current_status = task_status_map.get(task.task_id)
            
            if current_status in ["COMPLETED", "SKIPPED"]:
                stats["completed"] += 1
                continue
                
            if current_status in ["FAILED", "CANCELLED"]:
                if task.allow_failure:
                    # Allow run to proceed even if this task failed
                    pass
                else:
                    has_failed_tasks = True
                stats["failed"] += 1
                continue
                
            if current_status in ["RUNNING", "SUBMITTED", "WAITING_EXTERNAL"]:
                has_active_tasks = True
                stats["active"] += 1
                continue

            # If status is still PENDING but there is an active attempt, don't resubmit
            if task.task_id in {a.task_id for a in store.get_active_attempts(run_handle.run_id)}:
                has_active_tasks = True
                stats["active"] += 1
                continue

            # Status is None or PENDING

            # Check dependencies
            deps_met = True
            for dep_id in task.dependencies:
                dep_status = task_status_map.get(dep_id)
                if dep_status != "COMPLETED":
                    deps_met = False
                    break
            
            if deps_met:
                tasks_to_run.append(task)
                stats["ready"] += 1
            else:
                # Still waiting on deps
                has_active_tasks = True

        # Update submitted count based on what we are about to do
        stats["submitted"] = len(tasks_to_run)

        # Log Tick Summary
        logger.info(
            f"Tick Summary: "
            f"Ready={stats['ready']}, "
            f"Submitted={stats['submitted']}, "
            f"Completed={stats['completed']}, "
            f"Failed={stats['failed']}, "
            f"Active={stats['active']}"
        )

        # 3. EXECUTE Phase
        
        # Determine concurrency limit
        max_hpc_jobs = get_max_hpc_jobs(run_handle)
        config_path = run_handle.root_path / "config.json"
        logger.info(f"Checking for config at: {config_path}")

        # Count active executions for concurrency (attempt-aware)
        active_external_count, slots_available = calculate_concurrency_slots(
            run_handle, store, max_hpc_jobs
        )
            
        logger.info(f"Concurrency Check: Active={active_external_count}, Limit={max_hpc_jobs}, Slots={slots_available}")

        # Submit ready tasks (up to limit)
        for task in tasks_to_run:
            operator_type = determine_operator_type(task, run_handle)
            
            # Apply concurrency limit if it's an external run (Operator)
            is_external = operator_type is not None or isinstance(task, (ExternalTask, GateTask))
            
            if is_external:
                if slots_available <= 0:
                    logger.info(f"Concurrency limit reached ({max_hpc_jobs}). Postponing task {task.task_id}")
                    continue
                slots_available -= 1

            logger.info(f"Submitting task {task.task_id}")

            if operator_type:
                # v2: Create attempt first, then dispatch to operator
                success = submit_task_to_operator(
                    task, operator_type, run_handle, store, operators
                )
                if success:
                    has_active_tasks = True

            elif isinstance(task, (ExternalTask, GateTask)):
                # Attempt-aware placeholder for legacy "external coordination" tasks.
                submit_external_task_stub(task, run_handle, store)
                has_active_tasks = True

            else:
                # Local Compute Task - SIMULATION MODE for Verification (no attempt record)
                submit_local_simulation(task, store)

        # 4. ANALYZE Phase
        # Check if workflow is complete (no active tasks, no pending tasks)
        # If complete, run campaign.analyze() -> plan()
        
        if not has_active_tasks and not tasks_to_run:
            # All tasks are terminal.
            # Check for failures
            if has_failed_tasks:
                logger.error("Workflow has failed tasks. Stopping.")
                store.set_run_status(run_handle.run_id, "FAILED", reason="Workflow tasks failed")
                return "FAILED"
                
            # All completed successfully - execute analyze phase
            new_workflow = execute_analyze_phase(
                run_handle, campaign, tasks, task_status_map, store
            )
            
            if new_workflow:
                logger.info(f"Campaign generated new workflow with {len(new_workflow.tasks)} tasks.")
                store.add_workflow(new_workflow, run_handle.run_id)
                return "RUNNING"
            else:
                logger.info("Campaign has no further work. Run Completed.")
                store.set_run_status(run_handle.run_id, "COMPLETED")
                return "COMPLETED"

        return "RUNNING"


__all__ = [
    "step_run",
]
