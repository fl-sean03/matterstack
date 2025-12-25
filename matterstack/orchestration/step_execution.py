"""
Main step execution coordinator.

This module contains the step_run function that orchestrates one tick
of the run lifecycle, coordinating POLL, PLAN, EXECUTE, and ANALYZE phases.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from matterstack.config.operators import OperatorsConfig
from matterstack.core.campaign import Campaign
from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.core.lifecycle import AttemptLifecycleHook
from matterstack.core.run import RunHandle
from matterstack.orchestration.analyze import execute_analyze_phase
from matterstack.orchestration.dispatch import (
    calculate_concurrency_slots,
    determine_operator_type,
    get_max_hpc_jobs,
    resolve_operator_key_for_dispatch,
    submit_external_task_stub,
    submit_local_simulation,
    submit_task_to_operator,
)
from matterstack.orchestration.polling import (
    poll_active_attempts,
    poll_legacy_external_runs,
)
from matterstack.storage.state_store import SQLiteStateStore

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
    from matterstack.runtime.backends.local import LocalBackend
    from matterstack.runtime.operators.experiment import ExperimentOperator
    from matterstack.runtime.operators.hpc import ComputeOperator
    from matterstack.runtime.operators.human import HumanOperator

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


# Default global concurrency limit when no config is provided
DEFAULT_MAX_CONCURRENT_GLOBAL = 50


def step_run(
    run_handle: RunHandle,
    campaign: Campaign,
    operator_registry: Optional[Dict[str, Any]] = None,
    operators_config: Optional[OperatorsConfig] = None,
    lifecycle_hooks: Optional[AttemptLifecycleHook] = None,
) -> str:
    """
    Execute one 'tick' of the run lifecycle.

    This function orchestrates:
    1. POLL Phase: Check status of active attempts and external runs
    2. PLAN Phase: Check dependencies and find ready tasks
    3. EXECUTE Phase: Submit ready tasks to operators (with per-operator concurrency limits)
    4. ANALYZE Phase: If workflow complete, analyze and replan

    Args:
        run_handle: The handle to the run.
        campaign: The campaign instance.
        operator_registry: Optional operator registry. If None, a default is built.
        operators_config: Optional operators.yaml config with per-operator limits.
        lifecycle_hooks: Optional lifecycle hooks to fire during attempt processing.

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
        poll_active_attempts(run_handle.run_id, store, operators, lifecycle_hooks)

        # Poll legacy external runs (v1 fallback) ONLY for tasks that have no attempts
        poll_legacy_external_runs(run_handle.run_id, store, operators, attempt_task_ids)

        # 2. PLAN Phase: Check dependencies and find ready tasks
        tasks = store.get_tasks(run_handle.run_id)

        # Map task_id -> status
        task_status_map = {t.task_id: store.get_task_status(t.task_id) for t in tasks}

        # Calculate stats for logging
        stats = {"total": len(tasks), "completed": 0, "failed": 0, "active": 0, "ready": 0, "submitted": 0}

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

        # Build per-operator limits and global limit from config
        operator_limits: Dict[str, Optional[int]] = {}
        global_limit: int = DEFAULT_MAX_CONCURRENT_GLOBAL

        if operators_config:
            # Use per-operator limits from operators.yaml
            global_limit = operators_config.defaults.max_concurrent_global or DEFAULT_MAX_CONCURRENT_GLOBAL
            for op_key, op_cfg in operators_config.operators.items():
                operator_limits[op_key] = op_cfg.max_concurrent
            logger.info(f"Using per-operator limits from operators.yaml (global={global_limit})")
        else:
            # Legacy: use global max_hpc_jobs from config.json
            global_limit = get_max_hpc_jobs(run_handle)
            logger.info(f"Using legacy global limit from config.json: {global_limit}")

        config_path = run_handle.root_path / "config.json"
        logger.debug(f"Checking for config at: {config_path}")

        # Count active executions per operator for per-operator concurrency
        active_by_operator = store.count_active_attempts_by_operator(run_handle.run_id)

        # Also get global count for legacy logging
        active_external_count, _ = calculate_concurrency_slots(run_handle, store, global_limit)
        logger.info(f"Concurrency Check: Total Active={active_external_count}, Global Limit={global_limit}")

        # Submit ready tasks (respecting per-operator limits)
        for task in tasks_to_run:
            operator_type = determine_operator_type(task, run_handle)

            # Apply concurrency limit if it's an external run (Operator)
            is_external = operator_type is not None or isinstance(task, (ExternalTask, GateTask))

            if is_external:
                # Resolve to canonical operator key
                canonical_key = resolve_operator_key_for_dispatch(operator_type) or ""

                # Determine limit for this operator
                # None means "inherit from global", an explicit integer is used as-is
                if canonical_key in operator_limits and operator_limits[canonical_key] is not None:
                    limit = operator_limits[canonical_key]
                else:
                    limit = global_limit  # Fallback to global

                # Check if this operator has available slots
                active = active_by_operator.get(canonical_key, 0)

                if active >= limit:
                    logger.info(
                        f"Concurrency limit reached for {canonical_key or 'unknown'} "
                        f"({active}/{limit}). Postponing task {task.task_id}"
                    )
                    continue

                # Track that we're using a slot for this operator
                active_by_operator[canonical_key] = active + 1

            logger.info(f"Submitting task {task.task_id}")

            if operator_type:
                # v2: Create attempt first, then dispatch to operator
                success = submit_task_to_operator(
                    task,
                    operator_type,
                    run_handle,
                    store,
                    operators,
                    lifecycle_hooks=lifecycle_hooks,
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
            new_workflow = execute_analyze_phase(run_handle, campaign, tasks, task_status_map, store)

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
