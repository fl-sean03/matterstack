"""
EXECUTE phase logic for the run lifecycle.

This module contains functions for operator dispatch, concurrency control,
and task submission.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.core.lifecycle import (
    AttemptContext,
    AttemptLifecycleHook,
    fire_hook_safely,
)
from matterstack.core.operator_keys import (
    is_canonical_operator_key,
    legacy_operator_type_to_key,
    normalize_operator_key,
)
from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task

logger = logging.getLogger(__name__)


def resolve_operator_key_for_dispatch(operator_type: Optional[str]) -> Optional[str]:
    """
    Convert a dispatch routing string into a canonical operator_key.

    - If already canonical (e.g. "hpc.default"), normalize it.
    - Else treat as legacy operator_type (e.g. "Human", "HPC") and map to "*.default".

    Args:
        operator_type: The operator type string from task or config.

    Returns:
        The canonical operator key, or None if not resolvable.
    """
    if not operator_type:
        return None

    lowered = str(operator_type).strip().lower()
    if is_canonical_operator_key(lowered):
        try:
            return normalize_operator_key(lowered)
        except Exception:
            return None

    return legacy_operator_type_to_key(operator_type)


def calculate_concurrency_slots(
    run_handle: RunHandle,
    store: Any,
    max_hpc_jobs: int = 10,
) -> Tuple[int, int]:
    """
    Calculate available concurrency slots.

    Counts active executions (SUBMITTED/RUNNING/WAITING_EXTERNAL) from both
    attempts (v2) and legacy external runs (v1), then returns available slots.

    Args:
        run_handle: The run handle.
        store: The SQLiteStateStore instance.
        max_hpc_jobs: Maximum concurrent jobs allowed.

    Returns:
        Tuple of (active_count, slots_available).
    """
    active_external_count = 0

    attempt_task_ids = store.get_attempt_task_ids(run_handle.run_id)
    active_attempts_for_slots = store.get_active_attempts(run_handle.run_id)

    for a in active_attempts_for_slots:
        if a.status in [
            ExternalRunStatus.SUBMITTED.value,
            ExternalRunStatus.RUNNING.value,
            ExternalRunStatus.WAITING_EXTERNAL.value,
        ]:
            active_external_count += 1

    # Legacy external_runs count ONLY for tasks that have no attempts
    active_external = store.get_active_external_runs(run_handle.run_id)
    for ext in active_external:
        if ext.task_id in attempt_task_ids:
            continue
        if ext.status in [
            ExternalRunStatus.RUNNING,
            ExternalRunStatus.WAITING_EXTERNAL,
            ExternalRunStatus.SUBMITTED,
        ]:
            active_external_count += 1

    slots_available = max(0, max_hpc_jobs - active_external_count)

    return active_external_count, slots_available


def get_max_hpc_jobs(run_handle: RunHandle) -> int:
    """
    Read max_hpc_jobs_per_run from config.json if available.

    Args:
        run_handle: The run handle.

    Returns:
        The configured max jobs, or 10 as default.
    """
    max_hpc_jobs = 10
    config_path = run_handle.root_path / "config.json"

    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text())
            max_hpc_jobs = cfg.get("max_hpc_jobs_per_run", 10)
        except Exception as e:
            logger.warning(f"Failed to read config.json, using default limit: {e}")

    return max_hpc_jobs


def get_execution_mode(run_handle: RunHandle) -> str:
    """
    Read execution_mode from config.json if available.

    Args:
        run_handle: The run handle.

    Returns:
        The configured execution mode, or "Simulation" as default.
    """
    config_path = run_handle.root_path / "config.json"

    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text())
            return cfg.get("execution_mode", "Simulation")
        except Exception:
            pass

    return "Simulation"


def determine_operator_type(
    task: Task,
    run_handle: RunHandle,
) -> Optional[str]:
    """
    Determine the effective operator type for a task.

    Priority: Task operator_key > Task Env > Task Type > Config Default

    Args:
        task: The task to check.
        run_handle: The run handle.

    Returns:
        The operator type string, or None for local simulation.
    """
    # Priority 1: First-class operator_key on Task (v0.2.6+)
    if hasattr(task, "operator_key") and task.operator_key:
        return task.operator_key

    # Priority 2: Environment override (legacy)
    explicit_operator = task.env.get("MATTERSTACK_OPERATOR")

    if explicit_operator:
        return explicit_operator
    elif isinstance(task, GateTask):
        return "Human"  # GateTask maps to HumanOperator
    elif isinstance(task, ExternalTask):
        return None
    else:
        default_mode = get_execution_mode(run_handle)
        if default_mode == "HPC":
            return "HPC"
        elif default_mode == "Local":
            return "Local"

    return None


def submit_task_to_operator(
    task: Task,
    operator_type: str,
    run_handle: RunHandle,
    store: Any,
    operators: Dict[str, Any],
    lifecycle_hooks: Optional[AttemptLifecycleHook] = None,
) -> bool:
    """
    Submit a task to an operator.

    Creates an attempt, prepares, and submits the task.

    Args:
        task: The task to submit.
        operator_type: The operator type string.
        run_handle: The run handle.
        store: The SQLiteStateStore instance.
        operators: The operator registry dict.
        lifecycle_hooks: Optional lifecycle hooks to fire during attempt processing.

    Returns:
        True if successful, False if operator not found or error.
    """
    canonical_operator_key = resolve_operator_key_for_dispatch(operator_type)

    # Backward-compatible dispatch:
    # - Prefer canonical operator_key ("hpc.default") when registry is canonical
    # - Fall back to legacy registry keys ("HPC", "Human", ...)
    dispatch_candidates: List[str] = []
    if canonical_operator_key:
        dispatch_candidates.append(canonical_operator_key)
    if operator_type:
        dispatch_candidates.append(str(operator_type).strip())

    op = None
    dispatch_key_used: Optional[str] = None
    for k in dispatch_candidates:
        if k and k in operators:
            op = operators[k]
            dispatch_key_used = k
            break

    if op is None:
        logger.error(
            f"Unknown operator requested: {operator_type} "
            f"(resolved operator_key={canonical_operator_key!r}). "
            f"Registry keys={sorted(list(operators.keys()))[:10]}{'...' if len(operators) > 10 else ''}"
        )
        store.update_task_status(task.task_id, "FAILED")
        return False

    logger.info(
        f"Dispatching to Operator: {dispatch_key_used} (requested: {operator_type}, resolved operator_key={canonical_operator_key!r})"
    )

    attempt_id: Optional[str] = None
    attempt_context: Optional[AttemptContext] = None
    try:
        attempt_id = store.create_attempt(
            run_id=run_handle.run_id,
            task_id=task.task_id,
            operator_type=operator_type,
            operator_key=canonical_operator_key,
            status=ExternalRunStatus.CREATED.value,
            operator_data={},
            relative_path=None,
        )

        # Build attempt context for lifecycle hooks
        # Get attempt_index from store (count of attempts for this task)
        attempt_index = store.get_attempt_count(run_handle.run_id, task.task_id)
        attempt_context = AttemptContext(
            run_id=run_handle.run_id,
            task_id=task.task_id,
            attempt_id=attempt_id,
            operator_key=canonical_operator_key,
            attempt_index=attempt_index,
        )

        # Fire on_create lifecycle hook
        fire_hook_safely(lifecycle_hooks, "on_create", attempt_context)

        # 1. Prepare (operator directory, manifests, etc.)
        ext_handle = op.prepare_run(run_handle, task)
        store.update_attempt(
            attempt_id,
            status=ext_handle.status.value,
            operator_type=ext_handle.operator_type,
            operator_data=ext_handle.operator_data,
            relative_path=ext_handle.relative_path,
        )

        # 2. Submit
        ext_handle = op.submit(ext_handle)
        store.update_attempt(
            attempt_id,
            status=ext_handle.status.value,
            operator_type=ext_handle.operator_type,
            external_id=ext_handle.external_id,
            operator_data=ext_handle.operator_data,
            relative_path=ext_handle.relative_path,
        )

        # Fire on_submit lifecycle hook
        fire_hook_safely(lifecycle_hooks, "on_submit", attempt_context, ext_handle.external_id)

        # Update Task Status (SUBMITTED -> WAITING_EXTERNAL)
        if ext_handle.status == ExternalRunStatus.SUBMITTED:
            store.update_task_status(task.task_id, "WAITING_EXTERNAL")
        else:
            store.update_task_status(
                task.task_id,
                "WAITING_EXTERNAL"
                if ext_handle.status == ExternalRunStatus.WAITING_EXTERNAL
                else ext_handle.status.value,
            )
        return True

    except Exception as e:
        logger.error(
            f"Failed to dispatch operator {dispatch_key_used} (requested {operator_type}, resolved operator_key={canonical_operator_key!r}): {e}"
        )
        import traceback

        traceback.print_exc()

        if attempt_id is not None:
            try:
                store.update_attempt(
                    attempt_id,
                    status=ExternalRunStatus.FAILED_INIT.value,
                    operator_data={"error": str(e)},
                    status_reason=str(e),
                )
            except Exception:
                pass

            # Fire on_fail lifecycle hook
            if attempt_context is not None:
                fire_hook_safely(lifecycle_hooks, "on_fail", attempt_context, str(e))

        store.update_task_status(task.task_id, "FAILED")
        return False


def submit_external_task_stub(
    task: Task,
    run_handle: RunHandle,
    store: Any,
) -> None:
    """
    Create a stub attempt for ExternalTask/GateTask without operator execution.

    This is for legacy "external coordination" tasks where we record a
    WAITING_EXTERNAL attempt for provenance/idempotency.

    Args:
        task: The external or gate task.
        run_handle: The run handle.
        store: The SQLiteStateStore instance.
    """
    store.create_attempt(
        run_id=run_handle.run_id,
        task_id=task.task_id,
        operator_type="stub",
        status=ExternalRunStatus.WAITING_EXTERNAL.value,
        operator_data={},
        relative_path=None,
    )
    store.update_task_status(task.task_id, "WAITING_EXTERNAL")


def submit_local_simulation(
    task: Task,
    store: Any,
) -> None:
    """
    Handle local compute task in simulation mode.

    No attempt record is created; task is marked COMPLETED immediately.

    Args:
        task: The task to simulate.
        store: The SQLiteStateStore instance.
    """
    logger.info(f"Simulating Local execution for {task.task_id}")
    store.update_task_status(task.task_id, "COMPLETED")


__all__ = [
    "resolve_operator_key_for_dispatch",
    "calculate_concurrency_slots",
    "get_max_hpc_jobs",
    "get_execution_mode",
    "determine_operator_type",
    "submit_task_to_operator",
    "submit_external_task_stub",
    "submit_local_simulation",
]
