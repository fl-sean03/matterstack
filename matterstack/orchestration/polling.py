"""
POLL phase logic for the run lifecycle.

This module contains functions for polling active attempts and external runs,
mapping status values, and looking up operators.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from matterstack.core.lifecycle import (
    AttemptContext,
    AttemptLifecycleHook,
    fire_hook_safely,
)
from matterstack.core.operator_keys import (
    legacy_operator_type_to_key,
    resolve_operator_key_for_attempt,
)
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus

logger = logging.getLogger(__name__)


def task_status_from_external_status(s: ExternalRunStatus) -> str:
    """
    Map ExternalRunStatus to task status string.

    Status mapping (attempt/external status -> task status):
    - SUBMITTED maps to WAITING_EXTERNAL for user-facing stability in v0.2.5.
    - CREATED maps to PENDING.
    - RUNNING, COMPLETED, FAILED, FAILED_INIT, CANCELLED map directly.

    Args:
        s: The external run status.

    Returns:
        The corresponding task status string.
    """
    if s == ExternalRunStatus.CREATED:
        return "PENDING"
    if s == ExternalRunStatus.SUBMITTED:
        return "WAITING_EXTERNAL"
    if s == ExternalRunStatus.RUNNING:
        return "RUNNING"
    if s == ExternalRunStatus.WAITING_EXTERNAL:
        return "WAITING_EXTERNAL"
    if s == ExternalRunStatus.COMPLETED:
        return "COMPLETED"
    if s == ExternalRunStatus.FAILED:
        return "FAILED"
    if s == ExternalRunStatus.FAILED_INIT:
        return "FAILED"
    if s == ExternalRunStatus.CANCELLED:
        return "CANCELLED"
    return "UNKNOWN"


def lookup_operator_for_attempt(attempt: Any, operators: Dict[str, Any]) -> Optional[Any]:
    """
    Backward-compatible operator lookup for an attempt.

    Prefer canonical operator_key (v0.2.6+) when present, but fall back to
    legacy operator_type keys ("HPC", "Human", etc.) for older registries/tests.

    Args:
        attempt: The attempt object with operator_type and optional operator_key.
        operators: The operator registry dict.

    Returns:
        The operator instance if found, None otherwise.
    """
    candidates: List[str] = []

    resolved = resolve_operator_key_for_attempt(attempt)
    if resolved is not None and resolved.operator_key:
        candidates.append(resolved.operator_key)

    if getattr(attempt, "operator_type", None):
        raw_type = str(attempt.operator_type).strip()
        if raw_type:
            candidates.append(raw_type)

        derived = legacy_operator_type_to_key(raw_type)
        if derived:
            candidates.append(derived)

        lowered = raw_type.lower()
        if lowered and lowered != raw_type:
            candidates.append(lowered)

    for key in candidates:
        if key in operators:
            return operators[key]

    return None


def poll_active_attempts(
    run_id: str,
    store: Any,
    operators: Dict[str, Any],
    lifecycle_hooks: Optional[AttemptLifecycleHook] = None,
    stuck_timeout_seconds: int = 3600,
) -> None:
    """
    Poll active attempts and update their status.

    This is the v2 primary path for attempt-aware polling.
    Also detects stuck attempts (CREATED with no external_id past timeout).

    Args:
        run_id: The run ID.
        store: The SQLiteStateStore instance.
        operators: The operator registry dict.
        lifecycle_hooks: Optional lifecycle hooks to fire on terminal state transitions.
        stuck_timeout_seconds: Timeout in seconds to detect stuck attempts (default 1 hour).
    """
    from datetime import datetime, timedelta

    active_attempts = store.get_active_attempts(run_id)
    cutoff = datetime.utcnow() - timedelta(seconds=stuck_timeout_seconds)

    for attempt in active_attempts:
        # Detect stuck attempts: CREATED with no external_id past timeout
        if (
            attempt.status == ExternalRunStatus.CREATED.value
            and attempt.external_id is None
            and attempt.created_at is not None
            and attempt.created_at < cutoff
        ):
            logger.warning(
                f"Attempt {attempt.attempt_id} stuck in CREATED state for "
                f"> {stuck_timeout_seconds}s with no external_id, marking FAILED_INIT"
            )
            store.update_attempt(
                attempt.attempt_id,
                status=ExternalRunStatus.FAILED_INIT.value,
                status_reason=f"Stuck in CREATED state; no external_id after {stuck_timeout_seconds}s",
            )
            store.update_task_status(attempt.task_id, "FAILED")

            # Fire on_fail lifecycle hook
            if lifecycle_hooks:
                context = AttemptContext(
                    run_id=run_id,
                    task_id=attempt.task_id,
                    attempt_id=attempt.attempt_id,
                    operator_key=getattr(attempt, "operator_key", None),
                    attempt_index=getattr(attempt, "attempt_index", 1),
                )
                fire_hook_safely(
                    lifecycle_hooks,
                    "on_fail",
                    context,
                    "Stuck in CREATED state; marked FAILED_INIT",
                )
            continue

        if not attempt.operator_type and not getattr(attempt, "operator_key", None):
            # "stub" / incomplete attempts won't be polled; we still heal task status below.
            store.update_task_status(
                attempt.task_id,
                task_status_from_external_status(ExternalRunStatus(attempt.status)),
            )
            continue

        op = lookup_operator_for_attempt(attempt, operators)
        if op is None:
            # Unknown operator wiring for this attempt: skip polling but still heal task status.
            store.update_task_status(
                attempt.task_id,
                task_status_from_external_status(ExternalRunStatus(attempt.status)),
            )
            continue

        try:
            ext_handle = ExternalRunHandle(
                task_id=attempt.task_id,
                operator_type=attempt.operator_type,
                external_id=attempt.external_id,
                status=ExternalRunStatus(attempt.status),
                operator_data=attempt.operator_data or {},
                relative_path=Path(attempt.relative_path) if attempt.relative_path else None,
            )

            old_status = ext_handle.status
            updated_handle = op.check_status(ext_handle)

            if updated_handle.status != old_status:
                logger.info(
                    f"Attempt {attempt.attempt_id} (task {attempt.task_id}) transitioned to {updated_handle.status}"
                )

            # If completed or failed, try to collect results (logs are important on failure)
            if updated_handle.status in [ExternalRunStatus.COMPLETED, ExternalRunStatus.FAILED]:
                try:
                    result = op.collect_results(updated_handle)
                    if result.files:
                        files_dict = {k: str(v) for k, v in result.files.items()}
                        updated_handle.operator_data["output_files"] = files_dict
                    if result.data:
                        updated_handle.operator_data["output_data"] = result.data
                except Exception as e:
                    logger.error(
                        f"Failed to collect results for attempt {attempt.attempt_id} (task {attempt.task_id}): {e}"
                    )

            # Persist attempt state (always, for "healing" + operator_data updates)
            store.update_attempt(
                attempt.attempt_id,
                status=updated_handle.status.value,
                operator_type=updated_handle.operator_type,
                external_id=updated_handle.external_id,
                operator_data=updated_handle.operator_data,
                relative_path=updated_handle.relative_path,
            )

            # Fire lifecycle hooks on terminal state transitions
            if old_status != updated_handle.status:
                if updated_handle.status in [ExternalRunStatus.COMPLETED, ExternalRunStatus.FAILED]:
                    # Build context for lifecycle hooks
                    context = AttemptContext(
                        run_id=run_id,
                        task_id=attempt.task_id,
                        attempt_id=attempt.attempt_id,
                        operator_key=getattr(attempt, "operator_key", None),
                        attempt_index=getattr(attempt, "attempt_index", 1),
                    )

                    if updated_handle.status == ExternalRunStatus.COMPLETED:
                        fire_hook_safely(lifecycle_hooks, "on_complete", context, True)
                    elif updated_handle.status == ExternalRunStatus.FAILED:
                        error = updated_handle.operator_data.get("error", "Unknown error")
                        if not error and hasattr(attempt, "status_reason") and attempt.status_reason:
                            error = attempt.status_reason
                        fire_hook_safely(lifecycle_hooks, "on_fail", context, str(error))

            # Heal/sync task status from attempt status (even if unchanged)
            store.update_task_status(attempt.task_id, task_status_from_external_status(updated_handle.status))

        except Exception as e:
            logger.error(f"Error checking status for attempt {attempt.attempt_id} (task {attempt.task_id}): {e}")


def poll_legacy_external_runs(
    run_id: str,
    store: Any,
    operators: Dict[str, Any],
    attempt_task_ids: set,
) -> None:
    """
    Poll legacy external runs (v1 fallback) for tasks that have no attempts.

    Args:
        run_id: The run ID.
        store: The SQLiteStateStore instance.
        operators: The operator registry dict.
        attempt_task_ids: Set of task IDs that already have attempts.
    """
    active_external = store.get_active_external_runs(run_id)

    for ext_handle in active_external:
        if ext_handle.task_id in attempt_task_ids:
            continue

        op_type = ext_handle.operator_type
        if op_type in operators:
            op = operators[op_type]
            try:
                old_status = ext_handle.status
                updated_handle = op.check_status(ext_handle)

                if updated_handle.status != old_status:
                    logger.info(f"Legacy External Run {ext_handle.task_id} transitioned to {updated_handle.status}")

                if updated_handle.status in [ExternalRunStatus.COMPLETED, ExternalRunStatus.FAILED]:
                    try:
                        result = op.collect_results(updated_handle)
                        if result.files:
                            files_dict = {k: str(v) for k, v in result.files.items()}
                            updated_handle.operator_data["output_files"] = files_dict
                        if result.data:
                            updated_handle.operator_data["output_data"] = result.data
                    except Exception as e:
                        logger.error(f"Failed to collect results for legacy external run {ext_handle.task_id}: {e}")

                store.update_external_run(updated_handle)

                # Heal/sync task status from legacy run status (SUBMITTED -> WAITING_EXTERNAL)
                store.update_task_status(
                    ext_handle.task_id,
                    task_status_from_external_status(updated_handle.status),
                )

            except Exception as e:
                logger.error(f"Error checking status for {ext_handle.task_id}: {e}")


__all__ = [
    "task_status_from_external_status",
    "lookup_operator_for_attempt",
    "poll_active_attempts",
    "poll_legacy_external_runs",
]
