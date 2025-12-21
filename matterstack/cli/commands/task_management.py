"""
Task-level CLI commands.

Contains commands for managing individual tasks:
- cmd_rerun: Rerun a task by resetting it to PENDING
- cmd_attempts: List attempt history for a task
- cmd_cancel_attempt: Cancel an attempt
"""
import sys
import logging

from matterstack.storage.state_store import SQLiteStateStore
from matterstack.cli.utils import find_run
from matterstack.cli.reset import get_dependents

logger = logging.getLogger("cli.task_management")


def _confirm_or_exit(force: bool, prompt: str) -> None:
    """Helper to prompt for confirmation or exit if not confirmed."""
    if force:
        return
    confirm = input(f"{prompt} [y/N] ")
    if confirm.lower() != "y":
        print("Aborted.")
        sys.exit(0)


def cmd_rerun(args):
    """
    Mark a task as PENDING so that the next scheduler tick creates a new attempt.
    Optionally recurse to dependent tasks.
    """
    run_id = args.run_id
    task_id = args.task_id
    recursive = args.recursive
    force = args.force

    handle = find_run(run_id)
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)

        # Validate task exists within this run
        tasks = store.get_tasks(run_id)
        task_ids = {t.task_id for t in tasks}
        if task_id not in task_ids:
            logger.error(f"Task {task_id} not found in run {run_id}.")
            sys.exit(1)

        targets = {task_id}
        if recursive:
            deps = get_dependents(store, run_id, task_id)
            targets.update(deps)

        # Confirmation prompt
        if not force:
            print(f"You are about to RERUN (reset to PENDING) the following tasks in run {run_id}:")
            for t in sorted(targets):
                print(f"  - {t}")
            _confirm_or_exit(False, "\nProceed?")

        terminal_attempt_states = {"COMPLETED", "FAILED", "CANCELLED"}

        with store.lock():
            for tid in sorted(targets):
                # If current attempt exists and is active, require --force
                attempt = store.get_current_attempt(tid)
                if attempt is not None and attempt.status not in terminal_attempt_states:
                    if not force:
                        logger.error(
                            f"Task {tid} has an active attempt {attempt.attempt_id} in status {attempt.status}. "
                            f"Use --force to cancel and rerun."
                        )
                        sys.exit(1)

                    # Forced: mark attempt CANCELLED (backend cancellation is best-effort / not available here)
                    store.update_attempt(
                        attempt.attempt_id,
                        status="CANCELLED",
                        status_reason="User forced rerun via CLI (backend cancellation skipped)",
                    )

                # Also cancel any legacy external run rows to prevent zombies
                store.cancel_external_runs(tid)

                # Reset task to pending so next tick submits a NEW attempt
                store.update_task_status(tid, "PENDING")

        print(f"Rerun queued for {len(targets)} task(s). Next step/loop will create new attempt(s).")

    except Exception as e:
        logger.error(f"Failed to rerun task(s): {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def cmd_attempts(args):
    """
    List attempt history for a task (TSV).
    """
    run_id = args.run_id
    task_id = args.task_id

    handle = find_run(run_id)
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)

        tasks = store.get_tasks(run_id)
        task_ids = {t.task_id for t in tasks}
        if task_id not in task_ids:
            logger.error(f"Task {task_id} not found in run {run_id}.")
            sys.exit(1)

        attempts = store.list_attempts(task_id)

        # TSV header (stable, parseable)
        #
        # Backward-compat: keep the original v0.2.5 7-column format exactly.
        header = [
            "attempt_id",
            "attempt_index",
            "status",
            "operator_type",
            "external_id",
            "artifact_path",
            "config_hash",
        ]
        print("\t".join(header))

        for a in attempts:
            config_hash = ""
            try:
                if a.operator_data and isinstance(a.operator_data, dict):
                    config_hash = str(a.operator_data.get("config_hash") or "")
            except Exception:
                config_hash = ""

            row = [
                a.attempt_id or "",
                str(a.attempt_index or ""),
                a.status or "",
                a.operator_type or "",
                a.external_id or "",
                a.relative_path or "",
                config_hash,
            ]
            print("\t".join(row))

    except Exception as e:
        logger.error(f"Failed to list attempts: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def cmd_cancel_attempt(args):
    """
    Cancel an attempt (best-effort; local-only DB cancellation if backend job cancellation is unavailable).
    """
    run_id = args.run_id
    attempt_id = args.attempt_id
    force = args.force

    handle = find_run(run_id)
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)

        attempt = store.get_attempt(attempt_id)
        if attempt is None:
            logger.error(f"Attempt {attempt_id} not found.")
            sys.exit(1)

        if attempt.run_id != run_id:
            logger.error(f"Attempt {attempt_id} belongs to run {attempt.run_id}, not {run_id}.")
            sys.exit(1)

        if not force:
            print(f"You are about to CANCEL attempt {attempt_id} (task {attempt.task_id}) in run {run_id}.")
            _confirm_or_exit(False, "Proceed?")

        with store.lock():
            # Mark the attempt cancelled in DB (backend cancellation is skipped here)
            store.update_attempt(
                attempt_id,
                status="CANCELLED",
                status_reason="User cancelled attempt via CLI (backend cancellation skipped)",
            )
            # Heal task status to reflect current attempt state
            store.update_task_status(attempt.task_id, "CANCELLED")
            # Legacy external run safety
            store.cancel_external_runs(attempt.task_id)

        logger.info("Backend cancellation skipped (no job_id/backend available in local-only CLI path).")
        print(f"Attempt {attempt_id} cancelled.")

    except Exception as e:
        logger.error(f"Failed to cancel attempt: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
