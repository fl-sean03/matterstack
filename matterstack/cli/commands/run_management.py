"""
Run lifecycle CLI commands.

Contains commands for managing run lifecycle:
- cmd_init: Initialize a new run
- cmd_step: Execute one step
- cmd_loop: Loop until completion or act as scheduler
- cmd_cancel: Cancel a run
- cmd_pause: Pause a run
- cmd_resume: Resume a run
- cmd_revive: Revive terminal run
"""

import logging
import random
import sys
import time
from pathlib import Path

from matterstack.cli.operator_registry import RegistryConfig, build_operator_registry
from matterstack.cli.utils import find_run, load_workspace_context
from matterstack.config.operator_wiring import resolve_operator_wiring
from matterstack.orchestration.run_lifecycle import (
    initialize_run,
    list_active_runs,
    run_until_completion,
    step_run,
)
from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger("cli.run_management")


def cmd_init(args):
    """
    Initialize a new run.

    Optional: if --operators-config is provided, persist the operator wiring snapshot immediately
    so the run is ready to resume without re-specifying wiring flags.
    """
    workspace_slug = args.workspace
    try:
        operators_config = getattr(args, "operators_config", None)

        # Deterministic safety: explicit path must exist (avoid creating a run and then failing).
        if operators_config:
            p = Path(operators_config)
            if not p.is_file():
                raise FileNotFoundError(f"CLI --operators-config file not found: {p}")

        campaign = load_workspace_context(workspace_slug)
        handle = initialize_run(workspace_slug, campaign)

        if operators_config:
            resolve_operator_wiring(
                handle,
                cli_operators_config_path=str(operators_config),
                force_override=False,
            )

        print(f"Run initialized: {handle.run_id}")
        print(f"Path: {handle.root_path}")
    except Exception as e:
        logger.error(f"Failed to initialize run: {e}")
        sys.exit(1)


def cmd_step(args):
    """
    Execute one step of the run.
    """
    run_id = args.run_id
    handle = find_run(run_id)

    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        campaign = load_workspace_context(handle.workspace_slug)

        wiring = resolve_operator_wiring(
            handle,
            cli_operators_config_path=getattr(args, "operators_config", None),
            force_override=bool(getattr(args, "force_wiring_override", False)),
            legacy_hpc_config_path=getattr(args, "hpc_config", None),
            legacy_profile=getattr(args, "profile", None),
            profiles_config_path=getattr(args, "config", None),
        )

        registry_cfg = RegistryConfig(
            config_path=getattr(args, "config", None),
            operators_config_path=wiring.snapshot_path,
            # When a snapshot exists (including legacy-generated), we must not pass legacy flags.
            profile=None if wiring.snapshot_path else getattr(args, "profile", None),
            hpc_config_path=None if wiring.snapshot_path else getattr(args, "hpc_config", None),
        )

        operator_registry = build_operator_registry(handle, registry_config=registry_cfg)

        status = step_run(handle, campaign, operator_registry=operator_registry)
        print(f"Run {run_id} step complete. Status: {status}")
    except Exception as e:
        logger.error(f"Failed to step run: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def cmd_loop(args):
    """
    Loop step until run is completed or failed.
    """
    run_id = args.run_id

    # --- Single Run Mode ---
    if run_id:
        handle = find_run(run_id)

        if not handle:
            logger.error(f"Run {run_id} not found.")
            sys.exit(1)

        try:
            campaign = load_workspace_context(handle.workspace_slug)

            wiring = resolve_operator_wiring(
                handle,
                cli_operators_config_path=getattr(args, "operators_config", None),
                force_override=bool(getattr(args, "force_wiring_override", False)),
                legacy_hpc_config_path=getattr(args, "hpc_config", None),
                legacy_profile=getattr(args, "profile", None),
                profiles_config_path=getattr(args, "config", None),
            )

            registry_cfg = RegistryConfig(
                config_path=getattr(args, "config", None),
                operators_config_path=wiring.snapshot_path,
                profile=None if wiring.snapshot_path else getattr(args, "profile", None),
                hpc_config_path=None if wiring.snapshot_path else getattr(args, "hpc_config", None),
            )

            operator_registry = build_operator_registry(handle, registry_config=registry_cfg)

            run_until_completion(handle, campaign, operator_registry=operator_registry)
        except Exception as e:
            logger.error(f"Loop failed: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    # --- Multi-Run Scheduler Mode ---
    else:
        logger.info("Starting Multi-Run Scheduler Loop...")

        try:
            while True:
                active_runs = list_active_runs()

                if not active_runs:
                    logger.debug("No active runs found.")
                    time.sleep(5)
                    continue

                # Randomized Round-Robin to prevent starvation/convoys
                random.shuffle(active_runs)

                runs_processed = 0

                for handle in active_runs:
                    try:
                        campaign = load_workspace_context(handle.workspace_slug)

                        # Scheduler mode: do NOT apply a global operators-config override across multiple runs.
                        wiring = resolve_operator_wiring(
                            handle,
                            cli_operators_config_path=None,
                            force_override=False,
                            legacy_hpc_config_path=getattr(args, "hpc_config", None),
                            legacy_profile=getattr(args, "profile", None),
                            profiles_config_path=getattr(args, "config", None),
                        )

                        registry_cfg = RegistryConfig(
                            config_path=getattr(args, "config", None),
                            operators_config_path=wiring.snapshot_path,
                            profile=None if wiring.snapshot_path else getattr(args, "profile", None),
                            hpc_config_path=None if wiring.snapshot_path else getattr(args, "hpc_config", None),
                        )

                        operator_registry = build_operator_registry(handle, registry_config=registry_cfg)

                        # We use step_run which attempts to lock.
                        status = step_run(handle, campaign, operator_registry=operator_registry)

                        logger.info(f"Stepped run {handle.run_id} -> {status}")
                        runs_processed += 1

                    except RuntimeError as re:
                        if "Could not acquire lock" in str(re):
                            logger.debug(f"Run {handle.run_id} is locked. Skipping.")
                            continue
                        else:
                            logger.error(f"Runtime error in run {handle.run_id}: {re}")
                    except Exception as e:
                        logger.error(f"Error stepping run {handle.run_id}: {e}")
                        import traceback

                        traceback.print_exc()

                # Adjust sleep based on load?
                # For now, constant sleep
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
            sys.exit(0)
        except Exception as e:
            logger.critical(f"Scheduler crashed: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)


def cmd_cancel(args):
    """
    Cancel a run.
    """
    run_id = args.run_id
    handle = find_run(run_id)

    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)
        store.set_run_status(run_id, "CANCELLED", reason="User cancelled via CLI")
        print(f"Run {run_id} cancelled.")
    except Exception as e:
        logger.error(f"Failed to cancel run: {e}")
        sys.exit(1)


def cmd_pause(args):
    """
    Pause a run.
    """
    run_id = args.run_id
    handle = find_run(run_id)

    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)
        store.set_run_status(run_id, "PAUSED", reason="User paused via CLI")
        print(f"Run {run_id} paused.")
    except Exception as e:
        logger.error(f"Failed to pause run: {e}")
        sys.exit(1)


def cmd_resume(args):
    """
    Resume a paused run.
    """
    run_id = args.run_id
    handle = find_run(run_id)

    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)
        current_status = store.get_run_status(run_id)
        if current_status not in ["PAUSED", "PENDING"]:
            logger.warning(f"Run {run_id} is {current_status}, not PAUSED. Resuming anyway.")

        store.set_run_status(run_id, "RUNNING", reason="User resumed via CLI")
        print(f"Run {run_id} resumed.")
    except Exception as e:
        logger.error(f"Failed to resume run: {e}")
        sys.exit(1)


def cmd_revive(args):
    """
    Revive a terminal run (FAILED/COMPLETED/CANCELLED) back to PENDING.
    """
    run_id = args.run_id
    handle = find_run(run_id)

    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)

    try:
        store = SQLiteStateStore(handle.db_path)
        current = store.get_run_status(run_id)

        terminal = {"FAILED", "COMPLETED", "CANCELLED"}
        if current in terminal:
            reason = f"Revived via CLI from {current}"
            store.set_run_status(run_id, "PENDING", reason=reason)
            print(f"Run {run_id} revived: {current} -> PENDING")
        else:
            print(f"Run {run_id} is {current}; revive is a no-op.")
    except Exception as e:
        logger.error(f"Failed to revive run: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
