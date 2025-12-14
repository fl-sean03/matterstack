import argparse
import sys
import logging
import time
import random
from pathlib import Path
from typing import Optional, Any, Dict

from matterstack.orchestration.run_lifecycle import (
    initialize_run,
    step_run,
    list_active_runs,
    run_until_completion,
)
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle
from matterstack.core.operator_keys import resolve_operator_key_for_attempt
from matterstack.core.run import RunHandle
from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.orchestration.diagnostics import get_run_frontier
from matterstack.cli.tui import CampaignMonitor
from matterstack.cli.utils import load_workspace_context, find_run
from matterstack.cli.reset import cmd_reset, get_dependents
from matterstack.cli.operator_registry import RegistryConfig, build_operator_registry
from matterstack.config.operator_wiring import (
    resolve_operator_wiring,
    format_operator_wiring_explain_line,
)
import tempfile
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cli")

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

def cmd_status(args):
    """
    Show status of a run.
    """
    run_id = args.run_id
    handle = find_run(run_id)
    
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)
        
    store = SQLiteStateStore(handle.db_path)
    tasks = store.get_tasks(run_id)
    
    print(f"Run ID: {run_id}")
    print(f"Workspace: {handle.workspace_slug}")
    print(f"Root: {handle.root_path}")
    print(f"Total Tasks: {len(tasks)}")
    
    status_counts = {}
    for t in tasks:
        s = store.get_task_status(t.task_id) or "PENDING"
        status_counts[s] = status_counts.get(s, 0) + 1
        
    print("\nTask Status:")
    for s, count in status_counts.items():
        print(f"  {s}: {count}")

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

def _confirm_or_exit(force: bool, prompt: str) -> None:
    if force:
        return
    confirm = input(f"{prompt} [y/N] ")
    if confirm.lower() != "y":
        print("Aborted.")
        sys.exit(0)


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


def cmd_export_evidence(args):
    """
    Export evidence bundle for a run.
    """
    run_id = args.run_id
    handle = find_run(run_id)
    
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)
        
    try:
        store = SQLiteStateStore(handle.db_path)
        print(f"Building evidence bundle for run {run_id}...")
        bundle = build_evidence_bundle(handle, store)
        
        print(f"Exporting to {handle.evidence_path}...")
        export_evidence_bundle(bundle, handle.root_path)
        
        print("Evidence export complete.")
        print(f"Status: {bundle.run_status}")
        print(f"Report: {handle.evidence_path / 'report.md'}")
        
    except Exception as e:
        logger.error(f"Failed to export evidence: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def cmd_explain(args):
    """
    Analyze run state and explain blockers.
    """
    run_id = args.run_id
    handle = find_run(run_id)
    
    if not handle:
        logger.error(f"Run {run_id} not found.")
        sys.exit(1)
        
    try:
        store = SQLiteStateStore(handle.db_path)
        status = store.get_run_status(run_id)
        
        print(f"Run: {run_id}")
        print(f"Status: {status}")
        print(format_operator_wiring_explain_line(handle.root_path))
        
        if status in ["COMPLETED", "FAILED", "CANCELLED"]:
            print("Run is terminal. No active blockers.")
            if status == "FAILED":
                print(f"Reason: {store.get_run_status_reason(run_id)}")
            return

        frontier = get_run_frontier(store, run_id, handle.root_path)
        
        if not frontier:
            print("No active blocking tasks found. The run might be finished or in an inconsistent state.")
        else:
            print(f"\nFound {len(frontier)} blocking item(s):")
            for item in frontier:
                print(f"\n[Task {item.task_id}] - {item.status}")
                print(f"  Reason: {item.reason}")
                op_key = getattr(item, "operator_key", None)
                if op_key:
                    print(f"  Operator Key: {op_key}")
                if item.hint:
                    print(f"  Hint: {item.hint}")
                if item.path:
                    # Try to make path cleaner if it's absolute path to run dir
                    # But get_run_frontier's BlockingItem.path is whatever comes from operator_data
                    # diagnostics.py already handles relative path in hint, but here we print raw path if needed?
                    # Actually, the hint usually contains the path.
                    pass

    except Exception as e:
        logger.error(f"Failed to explain run: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def cmd_monitor(args):
    """
    Launch TUI monitor for a run.
    """
    run_id = args.run_id
    
    if run_id:
        handle = find_run(run_id)
        if not handle:
            logger.error(f"Run {run_id} not found.")
            sys.exit(1)
    else:
        # Find latest active run
        active_runs = list_active_runs()
        if active_runs:
             # Sort by run_id (timestamp) desc
             active_runs.sort(key=lambda h: h.run_id, reverse=True)
             handle = active_runs[0]
             logger.info(f"Auto-attaching to latest active run: {handle.run_id}")
        else:
             logger.error("No active runs found to monitor. Please specify a run ID.")
             sys.exit(1)
             
    try:
        monitor = CampaignMonitor(handle)
        monitor.run()
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
    except Exception as e:
        logger.error(f"Monitor crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

class SelfTestCampaign(Campaign):
    """Minimal campaign for self-test verification."""
    def plan(self, state: Optional[Any]) -> Optional[Workflow]:
        # Initial plan: Create one dummy task
        if not state:
            wf = Workflow()
            wf.add_task(Task(
                task_id="smoke_test_task",
                image="ubuntu:latest",
                command="echo 'MatterStack Self-Test'",
                files={}
            ))
            return wf
        return None # No further work

    def analyze(self, state: Optional[Any], results: Dict[str, Any]) -> Optional[Any]:
        # If we have results, we are done
        if not state and results:
             return {"status": "self_test_complete"}
        return None

def cmd_self_test(args):
    """
    Run a self-test of the MatterStack installation.
    """
    print("Running MatterStack Self-Test...")
    
    # Create temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        workspace_slug = "test_workspace"
        
        try:
            # 1. Initialize
            campaign = SelfTestCampaign()
            handle = initialize_run(workspace_slug, campaign, base_path=base_path)
            print(f"  [x] Initialized run {handle.run_id}")
            
            # 2. Run Loop
            # We expect:
            # - Tick 1: Submit task (Simulate execution) -> RUNNING
            # - Tick 2: Detect completion, Analyze -> RUNNING (New state)
            # - Tick 3: Plan (None) -> COMPLETED
            
            max_steps = 10
            steps = 0
            while steps < max_steps:
                status = step_run(handle, campaign)
                steps += 1
                if status in ["COMPLETED", "FAILED"]:
                    break
                time.sleep(0.1)
                
            if status == "COMPLETED":
                print(f"  [x] Run completed successfully in {steps} steps")
            else:
                print(f"  [!] Run failed or timed out. Status: {status}")
                sys.exit(1)
                
            # 3. Export Evidence
            store = SQLiteStateStore(handle.db_path)
            bundle = build_evidence_bundle(handle, store)
            export_path = base_path / "evidence_export"
            export_evidence_bundle(bundle, export_path)
            
            if (export_path / "evidence" / "report.md").exists():
                 print(f"  [x] Evidence exported to temporary path")
            else:
                 print(f"  [!] Evidence export failed: {(export_path / 'evidence' / 'report.md')} not found")
                 sys.exit(1)
                 
            print("\nSelf-test passed. MatterStack is operational.")
            
        except Exception as e:
            print(f"\n[!] Self-test failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="MatterStack CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # init
    parser_init = subparsers.add_parser("init", help="Initialize a new run")
    parser_init.add_argument("workspace", help="Workspace slug")
    parser_init.add_argument(
        "--operators-config",
        dest="operators_config",
        help="Path to operators.yaml defining operator instances (v0.2.6 Operator System v2).",
    )
    parser_init.set_defaults(func=cmd_init)
    
    # step
    parser_step = subparsers.add_parser("step", help="Execute one step of a run")
    parser_step.add_argument("run_id", help="Run ID")
    parser_step.add_argument(
        "--config",
        help="Path to matterstack YAML config file containing execution profiles (optional)",
    )
    parser_step.add_argument(
        "--operators-config",
        dest="operators_config",
        help="Path to operators.yaml defining operator instances (v0.2.6 Operator System v2).",
    )
    parser_step.add_argument(
        "--force-wiring-override",
        dest="force_wiring_override",
        action="store_true",
        help="Allow replacing an existing run's persisted operator wiring snapshot when --operators-config is provided.",
    )
    parser_step.add_argument(
        "--profile",
        help="Execution profile name to use for the HPC operator backend (optional)",
    )
    parser_step.add_argument(
        "--hpc-config",
        dest="hpc_config",
        help="Path to legacy HPC YAML config (CURC atesting adapter). Overrides --profile for HPC backend.",
    )
    parser_step.set_defaults(func=cmd_step)

    # status
    parser_status = subparsers.add_parser("status", help="Show run status")
    parser_status.add_argument("run_id", help="Run ID")
    parser_status.set_defaults(func=cmd_status)

    # loop
    parser_loop = subparsers.add_parser("loop", help="Loop run until completion or act as scheduler")
    parser_loop.add_argument("run_id", nargs="?", help="Run ID (optional). If omitted, runs in scheduler mode.")
    parser_loop.add_argument(
        "--config",
        help="Path to matterstack YAML config file containing execution profiles (optional)",
    )
    parser_loop.add_argument(
        "--operators-config",
        dest="operators_config",
        help="Path to operators.yaml defining operator instances (v0.2.6 Operator System v2).",
    )
    parser_loop.add_argument(
        "--force-wiring-override",
        dest="force_wiring_override",
        action="store_true",
        help="Allow replacing an existing run's persisted operator wiring snapshot when --operators-config is provided (single-run mode only).",
    )
    parser_loop.add_argument(
        "--profile",
        help="Execution profile name to use for the HPC operator backend (optional)",
    )
    parser_loop.add_argument(
        "--hpc-config",
        dest="hpc_config",
        help="Path to legacy HPC YAML config (CURC atesting adapter). Overrides --profile for HPC backend.",
    )
    parser_loop.set_defaults(func=cmd_loop)

    # cancel
    parser_cancel = subparsers.add_parser("cancel", help="Cancel a run")
    parser_cancel.add_argument("run_id", help="Run ID")
    parser_cancel.set_defaults(func=cmd_cancel)

    # pause
    parser_pause = subparsers.add_parser("pause", help="Pause a run")
    parser_pause.add_argument("run_id", help="Run ID")
    parser_pause.set_defaults(func=cmd_pause)

    # resume
    parser_resume = subparsers.add_parser("resume", help="Resume a run")
    parser_resume.add_argument("run_id", help="Run ID")
    parser_resume.set_defaults(func=cmd_resume)
    
    # export-evidence
    parser_export = subparsers.add_parser("export-evidence", help="Export run evidence")
    parser_export.add_argument("run_id", help="Run ID")
    parser_export.set_defaults(func=cmd_export_evidence)

    # explain
    parser_explain = subparsers.add_parser("explain", help="Explain run status and blockers")
    parser_explain.add_argument("run_id", help="Run ID")
    parser_explain.set_defaults(func=cmd_explain)

    # monitor
    parser_monitor = subparsers.add_parser("monitor", help="Monitor a run with TUI")
    parser_monitor.add_argument("run_id", nargs="?", help="Run ID (optional)")
    parser_monitor.set_defaults(func=cmd_monitor)

    # self-test
    parser_self_test = subparsers.add_parser("self-test", help="Run a self-test of the installation")
    parser_self_test.set_defaults(func=cmd_self_test)

    # revive
    parser_revive = subparsers.add_parser("revive", help="Revive a terminal run back to PENDING")
    parser_revive.add_argument("run_id", help="Run ID")
    parser_revive.set_defaults(func=cmd_revive)

    # rerun
    parser_rerun = subparsers.add_parser("rerun", help="Rerun a task by resetting it to PENDING (creates a new attempt on next tick)")
    parser_rerun.add_argument("run_id", help="Run ID")
    parser_rerun.add_argument("task_id", help="Task ID")
    parser_rerun.add_argument("--recursive", action="store_true", help="Include dependent tasks")
    parser_rerun.add_argument("--force", action="store_true", help="Skip confirmation prompt / force cancel active attempt")
    parser_rerun.set_defaults(func=cmd_rerun)

    # attempts
    parser_attempts = subparsers.add_parser("attempts", help="List attempts for a task (TSV)")
    parser_attempts.add_argument("run_id", help="Run ID")
    parser_attempts.add_argument("task_id", help="Task ID")
    parser_attempts.set_defaults(func=cmd_attempts)

    # cancel-attempt
    parser_cancel_attempt = subparsers.add_parser("cancel-attempt", help="Cancel an attempt safely (DB state; backend cancel best-effort)")
    parser_cancel_attempt.add_argument("run_id", help="Run ID")
    parser_cancel_attempt.add_argument("attempt_id", help="Attempt ID")
    parser_cancel_attempt.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser_cancel_attempt.set_defaults(func=cmd_cancel_attempt)

    # reset-run
    parser_reset = subparsers.add_parser("reset-run", help="Reset or delete tasks in a run")
    parser_reset.add_argument("run_id", help="Run ID")
    parser_reset.add_argument("task_id", help="Target Task ID")
    parser_reset.add_argument("--action", choices=["reset", "delete"], default="reset", help="Action to perform (reset to PENDING or delete)")
    parser_reset.add_argument("--recursive", action="store_true", help="Include dependent tasks")
    parser_reset.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser_reset.set_defaults(func=cmd_reset)

    args = parser.parse_args()
    
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()