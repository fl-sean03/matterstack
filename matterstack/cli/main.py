"""
MatterStack CLI entry point.

This module contains:
- Argparse setup for all subcommands
- main() entry point (referenced by pyproject.toml: matterstack.cli.main:main)

Command implementations are in the commands/ subpackage.
"""

import argparse
import logging

from matterstack.cli.commands import (
    cmd_attempts,
    cmd_cancel,
    cmd_cancel_attempt,
    cmd_cleanup_orphans,
    cmd_explain,
    cmd_export_evidence,
    cmd_init,
    cmd_loop,
    cmd_monitor,
    cmd_pause,
    cmd_rerun,
    cmd_resume,
    cmd_revive,
    cmd_self_test,
    cmd_status,
    cmd_step,
)
from matterstack.cli.reset import cmd_reset

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


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
    parser_rerun = subparsers.add_parser(
        "rerun", help="Rerun a task by resetting it to PENDING (creates a new attempt on next tick)"
    )
    parser_rerun.add_argument("run_id", help="Run ID")
    parser_rerun.add_argument("task_id", help="Task ID")
    parser_rerun.add_argument("--recursive", action="store_true", help="Include dependent tasks")
    parser_rerun.add_argument(
        "--force", action="store_true", help="Skip confirmation prompt / force cancel active attempt"
    )
    parser_rerun.set_defaults(func=cmd_rerun)

    # attempts
    parser_attempts = subparsers.add_parser("attempts", help="List attempts for a task (TSV)")
    parser_attempts.add_argument("run_id", help="Run ID")
    parser_attempts.add_argument("task_id", help="Task ID")
    parser_attempts.set_defaults(func=cmd_attempts)

    # cancel-attempt
    parser_cancel_attempt = subparsers.add_parser(
        "cancel-attempt", help="Cancel an attempt safely (DB state; backend cancel best-effort)"
    )
    parser_cancel_attempt.add_argument("run_id", help="Run ID")
    parser_cancel_attempt.add_argument("attempt_id", help="Attempt ID")
    parser_cancel_attempt.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser_cancel_attempt.set_defaults(func=cmd_cancel_attempt)

    # reset-run
    parser_reset = subparsers.add_parser("reset-run", help="Reset or delete tasks in a run")
    parser_reset.add_argument("run_id", help="Run ID")
    parser_reset.add_argument("task_id", help="Target Task ID")
    parser_reset.add_argument(
        "--action", choices=["reset", "delete"], default="reset", help="Action to perform (reset to PENDING or delete)"
    )
    parser_reset.add_argument("--recursive", action="store_true", help="Include dependent tasks")
    parser_reset.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser_reset.set_defaults(func=cmd_reset)

    # cleanup-orphans
    parser_cleanup = subparsers.add_parser(
        "cleanup-orphans",
        help="Find and clean up orphaned attempts (stuck in CREATED state)",
    )
    parser_cleanup.add_argument("--run-id", required=True, help="Run ID")
    parser_cleanup.add_argument(
        "--timeout",
        default="1h",
        help="Age threshold for orphan detection (e.g., 1h, 30m, 3600s). Default: 1h",
    )
    parser_cleanup.add_argument(
        "--confirm",
        action="store_true",
        help="Actually mark orphans as FAILED_INIT (without this flag, only lists)",
    )
    parser_cleanup.set_defaults(func=cmd_cleanup_orphans)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
