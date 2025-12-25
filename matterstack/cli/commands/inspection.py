"""
Inspection and monitoring CLI commands.

Contains commands for inspecting run state:
- cmd_status: Show run status
- cmd_explain: Explain run blockers
- cmd_monitor: TUI monitor
- cmd_export_evidence: Export evidence bundle
"""

import logging
import sys

from matterstack.cli.tui import CampaignMonitor
from matterstack.cli.utils import find_run
from matterstack.config.operator_wiring import format_operator_wiring_explain_line
from matterstack.orchestration.diagnostics import get_run_frontier
from matterstack.orchestration.run_lifecycle import list_active_runs
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle
from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger("cli.inspection")


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
