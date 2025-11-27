import argparse
import sys
import logging
import importlib.util
import time
import random
from pathlib import Path
from typing import Optional, Any, Dict

from matterstack.orchestration.run_lifecycle import initialize_run, step_run, list_active_runs, run_until_completion
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle
from matterstack.core.run import RunHandle
from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.orchestration.diagnostics import get_run_frontier
import tempfile
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cli")

def load_workspace_context(workspace_slug: str) -> Any:
    """
    Dynamically load the workspace module and retrieve the campaign.
    Expects 'workspaces/{workspace_slug}/main.py' to exist.
    It looks for a 'get_campaign()' function.
    """
    workspace_path = Path("workspaces") / workspace_slug
    main_py = workspace_path / "main.py"
    
    if not main_py.exists():
        raise FileNotFoundError(f"Workspace main file not found: {main_py}")
        
    spec = importlib.util.spec_from_file_location(f"workspace.{workspace_slug}", main_py)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {main_py}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"workspace.{workspace_slug}"] = module
    spec.loader.exec_module(module)
    
    if hasattr(module, "get_campaign"):
        return module.get_campaign()
    else:
        # Fallback: try to find a Campaign subclass to instantiate?
        # For now, let's strictly require get_campaign or raise helpful error
        # But for Thrust 9 testing purposes, maybe we can hack it if it doesn't exist?
        # No, strict is better. The user will be told to implement it.
        # However, for testing this CLI before Thrust 10, we might need to mock this.
        raise AttributeError(f"Workspace module {main_py} does not export 'get_campaign()'.")

def find_run(run_id: str, base_path: Path = Path("workspaces")) -> Optional[RunHandle]:
    """
    Locate a run directory by searching all workspaces.
    """
    if not base_path.exists():
        return None
        
    for ws_dir in base_path.iterdir():
        if ws_dir.is_dir():
            run_dir = ws_dir / "runs" / run_id
            if run_dir.exists():
                return RunHandle(
                    workspace_slug=ws_dir.name,
                    run_id=run_id,
                    root_path=run_dir
                )
    return None

def cmd_init(args):
    """
    Initialize a new run.
    """
    workspace_slug = args.workspace
    try:
        campaign = load_workspace_context(workspace_slug)
        handle = initialize_run(workspace_slug, campaign)
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
        status = step_run(handle, campaign)
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
            run_until_completion(handle, campaign)
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
                        # Attempt to step the run
                        # We need to load the campaign first.
                        # We do this inside the loop to ensure we pick up code changes if we wanted to (though modules are cached)
                        # but mainly to keep context fresh.
                        
                        # Optimization: We could check if locked BEFORE loading heavy context?
                        # But step_run handles the locking.
                        # However, step_run loads the store.
                        
                        # To implement "Skip if locked", we need to catch the RuntimeError from step_run
                        # OR check lock manually.
                        
                        # Let's wrap step_run calls.
                        
                        campaign = load_workspace_context(handle.workspace_slug)
                        
                        # We use step_run which attempts to lock.
                        status = step_run(handle, campaign)
                        
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
    parser_init.set_defaults(func=cmd_init)
    
    # step
    parser_step = subparsers.add_parser("step", help="Execute one step of a run")
    parser_step.add_argument("run_id", help="Run ID")
    parser_step.set_defaults(func=cmd_step)
    
    # status
    parser_status = subparsers.add_parser("status", help="Show run status")
    parser_status.add_argument("run_id", help="Run ID")
    parser_status.set_defaults(func=cmd_status)
    
    # loop
    parser_loop = subparsers.add_parser("loop", help="Loop run until completion or act as scheduler")
    parser_loop.add_argument("run_id", nargs="?", help="Run ID (optional). If omitted, runs in scheduler mode.")
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

    # self-test
    parser_self_test = subparsers.add_parser("self-test", help="Run a self-test of the installation")
    parser_self_test.set_defaults(func=cmd_self_test)

    args = parser.parse_args()
    
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()