"""
Self-test CLI command.

Contains:
- SelfTestCampaign: Minimal campaign for self-test verification
- cmd_self_test: Run a self-test of the MatterStack installation
"""
import sys
import time
import tempfile
from pathlib import Path
from typing import Optional, Any, Dict

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.storage.export import build_evidence_bundle, export_evidence_bundle


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
        return None  # No further work

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
