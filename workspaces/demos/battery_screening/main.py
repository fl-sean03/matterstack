import json
import random
from pathlib import Path
from typing import Any, Dict, Optional

from matterstack import Campaign, Task, Workflow, initialize_run, run_until_completion


class BatteryScreeningCampaign(Campaign):
    def plan(self, state: Optional[Dict[str, Any]] = None) -> Optional[Workflow]:
        # state is loaded from JSON, might be None initially
        iteration = 0
        successful_results = []
        all_candidate_paths = []
        if state:
            iteration = state.get("iteration", 0)
            successful_results = state.get("successful_results", [])
            all_candidate_paths = state.get("all_candidate_paths", [])

        workflow = Workflow()
        base_dir = Path(__file__).parent.absolute()
        scripts_dir = base_dir / "scripts"

        # Iteration 0: Generate Candidates
        if iteration == 0:
            print("Campaign: Planning Iteration 0 (Candidates)...")
            # We create 10 candidates for the demo
            for i in range(10):
                task_id = f"cand_{i:03d}"
                doping_level = round(random.uniform(0.01, 0.20), 3)
                dopant = random.choice(["Al", "Si", "Ti", "Mg"])

                cmd = f"python3 {scripts_dir}/calc_properties.py --candidate_id {task_id} --doping_level {doping_level} --dopant {dopant}"

                # Note: We rely on config.json in the run directory to determine execution mode (Local vs HPC)

                task = Task(
                    task_id=task_id,
                    image="python:3.9",
                    command=cmd,
                    allow_failure=True
                )
                workflow.add_task(task)
            return workflow

        # Iteration 1: Aggregate
        elif iteration == 1:
            print("Campaign: Planning Iteration 1 (Aggregation)...")

            # Pass list of result files to aggregator
            # Assuming tasks ran in run_root/task_id/ (LocalBackend behavior)
            # We construct paths relative to where aggregator runs (run_root usually, or run_root/aggregator)
            # Actually, tasks run in run_root/task_id. Aggregator runs in run_root/aggregator.
            # So paths should be ../{task_id}/results.json

            # We now pass ALL candidate paths (successful or not) so train_model can count failures
            targets = all_candidate_paths if all_candidate_paths else successful_results
            result_args = " ".join([f"../{r}" for r in targets])

            if not result_args:
                print("Warning: No results to aggregate.")
                # We still run it to generate the empty report/card
                result_args = "dummy_path_to_satisfy_argparse"

            agg_cmd = f"python3 {scripts_dir}/train_model.py {result_args}"

            agg_task = Task(
                task_id="aggregator",
                image="python:3.9",
                command=agg_cmd
            )
            workflow.add_task(agg_task)
            return workflow

        return None

    def analyze(self, current_state: Any, results: Dict[str, Any]) -> Any:
        # Update state
        iteration = 0
        successful_results = []
        all_candidate_paths = []

        if current_state:
            iteration = current_state.get("iteration", 0)
            successful_results = current_state.get("successful_results", [])
            all_candidate_paths = current_state.get("all_candidate_paths", [])

        print(f"Campaign: Analyzing results from iteration {iteration}...")

        if iteration == 0:
            # Collect successful result paths AND all candidate paths
            for task_id, res in results.items():
                # Store relative path from run_root for EVERY candidate
                all_candidate_paths.append(f"{task_id}/results.json")

                if res.get("status") == "COMPLETED":
                    # We expect the file at task_id/results.json
                    successful_results.append(f"{task_id}/results.json")

        # Stop after iteration 1 (Aggregator)
        if iteration >= 1:
            pass

        # Just increment iteration in the persisted state
        new_state = {
            "iteration": iteration + 1,
            "successful_results": successful_results,
            "all_candidate_paths": all_candidate_paths
        }
        return new_state

    # Fix for engine state management:
    # Engine.run loop calls: analyze(result) -> update internal state?
    # Engine.run loop: self.analyze(result)
    # The base Campaign.analyze is abstract.
    # My implementation returns `new_state`.
    # BUT `run_lifecycle.py` (Orchestrator) handles the loop now!
    # `run_lifecycle.py` calls `new_state = campaign.analyze(...)`.
    # And then persists it.
    # And then calls `campaign.plan(new_state)`.
    # So `self.state.stopped` is irrelevant for `run_lifecycle` loop.
    # `run_lifecycle` stops if `plan` returns None.

    # So I just need to make sure `plan` returns None when done.

def get_campaign():
    return BatteryScreeningCampaign()

if __name__ == "__main__":
    # Dev helper to run locally
    print("Initializing Run...")
    handle = initialize_run("battery_screening", get_campaign())

    # Create config.json to test Local execution explicitly
    # We use "HPC" mode to trigger DirectHPCOperator -> LocalBackend execution
    # because the default "Local" mode in run_lifecycle just simulates success.
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)

    # Run loop
    print(f"Starting Loop for Run {handle.run_id}")
    run_until_completion(handle, get_campaign())
