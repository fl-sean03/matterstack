import sys
import os
import json
import subprocess
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from matterstack import Campaign, Task, Workflow, initialize_run, run_until_completion
from matterstack.runtime.manifests import ExternalStatus

# Helper to simulate robot/lab equipment behavior for ExperimentOperator
def simulate_robot_daemon(run_root: Path):
    """
    Watches runs/<run_id>/operators/experiment/ for requests.
    Consumes experiment_request.json and produces experiment_result.json.
    """
    operators_dir = run_root / "operators" / "experiment"
    print(f"[Robot Daemon] Watching {operators_dir}...")
    
    processed_uuids = set()
    
    while True:
        if operators_dir.exists():
            for op_dir in operators_dir.iterdir():
                if op_dir.is_dir() and op_dir.name not in processed_uuids:
                    # New experiment request found
                    req_path = op_dir / "experiment_request.json"
                    if req_path.exists():
                        print(f"[Robot Daemon] Processing request in: {op_dir.name}")
                        
                        # Read request to check for output_path
                        output_path_override = None
                        try:
                            with open(req_path, 'r') as f:
                                req_data = json.load(f)
                                config = req_data.get("config", {})
                                output_path_override = config.get("output_path")
                        except Exception as e:
                            print(f"[Robot Daemon] Failed to parse request: {e}")

                        # Simulate experiment duration
                        time.sleep(2)
                        
                        # Create dummy spectrum file as artifact
                        spectrum_path = op_dir / "spectrum.csv"
                        spectrum_path.write_text("wavelength,intensity\n400,0.1\n500,0.8\n600,0.2")
                        
                        # Generate data
                        exp_data_content = {
                            "conductivity_exp": 55.0, # Dummy match for simulation
                            "stability_exp": 0.85
                        }
                        
                        # Write result manifest
                        result = {
                            "status": "COMPLETED",
                            "data": {"yield": 0.95, "purity": 0.99},
                            "files": ["spectrum.csv"]
                        }
                        
                        # If specific output path requested, write data there
                        # This mimics the robot system pushing data to a shared location
                        if output_path_override:
                            # Resolve path relative to run root.
                            # Daemon has run_root in scope? Yes, passed as arg.
                            dest_path = run_root / output_path_override
                            try:
                                with open(dest_path, "w") as f:
                                    json.dump(exp_data_content, f)
                                print(f"[Robot Daemon] Wrote specific output to {dest_path}")
                            except Exception as e:
                                print(f"[Robot Daemon] Failed to write output path: {e}")

                        with open(op_dir / "experiment_result.json", "w") as f:
                            json.dump(result, f)
                            
                        print(f"[Robot Daemon] Completed experiment. Result written.")
                        processed_uuids.add(op_dir.name)
        
        time.sleep(1)

class ThinFilmCampaign(Campaign):
    def plan(self, state: Optional[Dict[str, Any]] = None) -> Optional[Workflow]:
        iteration = 0
        if state:
            iteration = state.get("iteration", 0)
        
        workflow = Workflow()
        base_dir = Path(__file__).parent.absolute()
        scripts_dir = base_dir / "scripts"
        
        # Iteration 0: Simulation
        if iteration == 0:
            print("Campaign: Planning Iteration 0 (Simulation)...")
            sim_task = Task(
                task_id="sim_predict",
                image="python:3.9",
                command=f"python3 {scripts_dir}/sim_predict.py",
                env={"PYTHONPATH": os.getcwd()}
            )
            workflow.add_task(sim_task)
            return workflow
            
        # Iteration 1: Robot Handoff (Experiment)
        elif iteration == 1:
            print("Campaign: Planning Iteration 1 (Robot Experiment)...")
            
            # Define where we want the robot to put the data
            # Relative to run_root
            robot_output = "robot_data.json"
            
            # This task uses ExperimentOperator via env var
            robot_task = Task(
                task_id="robot_execution",
                image="python:3.9",
                command="echo 'Dispatching to Robot'",
                env={
                    "MATTERSTACK_OPERATOR": "Experiment",
                    "EXPERIMENT_CONFIG": json.dumps({
                        "recipe": "A123",
                        "temp": 300,
                        "output_path": robot_output
                    })
                }
            )
            workflow.add_task(robot_task)
            return workflow
            
        # Iteration 2: Reconcile Data
        elif iteration == 2:
            print("Campaign: Planning Iteration 2 (Reconcile)...")
            
            # Construct paths
            # sim_predict output: ../sim_predict/sim_results.json
            # robot output: ../robot_data.json
            
            sim_path = "../sim_predict/sim_results.json"
            exp_path = "../robot_data.json"
            
            reconcile_task = Task(
                task_id="reconcile_data",
                image="python:3.9",
                command=f"python3 {scripts_dir}/reconcile_data.py --sim {sim_path} --exp {exp_path}",
                env={"PYTHONPATH": os.getcwd()}
            )
            workflow.add_task(reconcile_task)
            return workflow
            
        return None

    def analyze(self, current_state: Any, results: Dict[str, Any]) -> Any:
        iteration = 0
        if current_state:
            iteration = current_state.get("iteration", 0)
            
        print(f"Campaign: Analyzing results from iteration {iteration}...")
        
        # In a real campaign, we would read the results from the robot task here
        # and update the state.
        
        new_state = {"iteration": iteration + 1}
        return new_state

def get_campaign():
    return ThinFilmCampaign()

if __name__ == "__main__":
    print("Initializing Run...")
    handle = initialize_run("thin_film_lab", get_campaign())
    
    # Create config.json to enforce actual execution via LocalBackend
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)

    # Start Robot Daemon
    t = threading.Thread(target=simulate_robot_daemon, args=(handle.root_path,), daemon=True)
    t.start()
    
    print(f"Starting Loop for Run {handle.run_id}")
    run_until_completion(handle, get_campaign())