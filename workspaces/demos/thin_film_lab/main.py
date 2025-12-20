import os
import json
import subprocess
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any

from matterstack import Campaign, Task, Workflow, initialize_run, run_until_completion
from matterstack.runtime.manifests import ExternalStatus


def _find_run_root(start: Path) -> Path:
    """
    Best-effort: find the run root by walking up from the current working directory.
    In v0.2.5 attempt-aware layout, we detect <run_root> by looking for state.sqlite.
    """
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        if (p / "state.sqlite").exists():
            return p
    return start.resolve()


# Helper to simulate robot/lab equipment behavior for ExperimentOperator
def simulate_robot_daemon(run_root: Path):
    """
    Watches for experiment requests in both legacy and v0.2.5+ locations:
    - Legacy: runs/<run_id>/operators/experiment/<uuid>/
    - v0.2.5+: runs/<run_id>/tasks/<task_id>/attempts/<attempt_id>/
    
    Consumes experiment_request.json and produces experiment_result.json.
    """
    legacy_dir = run_root / "operators" / "experiment"
    tasks_dir = run_root / "tasks"
    
    print(f"[Robot Daemon] Watching {legacy_dir} (legacy) and {tasks_dir} (v0.2.5+)...")
    
    processed_dirs = set()
    
    while True:
        # Check legacy path (operators/experiment/<uuid>/)
        if legacy_dir.exists():
            for op_dir in legacy_dir.iterdir():
                if op_dir.is_dir() and str(op_dir) not in processed_dirs:
                    _process_experiment_request(op_dir, run_root, processed_dirs)
        
        # Check v0.2.5+ path (tasks/<task_id>/attempts/<attempt_id>/)
        if tasks_dir.exists():
            for task_dir in tasks_dir.iterdir():
                if task_dir.is_dir():
                    attempts_dir = task_dir / "attempts"
                    if attempts_dir.exists():
                        for attempt_dir in attempts_dir.iterdir():
                            if attempt_dir.is_dir() and str(attempt_dir) not in processed_dirs:
                                _process_experiment_request(attempt_dir, run_root, processed_dirs)
        
        time.sleep(1)


def _process_experiment_request(op_dir: Path, run_root: Path, processed_dirs: set):
    """Process an experiment request in the given directory."""
    req_path = op_dir / "experiment_request.json"
    if not req_path.exists():
        return
    
    print(f"[Robot Daemon] Processing request in: {op_dir}")
    
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
    
    # Generate experiment data
    exp_data_content = {
        "conductivity_exp": 55.0,  # Dummy match for simulation
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
        # In v0.2.5+, we write to the attempt directory AND the run root
        # to ensure the reconcile task can find it
        dest_path = run_root / output_path_override
        try:
            with open(dest_path, "w") as f:
                json.dump(exp_data_content, f)
            print(f"[Robot Daemon] Wrote specific output to {dest_path}")
        except Exception as e:
            print(f"[Robot Daemon] Failed to write output path: {e}")
        
        # Also write to the attempt directory for v0.2.5+ layout
        attempt_dest = op_dir / output_path_override
        try:
            with open(attempt_dest, "w") as f:
                json.dump(exp_data_content, f)
            print(f"[Robot Daemon] Wrote output to attempt dir: {attempt_dest}")
        except Exception as e:
            print(f"[Robot Daemon] Failed to write to attempt dir: {e}")

    with open(op_dir / "experiment_result.json", "w") as f:
        json.dump(result, f)
        
    print(f"[Robot Daemon] Completed experiment. Result written.")
    processed_dirs.add(str(op_dir))


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
                    "MATTERSTACK_OPERATOR": "experiment.default",
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
            
            # Construct paths - these are hints that the script will resolve
            # using the _resolve_attempt_scoped_path helper
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
    handle = initialize_run("demos/thin_film_lab", get_campaign())
    
    # Create config.json to enforce actual execution via LocalBackend
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)

    # Start Robot Daemon
    t = threading.Thread(target=simulate_robot_daemon, args=(handle.root_path,), daemon=True)
    t.start()
    
    print(f"Starting Loop for Run {handle.run_id}")
    run_until_completion(handle, get_campaign())
