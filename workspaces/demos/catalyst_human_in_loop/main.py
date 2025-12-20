import os
import time
import json
import threading
from pathlib import Path
from typing import Optional, Dict, Any

from matterstack import Campaign, Task, Workflow, initialize_run, run_until_completion
from matterstack.runtime.manifests import ExternalStatus

# Helper to simulate human interaction with new Operator structure
def simulate_human_approver(run_root: Path):
    """
    Watches runs/<run_id>/operators/human/ for new tasks and approves them.
    """
    operators_dir = run_root / "operators" / "human"
    print(f"[Human Simulator] Watching {operators_dir}...")
    
    processed_uuids = set()
    
    while True:
        if operators_dir.exists():
            for op_dir in operators_dir.iterdir():
                if op_dir.is_dir() and op_dir.name not in processed_uuids:
                    # found a new human task directory
                    print(f"[Human Simulator] Found task: {op_dir.name}")
                    
                    # Wait a bit to simulate thinking
                    time.sleep(2)
                    
                    # Create response.json
                    response = {
                        "status": "COMPLETED",
                        "data": {"approved": True, "comment": "Looks good!"}
                    }
                    
                    resp_path = op_dir / "response.json"
                    with open(resp_path, "w") as f:
                        json.dump(response, f)
                        
                    print(f"[Human Simulator] Approved task in {resp_path}")
                    processed_uuids.add(op_dir.name)
        
        time.sleep(1)

class CatalystHumanCampaign(Campaign):
    def plan(self, state: Optional[Dict[str, Any]] = None) -> Optional[Workflow]:
        iteration = 0
        energy_files = []
        if state:
            iteration = state.get("iteration", 0)
            energy_files = state.get("energy_files", [])
            
        workflow = Workflow()
        base_dir = Path(__file__).parent.absolute()
        scripts_dir = base_dir / "scripts"
        
        # Iteration 0: Propose Candidates
        if iteration == 0:
            print("Campaign: Planning Iteration 0 (Propose Candidates)...")
            task_propose = Task(
                task_id="propose_candidates",
                image="python:3.9",
                command=f"python3 {scripts_dir}/propose_candidates.py --output candidates.csv",
                env={"PYTHONPATH": os.getcwd()}
            )
            workflow.add_task(task_propose)
            return workflow
            
        # Iteration 1: Human Approval Gate
        elif iteration == 1:
            print("Campaign: Planning Iteration 1 (Human Approval)...")
            
            # Point to the candidate file from previous step
            # Path relative to run_root/task_id?
            # The human operator UI would ideally get a link.
            # For now, we put the path in instructions.
            cand_path = "../propose_candidates/candidates.csv"
            
            # Using generic Task mapped to HumanOperator via env var
            gate_task = Task(
                task_id="human_approval",
                image="python:3.9",
                command="echo 'Waiting for human...'",
                env={
                    "MATTERSTACK_OPERATOR": "human.default",
                    "INSTRUCTIONS": f"Please review {cand_path} and approve."
                }
            )
            workflow.add_task(gate_task)
            return workflow

        # Iteration 2: Fan-out Adsorption Calcs
        elif iteration == 2:
            print("Campaign: Planning Iteration 2 (Calculations)...")
            for i in range(3):
                t = Task(
                    task_id=f"calc_ads_{i}",
                    image="python:3.9",
                    command=f"python3 {scripts_dir}/calc_adsorption.py candidate_{i}",
                    env={"PYTHONPATH": os.getcwd()}
                )
                workflow.add_task(t)
            return workflow

        # Iteration 3: Rank Results
        elif iteration == 3:
            print("Campaign: Planning Iteration 3 (Ranking)...")
            
            # Pass aggregated energy files
            # Paths relative to rank_results task dir (which is run_root/rank_results)
            # So we prepend "../" to the paths stored in state (which are relative to run_root)
            
            input_args = " ".join([f"../{f}" for f in energy_files])
            
            task_rank = Task(
                task_id="rank_results",
                image="python:3.9",
                command=f"python3 {scripts_dir}/rank_results.py {input_args}",
                env={"PYTHONPATH": os.getcwd()}
            )
            workflow.add_task(task_rank)
            return workflow
            
        return None

    def analyze(self, current_state: Any, results: Dict[str, Any]) -> Any:
        iteration = 0
        energy_files = []
        
        if current_state:
            iteration = current_state.get("iteration", 0)
            energy_files = current_state.get("energy_files", [])
            
        print(f"Campaign: Analyzing results from iteration {iteration}...")
        
        if iteration == 2:
            # Collect results from Fan-out Adsorption Calcs
            for task_id, res in results.items():
                if task_id.startswith("calc_ads_") and res.get("status") == "COMPLETED":
                     energy_files.append(f"{task_id}/energy.json")
        
        new_state = {
            "iteration": iteration + 1,
            "energy_files": energy_files
        }
        return new_state

def get_campaign():
    return CatalystHumanCampaign()

if __name__ == "__main__":
    print("Initializing Run...")
    handle = initialize_run("catalyst_human_in_loop", get_campaign())
    
    # Create config.json to enforce actual execution via LocalBackend
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)

    # Start Human Simulator
    t = threading.Thread(target=simulate_human_approver, args=(handle.root_path,), daemon=True)
    t.start()
    
    print(f"Starting Loop for Run {handle.run_id}")
    run_until_completion(handle, get_campaign())