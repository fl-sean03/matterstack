import threading
import time
import os
import sys
from pathlib import Path

# Add project root to path so we can import matterstack
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from matterstack.core.workflow import Workflow, Task
from matterstack.core.gate import GateTask
from matterstack.runtime.backends.local import LocalBackend
from matterstack.orchestration.api import run_workflow

# Helper to simulate human interaction
def simulate_human_approver(gate_dir: Path, delay: int = 5):
    """Waits for a bit, then creates approved.txt in the gate directory."""
    print(f"[Human Simulator] Waiting {delay}s before approving...")
    time.sleep(delay)
    
    # In LocalBackend, the task might run in a unique job directory or in the CWD depending on config.
    # But since we provided absolute paths to GateTask (via string), the wrapper should respect them.
    # However, GateTask wrapper receives config via command line JSON.
    # If we pass absolute paths, it should work.
    
    approve_file = gate_dir / "approved.txt"
    with open(approve_file, "w") as f:
        f.write("Approved by Human Simulator")
        
    print(f"[Human Simulator] Created {approve_file}. Gate should open now.")

def main():
    print("\n--- SIMULATION MODE: All data is synthetic and for demonstration purposes only ---\n")

    # 1. Setup paths
    # Use absolute paths to be safe
    base_dir = Path("workspaces/catalyst_human_in_loop").resolve()
    gate_dir = base_dir / "gate"
    scripts_dir = base_dir / "scripts"
    
    # Ensure gate dir exists and is clean
    os.makedirs(gate_dir, exist_ok=True)
    if (gate_dir / "approved.txt").exists():
        os.remove(gate_dir / "approved.txt")
        
    # 2. Define Tasks
    
    img = "python:3.9"
    env = {"PYTHONPATH": str(project_root)}
    
    # Task 1: Propose Candidates
    task_propose = Task(
        task_id="propose_candidates",
        image=img,
        command=f"python3 {scripts_dir}/propose_candidates.py",
        env=env
    )
    
    # Task 2: Human Gate
    gate_approve_path = str(gate_dir / "approved.txt")
    gate_reject_path = str(gate_dir / "rejected.txt")
    gate_info_path = str(gate_dir / "gate_info.json")

    task_gate = GateTask(
        task_id="human_approval",
        image=img,
        message="Please review candidates.csv and approve.",
        approve_file=gate_approve_path,
        reject_file=gate_reject_path,
        info_file=gate_info_path,
        poll_interval=1.0,
        env=env
    )
    
    # Task 3: Fan-out Adsorption Calcs
    calc_tasks = []
    for i in range(3):
        t = Task(
            task_id=f"calc_ads_{i}",
            image=img,
            command=f"python3 {scripts_dir}/calc_adsorption.py candidate_{i}",
            env=env
        )
        calc_tasks.append(t)
        
    # Task 4: Rank Results
    task_rank = Task(
        task_id="rank_results",
        image=img,
        command=f"python3 {scripts_dir}/rank_results.py",
        env=env
    )
    
    # 3. Assemble Workflow
    wf = Workflow()
    wf.add_task(task_propose)
    wf.add_task(task_gate)
    
    task_gate.dependencies.add(task_propose.task_id)
    
    for t in calc_tasks:
        wf.add_task(t)
        t.dependencies.add(task_gate.task_id)
        task_rank.dependencies.add(t.task_id)
        
    wf.add_task(task_rank)
    
    # 4. Start Simulated Human (Background Thread)
    t = threading.Thread(target=simulate_human_approver, args=(gate_dir, 5))
    t.start()
    
    # 5. Run Workflow
    # We use workspace root for LocalBackend to make sure it can run python module
    backend = LocalBackend(workspace_root=base_dir / "results")
    
    print("Starting Workflow...")
    result = run_workflow(wf, backend=backend)
    print("Workflow Completed.")
    
    t.join()

if __name__ == "__main__":
    main()