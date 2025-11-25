import sys
import os
import subprocess
import time
from pathlib import Path

# Ensure matterstack is in path (it is in current workspace)
sys.path.append(os.getcwd())

from matterstack.core.workflow import Workflow, Task
from matterstack.core.external import ExternalTask
from matterstack.orchestration.api import run_workflow
from matterstack.runtime.backends.local import LocalBackend

def main():
    print("\n--- SIMULATION MODE: All data is synthetic and for demonstration purposes only ---\n")
    print("=== Thin Film Lab Workflow ===")
    
    # 0. Start the Mock Robot Daemon in background
    print("Starting Robot Daemon...")
    # Using cwd as the watch directory for simplicity of the demo
    # In reality, it might watch a specific shared folder.
    # We pass "." so it watches the workspace root or where tasks run.
    # But tasks run in subdirectories. So we should tell robot to watch recursively or the specific task dir.
    # Our mock_robot.py recursively watches "."
    
    robot_proc = subprocess.Popen(
        [sys.executable, "scripts/mock_robot.py", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(os.path.abspath(__file__)) # run from workspace dir
    )
    
    # Give it a moment to start
    time.sleep(1)
    
    try:
        # Define Workflow
        wf = Workflow()
        
        # Task 1: Simulation
        # Generates sim_results.json
        sim_task = Task(
            task_id="sim_task",
            image="python:3.9",
            command="python3 scripts/sim_predict.py",
            # We map scripts so they are available
            env={"PYTHONPATH": os.getcwd()}
        )
        wf.add_task(sim_task)
        
        # Task 2: Robot Handoff
        # Uses ExternalTaskWrapper logic via scripts/handoff.py
        # We depend on sim_task to produce sim_results.json
        # NOTE: In a real system we would map file outputs explicitly.
        # Here, tasks share the working directory in LocalBackend (simplified).
        
        # However, LocalBackend creates separate directories for tasks by default?
        # Let's check LocalBackend implementation... 
        # Typically LocalBackend creates task_id folders.
        # But for this demo, we might need shared storage or artifact passing.
        
        # Assuming LocalBackend runs in a shared workspace root or we need to copy files?
        # If tasks run in isolated dirs, T2 won't see T1's file.
        # Let's assume for this "Mini-Project" that we run in a way that files are accessible 
        # OR we rely on artifact passing which might be implicit in this simplified 'matterstack'.
        
        # Let's just assume simple execution where they can access the scripts directory.
        # But T2 needs 'sim_results.json' from T1.
        
        # If we can't share files easily, we can chain them in one task? No, objective is workflow.
        
        # Workaround: For this demo, we assume we are running locally and can use relative paths 
        # or that the backend is configured to share the workspace.
        # Let's try to copy the artifact or just assume it works for the demo environment.
        
        # Actually, if we look at `test_external_task.py`, it uses `tmp_path` as workspace root.
        
        robot_task = Task(
            task_id="robot_task",
            image="python:3.9",
            command="python3 scripts/handoff.py",
            dependencies={sim_task.task_id},
             env={"PYTHONPATH": os.getcwd()}
        )
        wf.add_task(robot_task)
        
        # Task 3: Reconcile
        reconcile_task = Task(
            task_id="reconcile_task",
            image="python:3.9",
            command="python3 scripts/reconcile_data.py",
            dependencies={robot_task.task_id},
             env={"PYTHONPATH": os.getcwd()}
        )
        wf.add_task(reconcile_task)
        
        # Run Workflow
        # We use the current directory as the workspace root so files are shared/found easily?
        # Actually, we should probably let the backend handle it.
        # But if T2 needs T1's file, we need to ensure T2 runs in the same dir or copies it.
        # The provided `matterstack` code doesn't show explicit artifact passing in `Task` definition visible here.
        # We will assume a "shared filesystem" model for this demo where we can reference files.
        # But `scripts/handoff.py` reads `sim_results.json`.
        
        # To make this robust:
        # T1 writes to absolute path or shared path?
        # Let's just run it and see. If it fails, we fix the file passing.
        
        backend = LocalBackend() # Uses default temporary workspace usually
        # To make them share files, maybe we pass a specific workspace_root?
        # backend = LocalBackend(workspace_root=os.getcwd()) # This would be messy but effective for sharing.
        
        # Better: run in the current directory (workspaces/thin_film_lab)
        # We will initialize LocalBackend with the current dir.
        
        cwd = os.getcwd()
        print(f"Running in {cwd}")
        
        # Note: LocalBackend might clean up? Let's hope not or use existing dir.
        project_root = Path(__file__).resolve().parent.parent.parent
        backend = LocalBackend(workspace_root=Path(base_dir) / "results")
        
        # We also need to make sure the SCRIPTS are available in the run_workspace.
        # We can copy them or just reference them via absolute path in the command?
        # commands are `python3 scripts/sim_predict.py`. 
        # We need `scripts` folder inside `run_workspace` or `scripts` to be absolute.
        
        # Let's make commands use absolute paths to scripts to be safe.
        base_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_dir = os.path.join(base_dir, "scripts")
        
        sim_task.command = f"python3 {os.path.join(scripts_dir, 'sim_predict.py')}"
        robot_task.command = f"python3 {os.path.join(scripts_dir, 'handoff.py')}"
        reconcile_task.command = f"python3 {os.path.join(scripts_dir, 'reconcile_data.py')}"
        
        # Also, T1 needs to output to a place T2 can find.
        # By default they output to CWD (which is inside their task dir in LocalBackend).
        # We need them to share a data dir.
        # Let's pass a SHARED_DIR env var and have scripts use it?
        # Or simpler: have them write to the backend's root?
        
        # For this demo, let's try relying on the fact that we can write to a shared absolute path?
        # Or... let's modify the scripts to take output/input paths arguments?
        # That's best practice but I already wrote the scripts to use CWD.
        
        # Quick fix: Symlink `sim_results.json` from T1 to T2? 
        # No, that requires orchestration.
        
        # Let's use a shared folder for data exchange for this demo.
        data_dir = os.path.join(base_dir, "data_exchange")
        os.makedirs(data_dir, exist_ok=True)
        
        # Update commands to change dir to data_dir?
        # Or better, just run everything inside data_dir?
        # LocalBackend runs tasks in separate dirs.
        # We will use "cd {data_dir} && ..." for commands.
        
        cmd_prefix = f"cd {data_dir} &&"
        sim_task.command = f"{cmd_prefix} python3 {os.path.join(scripts_dir, 'sim_predict.py')}"
        robot_task.command = f"{cmd_prefix} python3 {os.path.join(scripts_dir, 'handoff.py')}"
        reconcile_task.command = f"{cmd_prefix} python3 {os.path.join(scripts_dir, 'reconcile_data.py')}"
        
        # Also need to make sure `matterstack` is importable.
        # PYTHONPATH is set to os.getcwd() (project root) in env.
        
        result = run_workflow(wf, backend=backend)
        
        print("\nWorkflow Finished!")
        print(f"Status: {result.status}")
        
    finally:
        print("Stopping Robot Daemon...")
        robot_proc.terminate()
        robot_proc.wait()

if __name__ == "__main__":
    main()