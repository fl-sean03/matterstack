import sys
import os
import random
from pathlib import Path

# Add project root to path to ensure matterstack imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.api import run_workflow
from matterstack.runtime.backends.local import LocalBackend

def main():
    print("\n--- SIMULATION MODE: All data is synthetic and for demonstration purposes only ---\n")

    # Setup workspace
    base_dir = Path(__file__).parent.absolute()
    # Assume project root is 2 levels up if we are in workspaces/battery_screening
    project_root = base_dir.parent.parent
    scripts_dir = base_dir / "scripts"
    
    # We use a dedicated directory for execution artifacts
    # execution_dir = base_dir / "execution"
    execution_dir = base_dir / "results"
    execution_dir.mkdir(parents=True, exist_ok=True)
    
    backend = LocalBackend(workspace_root=str(execution_dir))
    workflow = Workflow()
    
    print(f"Building workflow in {execution_dir}...")
    
    candidate_tasks = []
    task_dirs = []
    
    # 1. Generate 100 candidates
    for i in range(100):
        task_id = f"cand_{i:03d}"
        doping_level = round(random.uniform(0.01, 0.20), 3)
        dopant = random.choice(["Al", "Si", "Ti", "Mg"])
        
        # Calculate properties task
        # We use python explicitly. Assuming python is available in environment.
        cmd = f"python3 {scripts_dir}/calc_properties.py --candidate_id {task_id} --doping_level {doping_level} --dopant {dopant}"
        
        task = Task(
            task_id=task_id,
            image="python:3.9", # Ignored by LocalBackend but good practice
            command=cmd
        )
        
        workflow.add_task(task)
        candidate_tasks.append(task_id)
        
        # We anticipate the directory where this task will run
        # LocalBackend uses {workspace_root}/{task_id}
        task_dir = execution_dir / task_id
        task_dirs.append(str(task_dir))
        
    # 2. Aggregator Task
    # It depends on ALL candidate tasks
    # It receives the list of expected directories
    
    # We pass the directories as arguments. 
    # Since we have 100, the command line might get long but should be fine for local execution.
    # Alternatively we could write a manifest file, but passing args is simpler for this demo.
    
    dirs_arg = " ".join(task_dirs)
    agg_cmd = f"python3 {scripts_dir}/train_model.py {dirs_arg}"
    
    agg_task = Task(
        task_id="aggregator",
        image="python:3.9",
        command=agg_cmd,
        dependencies=set(candidate_tasks),
        allow_dependency_failure=True
    )
    
    workflow.add_task(agg_task)
    
    print(f"Submitting workflow with {len(candidate_tasks) + 1} tasks...")
    
    # 3. Execute with continue_on_error=True
    result = run_workflow(
        workflow,
        backend=backend,
        continue_on_error=True, # Critical for soft failure
        poll_interval=0.1
    )
    
    print(f"Workflow finished with status: {result.status}")
    
    # Verify results
    agg_result = result.tasks["aggregator"]
    # TaskResult.status is a JobStatus object, which HAS a .state attribute
    if agg_result.status.state == "COMPLETED":
        print("Aggregator completed successfully.")
        print("Check model_card.md in the aggregator directory.")
        
        # Find where aggregator ran
        agg_dir = execution_dir / "aggregator"
        model_card = agg_dir / "model_card.md"
        if model_card.exists():
            print(f"\n--- Model Card ({model_card}) ---")
            print(model_card.read_text())
        else:
            print("Error: model_card.md not found!")
    else:
        print(f"Aggregator failed/cancelled! Status: {agg_result.status.state}")
        # If aggregator was cancelled, it means continue_on_error didn't work as expected for dependencies?
        # Wait, if continue_on_error=True, failed dependencies normally cause cancellation of dependent tasks.
        # BUT the requirement says "The aggregator task logic must handle missing inputs (since some upstream tasks failed)".
        # IF the engine cancels tasks with failed dependencies, then the aggregator will NEVER run if one candidate fails.
        #
        # Let's re-read the prompt: "The aggregator task logic must handle missing inputs (since some upstream tasks failed)."
        # This implies the aggregator MUST RUN even if upstream tasks fail.
        #
        # Does `continue_on_error=True` allow dependent tasks to run?
        # Let's check `tests/test_soft_failure.py` again.
        # In `test_workflow_soft_failure`:
        # "Task C: Depends on B (Should be Cancelled)"
        # So standard behavior is: if dep fails, dependent is cancelled.
        #
        # This contradicts the requirement! If aggregator depends on ALL candidates, and one fails, aggregator is cancelled.
        # To satisfy "Aggregator ... handles missing inputs", the Aggregator must NOT depend on them in a "strict" way, 
        # OR we need a "soft dependency" feature, OR we just don't declare the dependencies and rely on order (not safe for parallel),
        # OR we modify the engine?
        #
        # "Constraints: Use `matterstack` imports."
        #
        # Is there a way to define "weak dependencies" or "allow_failure" on dependencies?
        # I checked `api.py`.
        # Lines 201-218:
        # `failed_deps = ...`
        # `if failed_deps: ... cancelled_result ... continue`
        #
        # It seems the current engine enforces strict dependencies.
        #
        # HOW TO SOLVE:
        # 1. Don't declare dependencies? 
        #    If we don't declare dependencies, the aggregator might run before candidates finish.
        #    We are running `run_workflow` which is sequential (lines 178: "run strictly sequentially").
        #    So if we add tasks in order (Candidates then Aggregator) and don't declare dependencies, it will work LOCALLY.
        #    BUT this is "cheating" the DAG.
        #
        # 2. Is there a "Trigger" or "After" relationship that isn't strict?
        #    Not in the code I saw.
        #
        # 3. Maybe the user *thinks* continue_on_error applies to dependencies too?
        #    The prompt says: "Execute run_workflow with continue_on_error=True."
        #    And: "The aggregator task logic must handle missing inputs".
        #
        #    If I follow the instructions strictly, the aggregator will be cancelled.
        #    This suggests I might need to *modify* the `run_workflow` logic or `Task` definition to allow this?
        #    OR the prompt assumes I will fix it.
        #
        #    Let's look at `run_workflow` again in `matterstack/orchestration/api.py`.
        #    The logic for cancelling due to failed deps is hardcoded.
        #
        #    However, if I use `continue_on_error=True`, the *workflow* continues.
        #    The *Task* C is cancelled.
        #
        #    If I want Aggregator to run, I must ensure it doesn't have *failed* dependencies.
        #    
        #    Hack: Wrap the candidate tasks so they "succeed" even if they fail?
        #    The prompt says: "Crucially, it should **fail randomly** (exit code 1)".
        #    So the task status MUST be FAILED.
        #
        #    So `run_workflow` sees FAILED.
        #    And Aggregator depends on FAILED.
        #    So Aggregator is CANCELLED.
        #
        #    This seems to be a conflict in the prompt's implied behavior vs actual engine behavior.
        #    "Verify that the workflow completes successfully (Partial Success) and the model card is generated despite failures."
        #
        #    To generate the model card, the Aggregator MUST run.
        #
        #    Maybe I should NOT declare dependencies in the DAG, but ensure ordering?
        #    "Create an Aggregator task (`train_model.py`) that depends on *all* candidate tasks."
        #    This is an explicit instruction.
        #
        #    So I must declare dependencies.
        #    And I must make it run.
        #
        #    This implies I need to modify `matterstack/orchestration/api.py` to allow "soft dependencies" or change how `continue_on_error` treats dependencies.
        #    OR, I wrap the command in `calc_properties.py` to catch the exit code and return success but write a "FAILED" status file?
        #    BUT the prompt says "fail randomly (exit code 1)".
        #
        #    So the Task Status WILL be FAILED.
        #
        #    I MUST modify `matterstack/orchestration/api.py` to support this use case.
        #    The `run_workflow` function checks `failed_deps`.
        #
        #    If `continue_on_error` is True, maybe we should NOT cancel dependents?
        #    "Dependent tasks will be cancelled, but independent tasks will run." (Docstring line 142)
        #    The docstring explicitly says dependents are cancelled.
        #
        #    So the current implementation matches the docstring.
        #    But the TASK asks for something different.
        #
        #    Hypothesis: The user wants me to implement a "best effort" aggregation pattern.
        #    In many workflow engines (like Argo), you can specify `continueOn: Failed`.
        #    
        #    I will modify `matterstack/orchestration/api.py` to add a flag `allow_partial_success_deps` or similar, OR change `continue_on_error` semantics if appropriate?
        #    Or maybe I can change the aggregator to have `trigger_rule="all_done"`?
        #    The `Task` class doesn't have `trigger_rule`.
        #
        #    Let's check `matterstack/core/workflow.py` to see Task definition.
        #    (I haven't read it yet, but `api.py` imports it).
        
        #    Actually, I'll first write `main.py` as requested. If it fails (Aggregator cancelled), then I have proof I need to fix the engine.
        #    This is a safer approach: Implement -> Fail -> Fix.
        
        #    Wait, I can't afford to fail and retry too many times.
        #    I'll assume the engine needs patching because the logic is clear.
        #    If I change `api.py` to NOT cancel if `continue_on_error=True`, that breaks the "Dependent tasks will be cancelled" contract.
        #
        #    Maybe I can add `always_run=True` to the Aggregator Task?
        #    I'll check `Task` definition first.
        
        pass

if __name__ == "__main__":
    main()