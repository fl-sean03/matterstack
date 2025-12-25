"""
Minimal validation workspace for testing chronologically sortable attempt IDs.

This campaign creates a single simple task that runs an echo command on HPC.
Used to validate that attempt IDs now use the format YYYYMMDD_HHMMSS_<uuid8>
which sorts chronologically in directory listings.
"""

from typing import Any, Dict, Optional

from matterstack import Campaign, Task, Workflow


class AttemptIdValidationCampaign(Campaign):
    """Simple campaign with one task to validate attempt ID format."""

    def plan(self, state: Optional[Dict[str, Any]] = None) -> Optional[Workflow]:
        """Plan a single echo task."""
        iteration = 0
        if state:
            iteration = state.get("iteration", 0)

        # Only run one iteration
        if iteration >= 1:
            return None

        workflow = Workflow()

        task = Task(
            task_id="echo_test",
            image="bash:latest",
            command='echo "Hello from attempt ID validation"',
            operator_key="hpc.atesting",  # v0.2.6+ first-class routing
        )
        workflow.add_task(task)

        return workflow

    def analyze(self, current_state: Any, results: Dict[str, Any]) -> Any:
        """Analyze results and update state."""
        iteration = 0
        if current_state:
            iteration = current_state.get("iteration", 0)

        print(f"Analyzed iteration {iteration} results: {list(results.keys())}")

        return {"iteration": iteration + 1}


def get_campaign():
    """Return campaign instance for CLI discovery."""
    return AttemptIdValidationCampaign()


if __name__ == "__main__":
    from matterstack import initialize_run, run_until_completion

    print("Initializing validation run...")
    handle = initialize_run("attempt_id_validation", get_campaign())
    print(f"Run ID: {handle.run_id}")
    print(f"Run root: {handle.root_path}")
    run_until_completion(handle, get_campaign())
