# Operator Development Guide

Operators are the bridge between MatterStack and external execution systems. While Backends (like `SlurmBackend`) handle standard shell tasks, **Operators** are designed for complex, asynchronous interactions where MatterStack delegates control to a third-party service (e.g., a cloud API, a specialized lab instrument, or a human-in-the-loop workflow) and waits for a result.

This guide explains how to implement custom Operators.

## The Operator Concept

An Operator manages the full lifecycle of an **External Run**. Unlike a simple subprocess, an external run has its own identity, state, and persistence on a remote system.

### Key Responsibilities
1.  **Prepare**: Create any necessary input files or payloads.
2.  **Submit**: Initiate the process on the external system.
3.  **Monitor**: Poll for status updates.
4.  **Collect**: Retrieve results and artifacts upon completion.

## Safety & Manifests

To ensure security and robustness, v0.2.2 introduces strict guidelines for Operator filesystem access and data exchange.

### Filesystem Safety

Operators must NOT write outside the designated run directory. The `matterstack.runtime.fs_safety` module provides helpers to enforce this.

*   `ensure_under_run_root(root, target)`: Verifies that a target path is a child of the run root.
*   `operator_run_dir(run_root, operator_type, uuid)`: Generates a safe, canonical path for operator data.

### Manifest Validation

All data exchanged between MatterStack and external systems (Operator inputs/outputs) must be validated using Pydantic schemas (Manifests).

*   **Input**: `manifest.json` (Task definition) and specific request files (e.g., `experiment_request.json`).
*   **Output**: `response.json` or `experiment_result.json`.

Operators must validate incoming JSON against the schema and fail gracefully if validation fails, rather than crashing.

## The External Run Lifecycle

Every external execution follows a strict state machine managed by `ExternalRunStatus`:

1.  `CREATED`: The run is initialized in the MatterStack database but not yet sent to the external system.
2.  `SUBMITTED`: The submit request was successful; the external system has accepted the job.
3.  `RUNNING`: The external system is actively processing the request.
4.  `WAITING_EXTERNAL`: The job is paused or queued externally.
5.  `COMPLETED`: The job finished successfully.
6.  `FAILED`: The job failed execution.
7.  `CANCELLED`: The job was stopped by a user request.

## Implementing an Operator

To create a new operator, subclass `matterstack.core.operators.Operator` and implement the four abstract methods.

```python
from typing import Any
from pathlib import Path
from matterstack.core.operators import (
    Operator, 
    ExternalRunHandle, 
    ExternalRunStatus, 
    OperatorResult
)
from matterstack.core.run import RunHandle

class MyCustomOperator(Operator):
    
    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Step 1: Setup the execution environment.
        Create directories, write config files, etc.
        """
        # Create a directory for this specific task execution
        task_dir = run.root_path / "tasks" / task.task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        
        # Return initial handle
        return ExternalRunHandle(
            task_id=task.task_id,
            operator_type="MyCustomOperator",
            status=ExternalRunStatus.CREATED,
            relative_path=task_dir.relative_to(run.root_path),
            operator_data={"params": task.custom_parameters}
        )

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Step 2: Submit to external API.
        """
        # Example: call external API
        external_id = my_api_client.submit_job(
            params=handle.operator_data["params"]
        )
        
        # Update handle with new status and external ID
        handle.external_id = external_id
        handle.status = ExternalRunStatus.SUBMITTED
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Step 3: Poll for updates.
        """
        if not handle.external_id:
            return handle
            
        status_response = my_api_client.get_status(handle.external_id)
        
        # Map external status to MatterStack status
        if status_response == "DONE":
            handle.status = ExternalRunStatus.COMPLETED
        elif status_response == "ERROR":
            handle.status = ExternalRunStatus.FAILED
        elif status_response == "PROCESSING":
            handle.status = ExternalRunStatus.RUNNING
            
        return handle

    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        """
        Step 4: Download and parse results.
        """
        # Define where to save results
        output_dir = handle.relative_path  # We stored this in prepare_run
        
        # Fetch results
        data = my_api_client.get_results(handle.external_id)
        
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
            data=data, # Structured data
            files={"report": Path("report.pdf")} # Paths to downloaded files
        )
```

## Best Practices

### 1. Idempotency
Your `submit` method might be called multiple times if the orchestrator restarts. Check `handle.status` or `handle.external_id` before re-submitting to avoid duplicate jobs.

```python
def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
    if handle.external_id:
        # Already submitted
        return handle
    # ... proceed with submission
```

### 2. Serialization
The `ExternalRunHandle` is serialized to the `state.sqlite` database. Ensure that `operator_data` only contains JSON-serializable types (dicts, lists, strings, numbers). Do not store complex Python objects or open file handles.

### 3. Error Handling
If an exception occurs within your operator methods, MatterStack captures it and may mark the task as failed. However, you should handle expected external errors (e.g., API timeouts) gracefully, perhaps by not updating the status and allowing the next poll cycle to retry.