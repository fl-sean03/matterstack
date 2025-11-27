from __future__ import annotations
import uuid
import logging
import json
from pathlib import Path
from typing import Any, Dict, Optional
from pydantic import ValidationError

from matterstack.core.operators import (
    Operator,
    ExternalRunHandle,
    ExternalRunStatus,
    OperatorResult
)
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.runtime.fs_safety import operator_run_dir, ensure_under_run_root
from matterstack.runtime.manifests import HumanResponseManifest, ExternalStatus

logger = logging.getLogger(__name__)

class HumanOperator(Operator):
    """
    Operator for human-in-the-loop tasks.
    Prepares a directory with instructions, then waits for
    external confirmation (response.json) to mark as complete.
    """

    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Prepare the execution environment (directories, instructions).
        """
        if not isinstance(task, Task):
            raise TypeError(f"HumanOperator expects a Task object, got {type(task)}")

        operator_uuid = str(uuid.uuid4())
        
        # Use fs_safety to get a safe path
        full_path = operator_run_dir(run.root_path, "human", operator_uuid)
        relative_path = full_path.relative_to(run.root_path.resolve())
        
        # Create directory
        full_path.mkdir(parents=True, exist_ok=True)
        
        # 1. Write manifest.json
        manifest_path = full_path / "manifest.json"
        with open(manifest_path, "w") as f:
            f.write(task.model_dump_json(indent=2))
            
        # 2. Generate instructions.md
        # Extract instructions from task env or files if available, otherwise default
        instructions_content = task.env.get("INSTRUCTIONS", "Please complete the task as described.")
        
        # Also check if 'instructions.md' is provided in task.files
        if "instructions.md" in task.files:
            content_val = task.files["instructions.md"]
            if isinstance(content_val, str):
                instructions_content = content_val
            # If path, we would copy it, but let's stick to string for now or basic logic
        
        instructions_path = full_path / "instructions.md"
        with open(instructions_path, "w") as f:
            f.write(f"# Human Task: {task.task_id}\n\n")
            f.write(instructions_content)
            f.write("\n\n")
            f.write("## Completion\n")
            f.write("To complete this task, create a file named `response.json` in this directory.\n")
            f.write("Format:\n")
            f.write("```json\n{\n  \"status\": \"COMPLETED\",\n  \"data\": { ... }\n}\n```\n")

        # 3. Generate schema.json (optional, for validated input)
        # Using a placeholder for now
        schema_path = full_path / "schema.json"
        with open(schema_path, "w") as f:
            f.write(json.dumps({
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["COMPLETED", "FAILED"]},
                    "data": {"type": "object"}
                },
                "required": ["status"]
            }, indent=2))
            
        handle = ExternalRunHandle(
            task_id=task.task_id,
            operator_type="Human",
            status=ExternalRunStatus.CREATED,
            operator_data={
                "operator_uuid": operator_uuid,
                "absolute_path": str(full_path)
            },
            relative_path=relative_path
        )
        
        logger.info(f"Prepared Human run for task {task.task_id} at {relative_path}")
        return handle

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Submit the work to the external system.
        For HumanOperator, this simply marks the task as waiting for external action.
        """
        if handle.status != ExternalRunStatus.CREATED:
            if handle.status in [ExternalRunStatus.WAITING_EXTERNAL, ExternalRunStatus.COMPLETED]:
                return handle
            logger.warning(f"Submit called on handle with status {handle.status}")
            
        handle.status = ExternalRunStatus.WAITING_EXTERNAL
        logger.info(f"Task {handle.task_id} is now WAITING_EXTERNAL for human input.")
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Check the current status of the external execution.
        Checks for response.json.
        """
        if handle.status == ExternalRunStatus.COMPLETED:
            return handle
            
        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
            logger.error(f"Cannot check status: absolute_path missing for {handle.task_id}")
            return handle
            
        op_dir = Path(path_str)
        if not op_dir.exists():
            logger.warning(f"Operator directory not found: {op_dir}")
            return handle

        # Check for response.json
        response_file = op_dir / "response.json"
        if response_file.exists():
            try:
                with open(response_file, "r") as f:
                    try:
                        # Validate with Pydantic
                        resp = HumanResponseManifest.model_validate_json(f.read())
                        
                        if resp.status == ExternalStatus.COMPLETED:
                            handle.status = ExternalRunStatus.COMPLETED
                            logger.info(f"Task {handle.task_id} completed (found response.json).")
                            return handle
                        elif resp.status == ExternalStatus.FAILED:
                            handle.status = ExternalRunStatus.FAILED
                            handle.operator_data["error"] = resp.error or "Unknown error from response.json"
                            return handle
                    except ValidationError as ve:
                        logger.error(f"Invalid response.json for {handle.task_id}: {ve}")
                        handle.status = ExternalRunStatus.FAILED
                        handle.operator_data["error"] = f"Invalid response format: {ve}"
                        return handle
            except Exception as e:
                logger.warning(f"Failed to read response.json for {handle.task_id}: {e}")
                
        return handle

    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        """
        Retrieve results after completion.
        """
        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
             return OperatorResult(
                task_id=handle.task_id,
                status=ExternalRunStatus.FAILED,
                error_message="Missing absolute_path in operator_data"
            )
            
        op_dir = Path(path_str)
        response_file = op_dir / "response.json"
        
        data = {}
        if response_file.exists():
            try:
                with open(response_file, "r") as f:
                    content = json.load(f)
                    data = content.get("data", {})
            except Exception as e:
                logger.warning(f"Failed to load data from response.json: {e}")

        # Collect any other files in the directory (excluding system files)
        result_files = {}
        system_files = {"manifest.json", "instructions.md", "schema.json", "response.json"}
        
        if op_dir.exists():
            for f in op_dir.rglob("*"):
                if f.is_file() and f.name not in system_files:
                    # Key relative to operator dir
                    rel_name = f.relative_to(op_dir).as_posix()
                    result_files[rel_name] = f
        
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
            files=result_files,
            data=data
        )