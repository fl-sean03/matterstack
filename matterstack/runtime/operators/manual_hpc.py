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
from matterstack.runtime.fs_safety import operator_run_dir
from matterstack.runtime.manifests import ManualHPCStatusManifest, ExternalStatus
from matterstack.runtime.task_manifest import write_task_manifest_json

logger = logging.getLogger(__name__)

class ManualHPCOperator(Operator):
    """
    Operator for manual execution of tasks.
    Prepares a directory with instructions/scripts, then waits for
    external confirmation (file existence) to mark as complete.
    """

    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Prepare the execution environment (directories, scripts).
        """
        if not isinstance(task, Task):
            raise TypeError(f"ManualHPCOperator expects a Task object, got {type(task)}")

        operator_uuid = str(uuid.uuid4())
        
        # Use fs_safety
        full_path = operator_run_dir(run.root_path, "manual", operator_uuid)
        relative_path = full_path.relative_to(run.root_path)
        
        # Create directory
        full_path.mkdir(parents=True, exist_ok=True)
        
        # 1. Write manifest.json (lean; no embedded file contents)
        manifest_path = full_path / "manifest.json"
        write_task_manifest_json(manifest_path, task)
            
        # 2. Generate job_template.sh
        job_script_path = full_path / "job_template.sh"
        with open(job_script_path, "w") as f:
            f.write("#!/bin/bash\n")
            f.write(f"# Job for Task: {task.task_id}\n")
            f.write("# Instructions:\n")
            f.write("# 1. Edit this script or run your commands manually.\n")
            f.write("# 2. Place output files in the 'output' directory.\n")
            f.write("# 3. When finished, create a 'status.json' file with {\"status\": \"COMPLETED\"}\n")
            f.write("#    OR simply ensure files exist in 'output/' folder.\n")
            f.write("\n")
            f.write("mkdir -p output\n")
            f.write("# YOUR COMMANDS HERE\n")
            f.write("echo 'Hello from Manual HPC' > output/result.txt\n")
            f.write("\n")
            f.write("# Signal completion\n")
            f.write("echo '{\"status\": \"COMPLETED\"}' > status.json\n")
            
        # 3. Create 'output' directory
        (full_path / "output").mkdir(exist_ok=True)
        
        handle = ExternalRunHandle(
            task_id=task.task_id,
            operator_type="ManualHPC",
            status=ExternalRunStatus.CREATED,
            operator_data={
                "operator_uuid": operator_uuid,
                "absolute_path": str(full_path)
            },
            relative_path=relative_path
        )
        
        logger.info(f"Prepared ManualHPC run for task {task.task_id} at {relative_path}")
        return handle

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Submit the work to the external system.
        For ManualHPC, this simply marks the task as waiting for external action.
        """
        if handle.status != ExternalRunStatus.CREATED:
            if handle.status in [ExternalRunStatus.WAITING_EXTERNAL, ExternalRunStatus.COMPLETED]:
                return handle
            logger.warning(f"Submit called on handle with status {handle.status}")
            
        handle.status = ExternalRunStatus.WAITING_EXTERNAL
        logger.info(f"Task {handle.task_id} is now WAITING_EXTERNAL for manual execution.")
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Check the current status of the external execution.
        Checks for status.json or files in output/ directory.
        """
        if handle.status == ExternalRunStatus.COMPLETED:
            return handle
            
        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
            logger.error(f"Cannot check status: absolute_path missing for {handle.task_id}")
            return handle
            
        op_dir = Path(path_str)
        if not op_dir.exists():
            # If the directory is gone, maybe it failed? Or we are on a different machine?
            # For now, just log and return
            logger.warning(f"Operator directory not found: {op_dir}")
            return handle

        # 1. Check for status.json
        status_file = op_dir / "status.json"
        if status_file.exists():
            try:
                with open(status_file, "r") as f:
                    try:
                        status_manifest = ManualHPCStatusManifest.model_validate_json(f.read())
                        
                        if status_manifest.status == ExternalStatus.COMPLETED:
                            handle.status = ExternalRunStatus.COMPLETED
                            logger.info(f"Task {handle.task_id} completed (found status.json).")
                            return handle
                        elif status_manifest.status == ExternalStatus.FAILED:
                            handle.status = ExternalRunStatus.FAILED
                            handle.operator_data["error"] = status_manifest.error or "Unknown error from status.json"
                            return handle
                    except ValidationError as ve:
                        logger.error(f"Invalid status.json for {handle.task_id}: {ve}")
                        handle.status = ExternalRunStatus.FAILED
                        handle.operator_data["error"] = f"Invalid status format: {ve}"
                        return handle
            except Exception as e:
                logger.warning(f"Failed to read status.json for {handle.task_id}: {e}")

        # 2. Check for output files (fallback if no status.json)
        output_dir = op_dir / "output"
        if output_dir.exists():
            # Check if directory has any files
            has_files = any(f.is_file() for f in output_dir.rglob("*"))
            if has_files:
                handle.status = ExternalRunStatus.COMPLETED
                logger.info(f"Task {handle.task_id} completed (found files in output/).")
                
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
        output_dir = op_dir / "output"
        
        result_files = {}
        
        # Collect files from output directory
        if output_dir.exists():
            for f in output_dir.rglob("*"):
                if f.is_file():
                    # Key relative to output dir
                    rel_name = f.relative_to(output_dir).as_posix()
                    result_files[rel_name] = f
        
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
            files=result_files
        )