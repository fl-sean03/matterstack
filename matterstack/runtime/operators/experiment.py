from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Optional

from pydantic import ValidationError

from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus, Operator, OperatorResult
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.runtime.fs_safety import attempt_evidence_dir, operator_run_dir
from matterstack.runtime.manifests import ExperimentRequestManifest, ExperimentResultManifest, ExternalStatus
from matterstack.runtime.task_manifest import write_task_manifest_json
from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger(__name__)


class ExperimentOperator(Operator):
    """
    Operator for physical experiment tasks.
    Prepares a directory with experiment parameters (experiment_request.json),
    then waits for external confirmation (experiment_result.json) to mark as complete.
    """

    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Prepare the execution environment.

        Attempt-aware layout (preferred):
            runs/<run_id>/tasks/<task_id>/attempts/<attempt_id>/

        Legacy fallback:
            runs/<run_id>/operators/experiment/<uuid>/
        """
        if not isinstance(task, Task):
            raise TypeError(f"ExperimentOperator expects a Task object, got {type(task)}")

        # Best-effort: discover attempt_id (created just before dispatch in the orchestrator)
        attempt_id: Optional[str] = None
        try:
            store = SQLiteStateStore(run.db_path)
            attempt = store.get_current_attempt(task.task_id)
            if attempt is not None:
                attempt_id = attempt.attempt_id
        except Exception as e:
            logger.debug(f"Could not resolve attempt_id for task {task.task_id}: {e}")

        operator_uuid = str(uuid.uuid4())

        if attempt_id:
            full_path = attempt_evidence_dir(run.root_path, task.task_id, attempt_id)
        else:
            full_path = operator_run_dir(run.root_path, "experiment", operator_uuid)

        # Ensure we compute relative path correctly by resolving run.root_path too
        relative_path = full_path.relative_to(run.root_path.resolve())

        # Create directory
        full_path.mkdir(parents=True, exist_ok=True)

        # 1. Write manifest.json (lean; no embedded file contents)
        manifest_path = full_path / "manifest.json"
        write_task_manifest_json(manifest_path, task)

        # 2. Generate experiment_request.json
        # This file is intended to be consumed by the lab control software.
        # We can populate it from task.env or a specific file in task.files
        # Create manifest using Pydantic model
        request_manifest = ExperimentRequestManifest(
            task_id=task.task_id, parameters=task.env, files=list(task.files.keys())
        )

        # If the task provides a specific 'experiment_config' in env, use that
        if "EXPERIMENT_CONFIG" in task.env:
            try:
                config = json.loads(task.env["EXPERIMENT_CONFIG"])
                request_manifest.config = config
            except json.JSONDecodeError:
                request_manifest.config_raw = task.env["EXPERIMENT_CONFIG"]

        request_path = full_path / "experiment_request.json"
        with open(request_path, "w") as f:
            f.write(request_manifest.model_dump_json(indent=2))

        # 3. Copy any provided files
        for filename, content in task.files.items():
            file_path = full_path / filename
            if isinstance(content, str):
                with open(file_path, "w") as f:
                    f.write(content)
            # Handling Path objects (copying) is omitted for simplicity in this version
            # as it requires source access which might be complex.

        handle = ExternalRunHandle(
            task_id=task.task_id,
            operator_type="Experiment",
            status=ExternalRunStatus.CREATED,
            operator_data={
                "operator_uuid": operator_uuid,
                "attempt_id": attempt_id,
                "absolute_path": str(full_path),
            },
            relative_path=relative_path,
        )

        logger.info(f"Prepared Experiment run for task {task.task_id} at {relative_path}")
        return handle

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Submit the work to the external system.
        Marks status as WAITING_EXTERNAL.
        """
        if handle.status != ExternalRunStatus.CREATED:
            if handle.status in [ExternalRunStatus.WAITING_EXTERNAL, ExternalRunStatus.COMPLETED]:
                return handle
            logger.warning(f"Submit called on handle with status {handle.status}")

        handle.status = ExternalRunStatus.WAITING_EXTERNAL
        logger.info(f"Task {handle.task_id} is now WAITING_EXTERNAL for experiment execution.")
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Check the current status of the external execution.
        Checks for experiment_result.json.
        """
        if handle.status == ExternalRunStatus.COMPLETED:
            return handle

        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
            return handle

        op_dir = Path(path_str)
        if not op_dir.exists():
            return handle

        # Check for experiment_result.json
        result_file = op_dir / "experiment_result.json"
        if result_file.exists():
            try:
                with open(result_file, "r") as f:
                    try:
                        result_manifest = ExperimentResultManifest.model_validate_json(f.read())

                        if result_manifest.status == ExternalStatus.COMPLETED:
                            handle.status = ExternalRunStatus.COMPLETED
                            logger.info(f"Task {handle.task_id} completed (found experiment_result.json).")
                            return handle
                        elif result_manifest.status == ExternalStatus.FAILED:
                            handle.status = ExternalRunStatus.FAILED
                            handle.operator_data["error"] = result_manifest.error or "Unknown error"
                            return handle
                    except ValidationError as ve:
                        logger.error(f"Invalid experiment_result.json for {handle.task_id}: {ve}")
                        handle.status = ExternalRunStatus.FAILED
                        handle.operator_data["error"] = f"Invalid result format: {ve}"
                        return handle
            except Exception as e:
                logger.warning(f"Failed to read experiment_result.json for {handle.task_id}: {e}")

        return handle

    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        """
        Retrieve results after completion.
        """
        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
            return OperatorResult(
                task_id=handle.task_id, status=ExternalRunStatus.FAILED, error_message="Missing absolute_path"
            )

        op_dir = Path(path_str)
        result_file = op_dir / "experiment_result.json"

        data = {}
        if result_file.exists():
            try:
                with open(result_file, "r") as f:
                    content = json.load(f)
                    data = content.get("data", {})
            except Exception:
                pass

        # Collect files
        result_files = {}
        system_files = {"manifest.json", "experiment_request.json", "experiment_result.json"}

        if op_dir.exists():
            for f in op_dir.rglob("*"):
                if f.is_file() and f.name not in system_files:
                    rel_name = f.relative_to(op_dir).as_posix()
                    result_files[rel_name] = f

        return OperatorResult(task_id=handle.task_id, status=handle.status, files=result_files, data=data)
