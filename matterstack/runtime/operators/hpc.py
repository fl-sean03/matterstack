from __future__ import annotations
import asyncio
import json
import uuid
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List

from matterstack.core.operators import (
    Operator,
    ExternalRunHandle,
    ExternalRunStatus,
    OperatorResult
)
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.core.backend import ComputeBackend, JobState
from matterstack.runtime.fs_safety import operator_run_dir

logger = logging.getLogger(__name__)

class ComputeOperator(Operator):
    """
    Generic Operator that submits tasks to a ComputeBackend (Local, Slurm, etc.).
    """

    def __init__(self, backend: ComputeBackend, slug: str = "compute", operator_name: str = "DirectHPC"):
        self.backend = backend
        self.slug = slug
        self.operator_name = operator_name

    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Prepare the execution environment.
        
        Creates: runs/<run_id>/operators/<slug>/<ext_uuid>/
        Writes: manifest.json
        """
        if not isinstance(task, Task):
            raise TypeError(f"ComputeOperator expects a Task object, got {type(task)}")

        # Generate a unique ID for this operator execution instance
        operator_uuid = str(uuid.uuid4())
        
        # Define the local path for this operator's data
        # Structure: <run_root>/operators/<slug>/<operator_uuid>
        full_path = operator_run_dir(run.root_path, self.slug, operator_uuid)
        relative_path = full_path.relative_to(run.root_path.resolve())
        
        # Create directory
        full_path.mkdir(parents=True, exist_ok=True)
        
        # Serialize task to manifest.json for persistence/debugging
        manifest_path = full_path / "manifest.json"
        with open(manifest_path, "w") as f:
            f.write(task.model_dump_json(indent=2))
            
        # Create handle
        handle = ExternalRunHandle(
            task_id=task.task_id,
            operator_type=self.operator_name,
            status=ExternalRunStatus.CREATED,
            operator_data={
                "operator_uuid": operator_uuid,
                "task_dump": task.model_dump(mode='json'), # Store task data for submit()
                "absolute_path": str(full_path)
            },
            relative_path=relative_path
        )
        
        logger.info(f"Prepared {self.slug} run for task {task.task_id} at {relative_path}")
        return handle

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Submit the work to the external system via Backend.
        """
        if handle.status != ExternalRunStatus.CREATED:
            logger.warning(f"Submit called on handle with status {handle.status}, expected CREATED.")
            # If already submitted, maybe just return? Or fail? 
            # For robustness, if it's already SUBMITTED or RUNNING, we assume it's done.
            if handle.status in [ExternalRunStatus.SUBMITTED, ExternalRunStatus.RUNNING]:
                return handle
        
        # Reconstruct Task from operator_data
        task_data = handle.operator_data.get("task_dump")
        if not task_data:
            raise ValueError("Task data missing from operator handle.")
        
        task = Task.model_validate(task_data)
        
        # Submit to backend (async call wrapped in sync)
        try:
            job_id = asyncio.run(self.backend.submit(task))
            
            # Update handle
            handle.external_id = job_id
            handle.status = ExternalRunStatus.SUBMITTED
            logger.info(f"Submitted task {handle.task_id} to backend. Job ID: {job_id}")
            
        except Exception as e:
            logger.error(f"Failed to submit task {handle.task_id}: {e}")
            handle.status = ExternalRunStatus.FAILED
            handle.operator_data["error"] = str(e)
            
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Check the current status of the external execution.
        """
        if not handle.external_id:
            # Not submitted yet?
            return handle
            
        try:
            job_status = asyncio.run(self.backend.poll(handle.external_id))
            
            # Map JobState to ExternalRunStatus
            new_status = self._map_status(job_status.state)
            
            if new_status != handle.status:
                logger.info(f"Task {handle.task_id} status changed: {handle.status} -> {new_status}")
                handle.status = new_status
                
            if job_status.reason:
                handle.operator_data["reason"] = job_status.reason
                
        except Exception as e:
            logger.error(f"Failed to poll status for {handle.task_id}: {e}")
            # Don't fail the run immediately on poll failure? 
            # Or maybe we do? Let's keep it as is, maybe retry logic belongs in orchestrator.
            
        return handle

    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        """
        Retrieve results after completion.
        """
        # Determine paths
        # relative_path is "operators/hpc/<uuid>"
        # We need the absolute path to write results to
        # But handle doesn't store run root.
        # However, collect_results typically happens when we have context.
        # Wait, the Operator interface assumes we can just return paths.
        # But we need to DOWNLOAD them from backend if they are remote.
        
        # We need to know where to download TO.
        # The handle has `relative_path` which is where we Prepared the run.
        # But we don't have the root path here!
        # This is a design flaw in the interface if Operator is stateless and doesn't know Run Root.
        # The `prepare_run` had `RunHandle`, but `collect_results` only has `ExternalRunHandle`.
        
        # Option A: Store absolute path in operator_data? No, absolute paths break relocatability.
        # Option B: The Caller passes context? No, interface is fixed.
        # Option C: We rely on the caller to have mounted the run directory or something?
        
        # Wait, `ExternalRunHandle` has `relative_path`.
        # If we are running in the context of the run, we might know the CWD or something.
        # But `DirectHPCOperator` might be initialized generally.
        
        # Let's look at `RunHandle` definition again in memory?
        # No, we only get `ExternalRunHandle`.
        
        # ASSUMPTION: The orchestrator or whatever calls this has ensured that
        # we can resolve the path. 
        # BUT, if we need to download files, we need a local target.
        # Let's assume for now we can't easily resolve the absolute path from just ExternalRunHandle
        # UNLESS we assume the process CWD is the run root OR we stored the root in `__init__`?
        # No, Operator is singleton-ish or instantiated per run?
        # "DirectHPCOperator wraps SlurmBackend". Backend usually knows `workspace_root` (remote).
        
        # Let's check `matterstack/core/operators.py`.
        # It doesn't give a solution.
        
        # Workaround: For now, we will SKIP downloading if we can't determine path,
        # OR we assume that `handle.relative_path` is relative to CWD if we are inside the run?
        # The orchestrator `step_run` doesn't change CWD.
        
        # CRITICAL FIX: The `ExternalRunHandle` SHOULD probably store the `run_id`.
        # But it doesn't.
        
        # Let's assume we can modify `ExternalRunHandle` or just fail to download for now?
        # Or, we just return the remote paths? 
        # `OperatorResult` has `files: Dict[str, Path]`.
        
        # Let's look at `collect_results` requirements again.
        # "Retrieve outputs from the operator directory."
        
        # If the backend is Remote (SSH), we MUST download them.
        # If the backend is Local, they are already there?
        
        # Let's assume for `DirectHPCOperator`, we want to download to the operator directory.
        # I will enforce that `operator_data` must contain `run_root` (as string) 
        # OR I'll assume that the CWD is the workspace root?
        
        # BETTER IDEA: `prepare_run` stores `run_root` in `operator_data`.
        # Absolute paths are risky if we move the folder, but for a running campaign it's okay.
        # Or we store `absolute_operator_path` in `operator_data`.
        
        path_str = handle.operator_data.get("absolute_path")
        if not path_str:
            # Fallback: try to construct from CWD if we are lucky, or fail.
            # But wait, `prepare_run` logic I wrote above:
            # full_path = run.root_path / relative_path
            # I should store `str(full_path)` in operator_data.
            pass
            
        local_dir = Path(path_str) if path_str else Path(handle.relative_path) 
        # Warning: if relative, where is it relative to?
        
        # If we can't download, we assume they are remote or we fail.
        # Let's assume we stored `absolute_path` in `prepare_run`.
        
        result_files = {}
        
        if handle.external_id:
            try:
                # Download everything to the operator dir
                # Backend.download(job_id, ".", local_path)
                asyncio.run(self.backend.download(handle.external_id, ".", str(local_dir)))
                
                # List files
                for f in local_dir.rglob("*"):
                    if f.is_file():
                        # Key is relative to operator dir? or just filename?
                        result_files[f.name] = f
                        
            except Exception as e:
                logger.error(f"Failed to download results for {handle.task_id}: {e}")
                return OperatorResult(
                    task_id=handle.task_id,
                    status=ExternalRunStatus.FAILED,
                    error_message=f"Download failed: {e}"
                )

        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
            files=result_files,
            data={"job_id": handle.external_id}
        )

    def _map_status(self, job_state: JobState) -> ExternalRunStatus:
        if job_state == JobState.QUEUED:
            return ExternalRunStatus.SUBMITTED
        elif job_state == JobState.RUNNING:
            return ExternalRunStatus.RUNNING
        elif job_state == JobState.COMPLETED_OK:
            # Internal mapping: COMPLETED_OK -> COMPLETED (which might mean DONE_PENDING_COLLECT logically,
            # but ExternalRunStatus only has COMPLETED)
            return ExternalRunStatus.COMPLETED
        elif job_state == JobState.COMPLETED_ERROR:
            return ExternalRunStatus.FAILED
        elif job_state == JobState.LOST:
            return ExternalRunStatus.FAILED
        elif job_state == JobState.CANCELLED:
            return ExternalRunStatus.CANCELLED
        
        # Default fallback
        logger.warning(f"Unknown JobState {job_state}, mapping to RUNNING.")
        return ExternalRunStatus.RUNNING