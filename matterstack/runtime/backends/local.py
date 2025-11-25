from __future__ import annotations
import os
import shlex
import shutil
import asyncio
import logging
from typing import Dict, Optional, Union, Any
from pathlib import Path

from ...core.backend import ComputeBackend, JobStatus, JobState
from ...core.workflow import Task

logger = logging.getLogger(__name__)

class LocalBackend(ComputeBackend):
    """
    Local Backend for executing Tasks as asynchronous subprocesses.
    Supports a "dry_run" mode for verification.
    """

    def __init__(self, workspace_root: str = "workspace", dry_run: bool = False):
        self.workspace_root = Path(workspace_root).resolve()
        self.dry_run = dry_run
        # Map job_id -> JobStatus
        self._jobs: Dict[str, JobStatus] = {}
        # Map job_id -> asyncio.subprocess.Process
        self._processes: Dict[str, asyncio.subprocess.Process] = {}

        if not self.dry_run:
            self.workspace_root.mkdir(parents=True, exist_ok=True)

    async def submit(self, task: Task) -> str:
        job_id = task.task_id
        task_dir = self.workspace_root / job_id
        
        self._jobs[job_id] = JobStatus(job_id, JobState.PENDING)

        if self.dry_run:
            print(f"[DRY-RUN] mkdir -p {task_dir}")
            self._stage_files_dry_run(task, task_dir)
            print(f"[DRY-RUN] cd {task_dir} && {task.command}")
            # In dry-run, we just mark it as COMPLETED for workflow progression simulation
            self._jobs[job_id] = JobStatus(job_id, JobState.COMPLETED, exit_code=0)
            return job_id

        # Real Execution
        try:
            # 1. Create task directory
            task_dir.mkdir(parents=True, exist_ok=True)

            # 2. Stage files
            self._stage_files(task, task_dir)

            # 3. Open log files
            stdout_path = task_dir / "stdout.log"
            stderr_path = task_dir / "stderr.log"
            
            # Open files for writing
            stdout_file = open(stdout_path, 'w')
            stderr_file = open(stderr_path, 'w')

            # 4. Execute
            # Merge environment
            env = {**os.environ, **task.env}
            
            process = await asyncio.create_subprocess_shell(
                task.command,
                cwd=str(task_dir),
                stdout=stdout_file,
                stderr=stderr_file,
                env=env
            )
            
            # Close file handles in parent (subprocess has them now)
            stdout_file.close()
            stderr_file.close()

            self._processes[job_id] = process
            self._jobs[job_id] = JobStatus(job_id, JobState.RUNNING)
            
            return job_id

        except Exception as e:
            logger.exception(f"Failed to submit task {job_id}")
            self._jobs[job_id] = JobStatus(job_id, JobState.FAILED, reason=str(e))
            return job_id

    def _stage_files(self, task: Task, task_dir: Path):
        """Write or copy files to the task directory."""
        for filename, content in task.files.items():
            dest_path = task_dir / filename
            # Ensure parent directory exists (for nested files)
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(content, Path):
                src_path = content
                if not src_path.exists():
                     raise FileNotFoundError(f"Input file not found: {src_path}")
                if src_path.is_dir():
                    if dest_path.exists():
                         shutil.rmtree(dest_path)
                    shutil.copytree(src_path, dest_path)
                else:
                    shutil.copy2(src_path, dest_path)
            elif isinstance(content, str):
                with open(dest_path, "w") as f:
                    f.write(content)
            else:
                logger.warning(f"Unknown content type for file {filename}: {type(content)}")

    def _stage_files_dry_run(self, task: Task, task_dir: Path):
        for filename, content in task.files.items():
            if isinstance(content, Path):
                 print(f"[DRY-RUN] cp {content} {task_dir}/{filename}")
            else:
                 print(f"[DRY-RUN] write string to {task_dir}/{filename} ({len(content)} chars)")

    async def poll(self, job_id: str) -> JobStatus:
        # Check if we have a process object
        process = self._processes.get(job_id)
        current_status = self._jobs.get(job_id)

        if not current_status:
             return JobStatus(job_id, JobState.UNKNOWN)

        # If already terminal, return it
        if current_status.state in [JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED]:
            return current_status

        if process:
            # Check if process has finished
            return_code = process.returncode
            if return_code is not None:
                # Process finished
                if return_code == 0:
                    state = JobState.COMPLETED
                else:
                    state = JobState.FAILED
                
                self._jobs[job_id] = JobStatus(job_id, state, exit_code=return_code)
            else:
                # Still running
                pass
        
        return self._jobs[job_id]

    async def download(self, job_id: str, remote_path: str, local_path: str) -> None:
        """
        Download files from the local job workspace.
        """
        task_dir = self.workspace_root / job_id
        src = task_dir if remote_path == "." else (task_dir / remote_path)
        dst = Path(local_path)

        if not src.exists():
            raise FileNotFoundError(f"Remote path {src} does not exist for job {job_id}")

        if src.is_dir():
            if dst.exists():
                dst = dst / src.name
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            if dst.is_dir():
                dst = dst / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    async def cancel(self, job_id: str) -> None:
        process = self._processes.get(job_id)
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
            
            self._jobs[job_id] = JobStatus(job_id, JobState.CANCELLED)

    async def get_logs(self, job_id: str) -> Dict[str, str]:
        task_dir = self.workspace_root / job_id
        stdout_path = task_dir / "stdout.log"
        stderr_path = task_dir / "stderr.log"
        
        return {
            "stdout": stdout_path.read_text() if stdout_path.exists() else "",
            "stderr": stderr_path.read_text() if stderr_path.exists() else ""
        }