from __future__ import annotations
import os
import shlex
import shutil
import asyncio
import subprocess
import logging
import json
import dataclasses
from typing import Dict, Optional, Union, Any, List
from pathlib import Path
import fnmatch

from ...core.backend import ComputeBackend, JobStatus, JobState
from ...core.workflow import Task, FileFromPath, FileFromContent

logger = logging.getLogger(__name__)

class LocalBackend(ComputeBackend):
    """
    Local Backend for executing Tasks using subprocess.Popen.
    Supports a "dry_run" mode for verification.
    """

    def __init__(self, workspace_root: str = "workspace", dry_run: bool = False):
        self.workspace_root = Path(workspace_root).resolve()
        self.dry_run = dry_run
        self.state_file = self.workspace_root / "local_backend_state.json"
        
        # Map job_id -> JobStatus
        self._jobs: Dict[str, JobStatus] = {}
        # Map job_id -> task_dir (path)
        self._job_paths: Dict[str, str] = {}
        # Map job_id -> subprocess.Popen
        self._processes: Dict[str, subprocess.Popen] = {}

        if not self.dry_run:
            self.workspace_root.mkdir(parents=True, exist_ok=True)
            self._load_state()

    def _load_state(self):
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text())
                # Handle both legacy format (dict of status) and new format (dict with 'jobs' and 'paths')
                if "jobs" in data and "paths" in data:
                    jobs_data = data["jobs"]
                    self._job_paths = data["paths"]
                else:
                    jobs_data = data # Legacy assumption
                    self._job_paths = {}

                for job_id, status_data in jobs_data.items():
                    # Reconstruct JobStatus
                    self._jobs[job_id] = JobStatus(
                        job_id=status_data["job_id"],
                        state=JobState(status_data["state"]),
                        exit_code=status_data.get("exit_code"),
                        reason=status_data.get("reason")
                    )
            except Exception as e:
                logger.warning(f"Failed to load local backend state: {e}")

    def _save_state(self):
        if self.dry_run:
            return
        try:
            jobs_data = {}
            for job_id, status in self._jobs.items():
                jobs_data[job_id] = {
                    "job_id": status.job_id,
                    "state": status.state.value,
                    "exit_code": status.exit_code,
                    "reason": status.reason
                }
            
            data = {
                "jobs": jobs_data,
                "paths": self._job_paths
            }
            self.state_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save local backend state: {e}")

    async def submit(
        self,
        task: Task,
        workdir_override: Optional[str] = None,
        local_debug_dir: Optional[Path] = None,
    ) -> str:
        job_id = task.task_id
        
        if workdir_override:
            task_dir = Path(workdir_override).resolve()
        else:
            task_dir = self.workspace_root / job_id
        
        self._job_paths[job_id] = str(task_dir)
        self._jobs[job_id] = JobStatus(job_id, JobState.QUEUED)
        self._save_state()

        if self.dry_run:
            print(f"[DRY-RUN] mkdir -p {task_dir}")
            self._stage_files_dry_run(task, task_dir)
            print(f"[DRY-RUN] cd {task_dir} && {task.command}")
            # In dry-run, we just mark it as COMPLETED for workflow progression simulation
            self._jobs[job_id] = JobStatus(job_id, JobState.COMPLETED_OK, exit_code=0)
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
            
            # Wrap command to capture exit code
            # We use a subshell to ensure exit code is captured even if command fails
            # Use absolute path for exit_code file to be safe
            exit_code_path = task_dir / "exit_code"
            wrapped_command = f"({task.command}); echo $? > {exit_code_path}"
            
            # Use subprocess.Popen instead of asyncio for robustness in sync-wrapped contexts
            logger.info(f"Executing command in {task_dir}: {wrapped_command}")
            try:
                process = subprocess.Popen(
                    wrapped_command,
                    shell=True,
                    cwd=str(task_dir),
                    stdout=stdout_file,
                    stderr=stderr_file,
                    env=env
                )
                logger.info(f"Process started with PID {process.pid}")
            except Exception as e:
                logger.error(f"Failed to start subprocess: {e}")
                raise
            
            # Close file handles in parent
            stdout_file.close()
            stderr_file.close()

            self._processes[job_id] = process
            self._jobs[job_id] = JobStatus(job_id, JobState.RUNNING)
            self._save_state()
            
            return job_id

        except Exception as e:
            logger.exception(f"Failed to submit task {job_id}")
            self._jobs[job_id] = JobStatus(job_id, JobState.COMPLETED_ERROR, reason=str(e))
            self._save_state()
            return job_id

    def _stage_files(self, task: Task, task_dir: Path):
        """Write or copy files to the task directory."""
        for filename, content_or_path in task.files.items():
            dest_path = task_dir / filename
            # Ensure parent directory exists (for nested files)
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Helper to copy from path
            def copy_from_path(src_path: Path):
                if not src_path.exists():
                     raise FileNotFoundError(f"Input file not found: {src_path}")
                if src_path.is_dir():
                    if dest_path.exists():
                         shutil.rmtree(dest_path)
                    shutil.copytree(src_path, dest_path)
                else:
                    shutil.copy2(src_path, dest_path)

            if isinstance(content_or_path, FileFromPath):
                copy_from_path(content_or_path.source_path)
            elif isinstance(content_or_path, FileFromContent):
                with open(dest_path, "w") as f:
                    f.write(content_or_path.content)
            elif isinstance(content_or_path, Path):
                copy_from_path(content_or_path)
            elif isinstance(content_or_path, str):
                # Legacy heuristic
                # Check if it looks like a path AND exists
                is_likely_path = len(content_or_path) > 0 and len(content_or_path) < 1024 and "\n" not in content_or_path
                if is_likely_path and Path(content_or_path).exists():
                     copy_from_path(Path(content_or_path))
                else:
                     with open(dest_path, "w") as f:
                        f.write(content_or_path)
            else:
                logger.warning(f"Unknown content type for file {filename}: {type(content_or_path)}")

    def _stage_files_dry_run(self, task: Task, task_dir: Path):
        for filename, content_or_path in task.files.items():
            if isinstance(content_or_path, FileFromPath):
                 print(f"[DRY-RUN] cp {content_or_path.source_path} {task_dir}/{filename}")
            elif isinstance(content_or_path, FileFromContent):
                 print(f"[DRY-RUN] write string to {task_dir}/{filename} ({len(content_or_path.content)} chars)")
            elif isinstance(content_or_path, Path):
                 print(f"[DRY-RUN] cp {content_or_path} {task_dir}/{filename}")
            elif isinstance(content_or_path, str):
                 is_likely_path = len(content_or_path) > 0 and len(content_or_path) < 1024 and "\n" not in content_or_path
                 if is_likely_path and Path(content_or_path).exists():
                      print(f"[DRY-RUN] cp {content_or_path} {task_dir}/{filename}")
                 else:
                      print(f"[DRY-RUN] write string to {task_dir}/{filename} ({len(content_or_path)} chars)")
            else:
                 print(f"[DRY-RUN] Unknown type for {filename}: {type(content_or_path)}")

    async def poll(self, job_id: str) -> JobStatus:
        current_status = self._jobs.get(job_id)

        if not current_status:
             return JobStatus(job_id, JobState.UNKNOWN)

        # If already terminal, return it
        if current_status.state in [JobState.COMPLETED_OK, JobState.COMPLETED_ERROR, JobState.CANCELLED]:
            return current_status

        # Determine task dir
        if job_id in self._job_paths:
            task_dir = Path(self._job_paths[job_id])
        else:
            task_dir = self.workspace_root / job_id

        # Check for exit_code file in task dir
        exit_code_file = task_dir / "exit_code"
        
        if exit_code_file.exists():
            try:
                exit_code = int(exit_code_file.read_text().strip())
                if exit_code == 0:
                    state = JobState.COMPLETED_OK
                else:
                    state = JobState.COMPLETED_ERROR
                
                self._jobs[job_id] = JobStatus(job_id, state, exit_code=exit_code)
                self._save_state()
                return self._jobs[job_id]
            except:
                pass

        # Fallback to process object if available (for immediate feedback)
        process = self._processes.get(job_id)
        if process:
            return_code = process.poll()
            if return_code is not None:
                if return_code == 0:
                    state = JobState.COMPLETED_OK
                else:
                    state = JobState.COMPLETED_ERROR
                self._jobs[job_id] = JobStatus(job_id, state, exit_code=return_code)
                self._save_state()

        return self._jobs[job_id]

    async def download(
        self,
        job_id: str,
        remote_path: str,
        local_path: str,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        workdir_override: Optional[str] = None
    ) -> None:
        """
        Download files from the local job workspace.
        """
        if workdir_override:
            task_dir = Path(workdir_override).resolve()
        else:
            task_dir = self.workspace_root / job_id
            
        src = task_dir if remote_path == "." else (task_dir / remote_path)
        dst = Path(local_path)

        if not src.exists():
            raise FileNotFoundError(f"Remote path {src} does not exist for job {job_id}")

        def _ignore_patterns(path: str, names: List[str]) -> List[str]:
            """Callback for shutil.copytree to filter files."""
            ignored = set()
            rel_path = Path(path).relative_to(src)
            
            for name in names:
                # Construct relative path for the file/dir
                # Note: copytree passes the directory path as first arg, and list of names
                # We need to match against the relative path from src root
                file_rel_path = rel_path / name
                
                # Check inclusion (if specified, file MUST match at least one pattern)
                if include_patterns:
                    # If it's a directory, we generally want to traverse it unless it's explicitly excluded?
                    # But shutil.ignore expects us to return what to IGNORE.
                    # If include_patterns is set, we ignore everything that DOESN'T match.
                    # HOWEVER, for directories, we must be careful not to prune the tree too early.
                    # Simple approach: If it is a directory, don't ignore it based on include patterns
                    # (unless we have a specific dir match logic, but here we assume patterns are for files).
                    # Actually, we should probably check if any pattern *could* match inside.
                    # For simplicity: Include matches for files. For dirs, we keep them to traverse.
                    
                    is_dir = (Path(path) / name).is_dir()
                    if not is_dir:
                        # It's a file. Does it match any include pattern?
                        # fnmatch works on names or paths? We usually match against relative path.
                        matched = any(fnmatch.fnmatch(str(file_rel_path), p) for p in include_patterns)
                        if not matched:
                            ignored.add(name)
                
                # Check exclusion
                if exclude_patterns:
                    # If matches any exclude pattern, ignore it
                    if any(fnmatch.fnmatch(str(file_rel_path), p) for p in exclude_patterns):
                        ignored.add(name)
            
            return list(ignored)

        if src.is_dir():
            # If dst exists and is a dir, we usually copy INTO it.
            # But standard behavior here (based on previous impl) was:
            # if dst.exists(): dst = dst / src.name
            # shutil.copytree(src, dst, dirs_exist_ok=True)
            
            # The previous logic had a slight quirk: if dst exists, it appends src name.
            # Let's preserve that.
            if dst.exists() and dst.is_dir():
                dst = dst / src.name

            # Use ignore callback if patterns are provided
            ignore_func = _ignore_patterns if (include_patterns or exclude_patterns) else None
            
            shutil.copytree(src, dst, dirs_exist_ok=True, ignore=ignore_func)
            
        else:
            # It's a single file. Check patterns.
            # Since we are downloading a specific file, include/exclude might seem redundant
            # but we should still respect them if passed.
            
            rel_name = src.name # For single file, relative path is just the name if we consider parent?
            # Or is it relative to task_dir?
            # If remote_path pointed to a file, that IS the target.
            
            should_download = True
            if include_patterns:
                if not any(fnmatch.fnmatch(rel_name, p) for p in include_patterns):
                    should_download = False
            if exclude_patterns:
                if any(fnmatch.fnmatch(rel_name, p) for p in exclude_patterns):
                    should_download = False
            
            if should_download:
                if dst.is_dir():
                    dst = dst / src.name
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

    async def cancel(self, job_id: str) -> None:
        process = self._processes.get(job_id)
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
            
            self._jobs[job_id] = JobStatus(job_id, JobState.CANCELLED)
            self._save_state()

    async def get_logs(self, job_id: str) -> Dict[str, str]:
        task_dir = self.workspace_root / job_id
        stdout_path = task_dir / "stdout.log"
        stderr_path = task_dir / "stderr.log"
        
        return {
            "stdout": stdout_path.read_text() if stdout_path.exists() else "",
            "stderr": stderr_path.read_text() if stderr_path.exists() else ""
        }