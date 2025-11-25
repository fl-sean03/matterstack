from __future__ import annotations
import os
import asyncio
from typing import Optional, Dict, List, Any
from pathlib import PurePosixPath, Path
from dataclasses import dataclass

from matterstack.runtime.backends.hpc.ssh import SSHClient, CommandResult, SSHConfig

@dataclass
class JobInfo:
    job_id: str
    state: str  # PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    exit_code: str = "0:0"
    reason: str = "None"

class MockSSHClient:
    """
    Mock SSH Client that simulates file operations in memory and
    Slurm commands via simple state tracking.
    """
    def __init__(self, config: Optional[SSHConfig] = None):
        self.config = config
        # Remote filesystem: path -> bytes
        self.files: Dict[str, bytes] = {}
        # Job tracking: job_id -> JobInfo
        self.jobs: Dict[str, JobInfo] = {}
        self.job_counter = 1000
        self.cmds_executed: List[str] = []

    @classmethod
    async def connect(cls, config: SSHConfig) -> "MockSSHClient":
        return cls(config)

    async def close(self) -> None:
        pass

    async def run(self, command: str, *, cwd: Optional[str] = None) -> CommandResult:
        self.cmds_executed.append(command)
        
        # 1. Handle sbatch
        if command.startswith("sbatch "):
            # Format: sbatch script.sh
            # We assume success and return "Submitted batch job <id>"
            script_path = command.split(" ", 1)[1].strip()
            
            # Verify script exists (optional, but good for realism)
            full_path = self._resolve_path(script_path, cwd)
            if full_path not in self.files:
                 return CommandResult("", f"sbatch: error: script not found: {script_path}", 1)

            job_id = str(self.job_counter)
            self.job_counter += 1
            self.jobs[job_id] = JobInfo(job_id, "PENDING")
            
            return CommandResult(f"Submitted batch job {job_id}\n", "", 0)

        # 2. Handle sacct
        if command.startswith("sacct "):
            # We look for "-j <job_id>"
            parts = command.split()
            job_id = None
            for i, part in enumerate(parts):
                if part == "-j" and i + 1 < len(parts):
                    job_id = parts[i+1]
                    break
            
            if not job_id:
                # Maybe fallback or empty
                return CommandResult("", "", 0)
                
            if job_id not in self.jobs:
                return CommandResult("", "", 0) # Job not found in history

            info = self.jobs[job_id]
            # Format expected by _parse_sacct_line: JobID|State|ExitCode|Start|End|Elapsed
            # We mock dummy times
            line = f"{info.job_id}|{info.state}|{info.exit_code}|2023-01-01T00:00:00|2023-01-01T00:01:00|00:01:00"
            return CommandResult(line + "\n", "", 0)

        # 3. Handle squeue
        if command.startswith("squeue "):
             # We look for "-j <job_id>"
            parts = command.split()
            job_id = None
            for i, part in enumerate(parts):
                if part == "-j" and i + 1 < len(parts):
                    job_id = parts[i+1]
                    break
            
            if not job_id or job_id not in self.jobs:
                return CommandResult("", "", 0)
            
            info = self.jobs[job_id]
            # State mapping for squeue (simplified)
            short_state = {
                "PENDING": "PD",
                "RUNNING": "R",
                "COMPLETED": "CG", # or gone
                "FAILED": "F",
                "CANCELLED": "CA"
            }.get(info.state, "PD")

            # Format: %i|%T|%M|%R -> JobID|State|Time|Reason
            line = f"{info.job_id}|{short_state}|1:00|{info.reason}"
            return CommandResult(line + "\n", "", 0)

        # 4. Handle scancel
        if command.startswith("scancel "):
            parts = command.split()
            if len(parts) > 1:
                job_id = parts[1]
                if job_id in self.jobs:
                    self.jobs[job_id].state = "CANCELLED"
            return CommandResult("", "", 0)

        # 5. Handle scontrol (for checking paths)
        if command.startswith("scontrol show job"):
            parts = command.split()
            if len(parts) > 3:
                job_id = parts[3]
                # Mock response for get_job_io_paths
                # We assume standard paths relative to cwd or specific if we knew them
                # For now, return generic
                return CommandResult(f"JobId={job_id} StdOut=stdout.txt StdErr=stderr.txt WorkDir=/tmp/work", "", 0)

        # Default fallback
        return CommandResult("", "Mock command not handled", 127)

    async def mkdir_p(self, path: str) -> None:
        # In a map-based FS, directories are implicit, but we can track them if we want.
        # For now, do nothing.
        pass

    async def write_text(self, path: str, content: str) -> None:
        self.files[path] = content.encode("utf-8")

    async def read_bytes(self, path: str, *, offset: Optional[int] = None, max_bytes: Optional[int] = None) -> bytes:
        if path not in self.files:
            raise FileNotFoundError(f"Remote file not found: {path}")
        data = self.files[path]
        if offset:
            data = data[offset:]
        if max_bytes:
            data = data[:max_bytes]
        return data

    async def put(self, local_path: str, remote_path: str, recursive: bool = False) -> None:
        if not recursive:
            with open(local_path, "rb") as f:
                self.files[remote_path] = f.read()
        else:
            # Recursive upload
            base_local = Path(local_path)
            if not base_local.is_dir():
                 with open(local_path, "rb") as f:
                    self.files[remote_path] = f.read()
                 return

            # It's a directory
            for root, _, files in os.walk(local_path):
                rel_root = os.path.relpath(root, local_path)
                
                # Determine remote base for this root
                if rel_root == ".":
                     curr_remote_dir = remote_path
                else:
                     curr_remote_dir = str(PurePosixPath(remote_path) / rel_root)

                for f in files:
                    l_file = os.path.join(root, f)
                    r_file = str(PurePosixPath(curr_remote_dir) / f)
                    with open(l_file, "rb") as f_in:
                        self.files[r_file] = f_in.read()

    async def get(self, remote_path: str, local_path: str, recursive: bool = False) -> None:
        # Determine if remote_path acts as a file or directory
        is_exact_file = remote_path in self.files
        
        # Check if it has "children"
        prefix = remote_path if remote_path.endswith("/") else f"{remote_path}/"
        has_children = any(k.startswith(prefix) for k in self.files)

        if is_exact_file and not has_children:
            # It's a single file. Even if recursive=True, we treat it as file download
            # to mimic paramiko behavior on file targets.
            with open(local_path, "wb") as f:
                f.write(self.files[remote_path])
            return

        # Treat as directory download
        if not recursive:
             # If not recursive but matches a dir, paramiko would fail or download empty?
             # For now, if we think it's a dir, require recursive=True
             pass

        found_any = False
        for r_path, content in self.files.items():
            if r_path.startswith(prefix):
                found_any = True
                rel = r_path[len(prefix):] # Path inside dir
                
                l_file = os.path.join(local_path, rel)
                os.makedirs(os.path.dirname(l_file), exist_ok=True)
                with open(l_file, "wb") as f:
                    f.write(content)

    def _resolve_path(self, path: str, cwd: Optional[str]) -> str:
        if path.startswith("/") or not cwd:
            return path
        return str(PurePosixPath(cwd) / path)