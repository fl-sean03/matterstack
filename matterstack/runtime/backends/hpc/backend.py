from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Any, List
from ....core.backend import ComputeBackend, JobStatus, JobState
from ....core.workflow import Task
from .ssh import SSHClient, SSHConfig
from .slurm import submit_job, get_job_status, get_job_io_paths

class SlurmBackend(ComputeBackend):
    """
    HPC Backend using Slurm over SSH.
    """

    def __init__(self, ssh_config: SSHConfig, workspace_root: str, slurm_config: Optional[Dict[str, Any]] = None):
        self.ssh_config = ssh_config
        self.workspace_root = workspace_root
        self.slurm_config = slurm_config or {}
        self._client: Optional[SSHClient] = None

    async def _get_client(self) -> SSHClient:
        if self._client is None:
            self._client = await SSHClient.connect(self.ssh_config)
        return self._client

    async def submit(self, task: Task) -> str:
        client = await self._get_client()
        
        # 1. Prepare workspace
        task_dir = f"{self.workspace_root}/{task.task_id}"
        await client.mkdir_p(task_dir)

        # 2. Upload files
        for name, content_or_path in task.files.items():
            remote_path = f"{task_dir}/{name}"
            
            if isinstance(content_or_path, Path):
                # Upload file or directory recursively
                await client.put(str(content_or_path), remote_path, recursive=True)
            else:
                # Assume string content
                await client.write_text(remote_path, str(content_or_path))

        # 3. Create batch script
        batch_script = self._generate_batch_script(task, task_dir)
        script_path = f"{task_dir}/submit.sh"
        await client.write_text(script_path, batch_script)
        
        # 4. Submit
        job_id = await submit_job(client, task_dir, "submit.sh")
        return job_id

    def _generate_batch_script(self, task: Task, task_dir: str) -> str:
        """Generate the Slurm batch script content."""
        lines = ["#!/bin/bash"]
        lines.append(f"#SBATCH --job-name={task.task_id}")
        lines.append(f"#SBATCH --time={task.time_limit_minutes}")
        lines.append(f"#SBATCH --cpus-per-task={task.cores}")
        lines.append(f"#SBATCH --mem={task.memory_gb}G")
        lines.append(f"#SBATCH --output={task_dir}/stdout.log")
        lines.append(f"#SBATCH --error={task_dir}/stderr.log")
        
        if task.gpus > 0:
            lines.append(f"#SBATCH --gres=gpu:{task.gpus}")
        
        # Add Slurm specific config
        for key in ["account", "partition", "qos"]:
            if val := self.slurm_config.get(key):
                lines.append(f"#SBATCH --{key}={val}")

        lines.append("")

        # Load modules
        if modules := self.slurm_config.get("modules"):
            lines.extend(modules)
        
        lines.append("")
        
        # Export env vars
        for k, v in task.env.items():
            lines.append(f"export {k}={v}")
        
        lines.append("")
        lines.append(f"cd {task_dir}")
        
        # Execute command
        if task.image:
             lines.append(f"# Image: {task.image} (Logic to be implemented)")
             
        lines.append(task.command)
        
        return "\n".join(lines) + "\n"

    async def poll(self, job_id: str) -> JobStatus:
        client = await self._get_client()
        return await get_job_status(client, job_id)

    async def cancel(self, job_id: str) -> None:
        client = await self._get_client()
        await client.run(f"scancel {job_id}")

    async def get_logs(self, job_id: str) -> Dict[str, str]:
        client = await self._get_client()
        
        paths = await get_job_io_paths(client, job_id)
        
        logs = {"stdout": "", "stderr": ""}
        
        async def _read_safe(p: str) -> str:
            # Handle potential relative paths if WorkDir is known, but usually Slurm gives absolute or relative to WorkDir.
            # If path is relative and we have WorkDir, join them.
            # But let's assume absolute or relative to CWD (which might be confusing if CWD changed).
            # Simplest check: does it start with /?
            
            full_path = p
            if "workdir" in paths and not p.startswith("/"):
                full_path = f"{paths['workdir']}/{p}"
            
            try:
                # Read bytes and decode
                content_bytes = await client.read_bytes(full_path)
                return content_bytes.decode('utf-8', errors='replace')
            except Exception:
                return ""

        if "stdout" in paths:
            logs["stdout"] = await _read_safe(paths["stdout"])
        
        if "stderr" in paths:
            # If stderr is same as stdout (often default), just reuse
            if paths.get("stderr") == paths.get("stdout"):
                logs["stderr"] = logs["stdout"]
            else:
                logs["stderr"] = await _read_safe(paths["stderr"])

        return logs
        
    async def download(self, task_id: str, remote_path: str, local_path: str) -> None:
        """
        Download a file or directory from the task's workspace.
        
        Args:
            task_id: The ID of the task/job.
            remote_path: Relative path within the task directory (or absolute).
                         Use "." to download the entire task workspace.
            local_path: Destination path on the local machine.
        """
        client = await self._get_client()
        
        # Resolve full remote path
        if remote_path.startswith("/"):
            full_remote = remote_path
        else:
            full_remote = f"{self.workspace_root}/{task_id}/{remote_path}"
            
        await client.get(full_remote, local_path, recursive=True)

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
