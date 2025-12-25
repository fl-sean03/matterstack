from __future__ import annotations

import fnmatch
import shlex
from pathlib import Path
from typing import Any, Dict, List, Optional

from ....core.backend import ComputeBackend, JobStatus
from ....core.workflow import Task
from .._file_staging import classify_file_entry
from .slurm import get_job_io_paths, get_job_status, submit_job
from .ssh import SSHClient, SSHConfig


class SlurmBackend(ComputeBackend):
    """
    HPC Backend using Slurm over SSH.
    """

    @property
    def is_local_execution(self) -> bool:
        """Return False - this backend executes tasks remotely via SSH."""
        return False

    def __init__(self, ssh_config: SSHConfig, workspace_root: str, slurm_config: Optional[Dict[str, Any]] = None):
        self.ssh_config = ssh_config
        self.workspace_root = workspace_root
        self.slurm_config = slurm_config or {}
        self._client: Optional[SSHClient] = None

    async def _get_client(self) -> SSHClient:
        if self._client is None:
            self._client = await SSHClient.connect(self.ssh_config)
        return self._client

    async def _execute_with_retry(self, func, *args, **kwargs):
        """Execute a backend function with auto-reconnection on failure."""
        try:
            return await func(*args, **kwargs)
        except RuntimeError as e:
            if "SSH connection lost" in str(e) or "Socket is closed" in str(e) or "not connected" in str(e):
                # Reset client and retry once
                await self.close()
                return await func(*args, **kwargs)
            raise e

    async def submit(
        self, task: Task, workdir_override: Optional[str] = None, local_debug_dir: Optional[Path] = None
    ) -> str:
        return await self._execute_with_retry(self._submit_impl, task, workdir_override, local_debug_dir)

    async def _submit_impl(
        self, task: Task, workdir_override: Optional[str] = None, local_debug_dir: Optional[Path] = None
    ) -> str:
        client = await self._get_client()

        # 1. Prepare workspace
        if workdir_override:
            task_dir = workdir_override
        else:
            task_dir = f"{self.workspace_root}/{task.task_id}"

        await client.mkdir_p(task_dir)

        # 2. Upload files using shared staging utility
        for name, content_or_path in task.files.items():
            remote_path = f"{task_dir}/{name}"
            staged = classify_file_entry(name, content_or_path)

            if staged.is_path_based:
                # Upload from local path (file or directory)
                await client.put(str(staged.source_path), remote_path, recursive=True)
            else:
                # Write content directly
                await client.write_text(remote_path, staged.content)

        # 3. Create batch script
        batch_script = self._generate_batch_script(task, task_dir)

        # Save locally for debugging if requested
        if local_debug_dir:
            try:
                local_debug_dir.mkdir(parents=True, exist_ok=True)
                local_script_path = local_debug_dir / "submit.sh"
                local_script_path.write_text(batch_script)
            except Exception as e:
                # Don't fail the run if local write fails, just log/warn
                print(f"WARNING: Failed to save local debug script: {e}")

        script_path = f"{task_dir}/submit.sh"
        await client.write_text(script_path, batch_script)

        # 4. Submit
        job_id = await submit_job(client, task_dir, "submit.sh")
        return job_id

    def _generate_batch_script(self, task: Task, task_dir: str) -> str:
        """Generate the Slurm batch script content."""
        lines = ["#!/bin/bash"]

        # Track which directives have been explicitly set by the Task
        # so we don't override them with global defaults.
        configured_directives = set()

        # 1. Task-specific attributes (Higher Precedence)
        lines.append(f"#SBATCH --job-name={task.task_id}")
        configured_directives.add("job-name")

        if task.time_limit_minutes is not None:
            lines.append(f"#SBATCH --time={task.time_limit_minutes}")
            configured_directives.add("time")

        if task.cores is not None:
            lines.append(f"#SBATCH --cpus-per-task={task.cores}")
            configured_directives.add("cpus-per-task")

        if task.memory_gb is not None:
            lines.append(f"#SBATCH --mem={task.memory_gb}G")
            configured_directives.add("mem")

        lines.append(f"#SBATCH --output={task_dir}/stdout.log")
        lines.append(f"#SBATCH --error={task_dir}/stderr.log")
        # output/error are usually always task-specific, but we track them anyway
        configured_directives.add("output")
        configured_directives.add("error")

        if task.gpus is not None and task.gpus > 0:
            lines.append(f"#SBATCH --gres=gpu:{task.gpus}")
            configured_directives.add("gres")

        # 2. Global Defaults (Lower Precedence)
        # Added "mem" and "gres" to the supported list
        supported_keys = ["account", "partition", "qos", "ntasks", "cpus-per-task", "nodes", "time", "mem", "gres"]

        for key in supported_keys:
            if key in configured_directives:
                continue

            if val := self.slurm_config.get(key):
                lines.append(f"#SBATCH --{key}={val}")
                configured_directives.add(key)

        lines.append("")

        # Load modules
        if modules := self.slurm_config.get("modules"):
            lines.extend(modules)

        lines.append("")

        # Export env vars
        for k, v in task.env.items():
            lines.append(f"export {k}={shlex.quote(str(v))}")

        lines.append("")
        lines.append(f"cd {task_dir}")

        # Always write an exit_code file into the remote task dir.
        #
        # We prefer an EXIT trap over wrapping the command so that:
        # - the file is written even if the command fails
        # - the script can evolve to use `set -e` without breaking exit_code capture
        exit_code_path = f"{task_dir}/exit_code"
        lines.append(f"EXIT_CODE_FILE={shlex.quote(exit_code_path)}")
        lines.append("trap 'ec=$?; echo $ec > \"$EXIT_CODE_FILE\"' EXIT")

        # Execute command
        if task.image:
            lines.append(f"# Image: {task.image} (Logic to be implemented)")

        # Ensure python is run from the active conda environment
        # If task.command starts with "python", we might want to ensure it uses the env python
        # But generally, if conda is activated, 'python' in PATH should be correct.

        # Add conda hook for shell activation if not present in modules
        # This is a bit heuristic, but safer for CURC/anaconda usage
        if any("anaconda" in m or "miniforge" in m for m in self.slurm_config.get("modules", [])):
            lines.insert(-1, 'eval "$(conda shell.bash hook)"')
            # Note: Activation should ideally happen in 'modules' config (e.g. 'conda activate base')
            # But the hook is needed for 'conda activate' to work in script.

        lines.append('echo "Job started on $(hostname)"')
        lines.append('echo "Python: $(which python3)"')
        lines.append(task.command)

        script_content = "\n".join(lines) + "\n"
        print(f"DEBUG: Generated Batch Script:\n{script_content}")
        return script_content

    async def poll(self, job_id: str) -> JobStatus:
        return await self._execute_with_retry(self._poll_impl, job_id)

    async def _poll_impl(self, job_id: str) -> JobStatus:
        client = await self._get_client()
        return await get_job_status(client, job_id)

    async def cancel(self, job_id: str) -> None:
        await self._execute_with_retry(self._cancel_impl, job_id)

    async def _cancel_impl(self, job_id: str) -> None:
        client = await self._get_client()
        await client.run(f"scancel {job_id}")

    async def get_logs(self, job_id: str) -> Dict[str, str]:
        return await self._execute_with_retry(self._get_logs_impl, job_id)

    async def _get_logs_impl(self, job_id: str) -> Dict[str, str]:
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
                return content_bytes.decode("utf-8", errors="replace")
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

    async def download(
        self,
        job_id: str,
        remote_path: str,
        local_path: str,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        workdir_override: Optional[str] = None,
    ) -> None:
        return await self._execute_with_retry(
            self._download_impl,
            job_id,
            remote_path,
            local_path,
            include_patterns,
            exclude_patterns,
            workdir_override,
        )

    async def _download_impl(
        self,
        job_id: str,
        remote_path: str,
        local_path: str,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        workdir_override: Optional[str] = None,
    ) -> None:
        """
        Download a file or directory from the job's workspace.

        Note: For the Slurm backend, `job_id` is historically the MatterStack task_id
        used to construct the default remote workspace path when `workdir_override`
        is not provided.
        """
        client = await self._get_client()

        # Resolve base task dir
        if workdir_override:
            task_dir = workdir_override
        else:
            task_dir = f"{self.workspace_root}/{job_id}"

        # Resolve full remote path
        if remote_path.startswith("/"):
            full_remote = remote_path
        else:
            if remote_path == ".":
                full_remote = task_dir
            else:
                full_remote = f"{task_dir}/{remote_path}"

        # Define filter callback
        def _should_download(path: str) -> bool:
            # path is the absolute remote path of the file
            # We need to match against the relative path from the download root (full_remote)
            # This mimics rsync include/exclude logic relative to transfer root

            try:
                rel_path = Path(path).relative_to(full_remote)
                rel_path_str = str(rel_path)
            except ValueError:
                # Should not happen if SSHClient logic is correct
                rel_path_str = Path(path).name

            should = True

            if include_patterns:
                # If include patterns exist, defaults to exclude unless matched
                # (Standard rsync behavior is complex, but here simplistic:
                # if include_patterns provided, we require a match)
                if not any(fnmatch.fnmatch(rel_path_str, p) for p in include_patterns):
                    should = False

            if exclude_patterns:
                if any(fnmatch.fnmatch(rel_path_str, p) for p in exclude_patterns):
                    should = False

            return should

        filter_cb = _should_download if (include_patterns or exclude_patterns) else None

        await client.get(full_remote, local_path, recursive=True, filter_callback=filter_cb)

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
