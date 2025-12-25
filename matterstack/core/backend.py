from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from .workflow import Task


class JobState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED_OK = "COMPLETED_OK"
    COMPLETED_ERROR = "COMPLETED_ERROR"
    CANCELLED = "CANCELLED"
    LOST = "LOST"
    UNKNOWN = "UNKNOWN"


@dataclass
class JobStatus:
    job_id: str
    state: JobState
    exit_code: Optional[int] = None
    reason: Optional[str] = None


class ComputeBackend(ABC):
    """
    Abstract interface for executing Tasks on a compute resource.
    """

    @property
    @abstractmethod
    def is_local_execution(self) -> bool:
        """
        Return True if this backend executes tasks locally (same machine).

        Used by operators to determine path handling behavior:
        - Local backends: use local filesystem paths directly
        - Remote backends: use remote workspace paths and download results
        """
        pass

    @abstractmethod
    async def submit(
        self,
        task: Task,
        workdir_override: Optional[str] = None,
        local_debug_dir: Optional[Path] = None,
    ) -> str:
        """
        Submit a Task for execution.

        Args:
            task: The Task to submit.
            workdir_override: Optional override for the (remote) working directory.
                If provided, the backend should use this directory instead of constructing
                one from its default root + task_id.
            local_debug_dir: Optional local directory for best-effort debug artifacts
                generated during submission (e.g., the rendered submit.sh for HPC backends).

        Returns:
            The job ID.
        """
        pass

    @abstractmethod
    async def poll(self, job_id: str) -> JobStatus:
        """
        Check the status of a job.
        """
        pass

    @abstractmethod
    async def download(
        self,
        job_id: str,
        remote_path: str,
        local_path: str,
        include_patterns: Optional[list[str]] = None,
        exclude_patterns: Optional[list[str]] = None,
        workdir_override: Optional[str] = None,
    ) -> None:
        """
        Download a file or directory from the job's workspace.
        If remote_path is ".", download the entire workspace.

        Args:
            job_id: The ID of the job.
            remote_path: Path relative to job workspace.
            local_path: Local destination path.
            include_patterns: List of glob patterns to include (e.g., ["*.json", "results/*"]).
            exclude_patterns: List of glob patterns to exclude.
            workdir_override: Optional override for the remote working directory.
        """
        pass

    @abstractmethod
    async def cancel(self, job_id: str) -> None:
        """
        Cancel a running job.
        """
        pass

    @abstractmethod
    async def get_logs(self, job_id: str) -> Dict[str, str]:
        """
        Retrieve stdout and stderr for a job.
        Returns {'stdout': '...', 'stderr': '...'}
        """
        pass
