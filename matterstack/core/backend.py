from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Dict
from dataclasses import dataclass
from enum import Enum
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

    @abstractmethod
    async def submit(self, task: Task) -> str:
        """
        Submit a Task for execution.
        Returns the job ID.
        """
        pass

    @abstractmethod
    async def poll(self, job_id: str) -> JobStatus:
        """
        Check the status of a job.
        """
        pass

    @abstractmethod
    async def download(self, job_id: str, remote_path: str, local_path: str) -> None:
        """
        Download a file or directory from the job's workspace.
        If remote_path is ".", download the entire workspace.
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
