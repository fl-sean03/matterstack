from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional, List
from pathlib import Path

from pydantic import BaseModel, Field

from matterstack.core.run import RunHandle

class ExternalRunStatus(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"

class ExternalRunHandle(BaseModel):
    """
    Handle for a task execution managed by an Operator.
    """
    task_id: str
    operator_type: str
    external_id: Optional[str] = None
    status: ExternalRunStatus = ExternalRunStatus.CREATED
    operator_data: Dict[str, Any] = Field(default_factory=dict)
    
    # Path relative to run root where operator data is stored
    relative_path: Optional[Path] = None

class OperatorResult(BaseModel):
    """
    Result returned by an operator after successful execution.
    """
    task_id: str
    status: ExternalRunStatus
    files: Dict[str, Path] = Field(default_factory=dict)  # Output files
    data: Dict[str, Any] = Field(default_factory=dict)    # Structured data
    error_message: Optional[str] = None

class Operator(ABC):
    """
    Abstract interface for executing tasks on external systems.
    """
    
    @abstractmethod
    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        """
        Prepare the execution environment (directories, scripts).
        """
        pass

    @abstractmethod
    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Submit the work to the external system.
        """
        pass

    @abstractmethod
    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        """
        Check the current status of the external execution.
        """
        pass

    @abstractmethod
    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        """
        Retrieve results after completion.
        """
        pass