from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ExternalStatus(str, Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    WAITING = "WAITING"


class BaseManifest(BaseModel):
    """Base model for all manifests to ensure basic consistency."""

    pass


class HumanResponseManifest(BaseManifest):
    """
    Schema for the response.json file provided by a human operator.
    """

    status: ExternalStatus
    data: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class ManualHPCStatusManifest(BaseManifest):
    """
    Schema for the status.json file provided by a manual HPC operator.
    """

    status: ExternalStatus
    error: Optional[str] = None


class ExperimentResultManifest(BaseManifest):
    """
    Schema for the experiment_result.json file provided by lab control software.
    """

    status: ExternalStatus
    data: Dict[str, Any] = Field(default_factory=dict)
    files: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class ExperimentRequestManifest(BaseManifest):
    """
    Schema for the experiment_request.json file generated for lab control software.
    """

    task_id: str
    parameters: Dict[str, Any]
    files: List[str] = Field(default_factory=list)
    config: Optional[Dict[str, Any]] = None
    config_raw: Optional[str] = None
