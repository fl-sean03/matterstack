from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, Field


class RunHandle(BaseModel):
    """
    Handle identifying a specific execution run.
    Stores location and identity information.
    """

    workspace_slug: str
    run_id: str
    root_path: Path

    @property
    def db_path(self) -> Path:
        return self.root_path / "state.sqlite"

    @property
    def config_path(self) -> Path:
        return self.root_path / "config.json"

    @property
    def operators_path(self) -> Path:
        return self.root_path / "operators"

    @property
    def evidence_path(self) -> Path:
        return self.root_path / "evidence"


class RunMetadata(BaseModel):
    """
    Metadata associated with a run.
    """

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "active"
    tags: Dict[str, str] = Field(default_factory=dict)
    description: Optional[str] = None
