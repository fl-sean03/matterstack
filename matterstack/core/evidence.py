from __future__ import annotations
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timezone

from pydantic import BaseModel, Field

class EvidenceBundle(BaseModel):
    """
    A collection of data and files serving as evidence for a completed run.
    """
    run_id: str
    workspace_slug: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Run completion status
    run_status: str = "UNKNOWN"
    status_reason: Optional[str] = None
    is_complete: bool = False
    
    # Task statistics
    task_counts: Dict[str, int] = Field(default_factory=lambda: {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0
    })

    # Structured data results (e.g., computed properties)
    data: Dict[str, Any] = Field(default_factory=dict)
    
    # Paths to key artifacts (plots, log files)
    artifacts: Dict[str, Path] = Field(default_factory=dict)
    
    # Summary report content
    report_content: str = ""
    
    # Tags for indexing
    tags: List[str] = Field(default_factory=list)
    
    class Config:
        extra = "ignore"