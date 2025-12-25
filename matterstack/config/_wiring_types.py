"""
Internal types and dataclasses for operator wiring.

This module contains the core type definitions used throughout the operator wiring system:
- OperatorWiringSource: Enum representing the source of wiring configuration
- ResolvedOperatorWiring: Result of resolving operator wiring for a run
- OperatorWiringProvenance: Lightweight provenance view for diagnostics
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

# Environment variable name for operators config path
_ENV_OPERATORS_CONFIG = "MATTERSTACK_OPERATORS_CONFIG"


class OperatorWiringSource(str, Enum):
    """Source of operator wiring configuration."""

    CLI_OVERRIDE = "CLI_OVERRIDE"
    RUN_PERSISTED = "RUN_PERSISTED"
    WORKSPACE_DEFAULT = "WORKSPACE_DEFAULT"
    ENV_VAR = "ENV_VAR"
    LEGACY_PROFILE = "LEGACY_PROFILE"
    LEGACY_HPC_CONFIG = "LEGACY_HPC_CONFIG"
    NONE = "NONE"


@dataclass(frozen=True)
class ResolvedOperatorWiring:
    """
    Result of resolving operator wiring for a run.

    - `snapshot_path` is the authoritative path used to build the operator registry
      (when present), and should always point to `<run_root>/operators_snapshot/operators.yaml`.
    - `resolved_path` records the origin path used to produce the snapshot (CLI/workspace/env),
      or may point at the snapshot itself for RUN_PERSISTED resolutions.
    """

    source: OperatorWiringSource
    resolved_path: Optional[str]
    sha256: Optional[str]

    snapshot_path: Optional[str]

    snapshot_dir: str
    metadata_path: str
    history_path: str

    is_persisted: bool
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OperatorWiringProvenance:
    """
    Lightweight wiring provenance view loaded from `<run_root>/operators_snapshot/metadata.json`.

    This is intended for diagnostics (`explain`, etc.), and must be resilient:
    missing files or malformed JSON should not crash the caller.
    """

    source: str
    sha256: Optional[str]
    snapshot_relpath: Optional[str]

    # Optional/diagnostic-only fields (not required for explain output).
    resolved_path: Optional[str] = None
    created_at_utc: Optional[str] = None
