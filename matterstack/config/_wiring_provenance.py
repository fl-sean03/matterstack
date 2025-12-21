"""
Internal provenance handling for operator wiring.

This module contains functions for loading and formatting operator wiring provenance
information from run metadata files.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from ._wiring_types import OperatorWiringProvenance


def load_wiring_provenance_from_run_root(run_root: Path) -> Optional[OperatorWiringProvenance]:
    """
    Best-effort: load wiring provenance from `<run_root>/operators_snapshot/metadata.json`.

    Returns None if the metadata file is missing or unreadable.
    """
    try:
        meta_path = run_root / "operators_snapshot" / "metadata.json"
        if not meta_path.is_file():
            return None

        payload = json.loads(meta_path.read_text(encoding="utf-8") or "{}")
        effective = payload.get("effective") if isinstance(payload, dict) else None
        if not isinstance(effective, dict):
            return None

        source = effective.get("source")
        if not source:
            return None

        sha256 = effective.get("sha256")
        snapshot_relpath = effective.get("snapshot_relpath")

        created_at_utc = payload.get("created_at_utc") if isinstance(payload, dict) else None
        resolved_path = effective.get("resolved_path")

        return OperatorWiringProvenance(
            source=str(source),
            sha256=str(sha256) if sha256 is not None else None,
            snapshot_relpath=str(snapshot_relpath) if snapshot_relpath is not None else None,
            resolved_path=str(resolved_path) if resolved_path is not None else None,
            created_at_utc=str(created_at_utc) if created_at_utc is not None else None,
        )
    except Exception:
        return None


def format_operator_wiring_explain_line(run_root: Path) -> str:
    """
    Format a single stable, human-readable line for `matterstack explain`.

    Output (when present):
      Operator wiring: source=..., sha256=..., snapshot=operators_snapshot/operators.yaml

    Output (when absent):
      Operator wiring: none/unknown
    """
    prov = load_wiring_provenance_from_run_root(run_root)
    if prov is None:
        return "Operator wiring: none/unknown"

    if not prov.source or not prov.sha256 or not prov.snapshot_relpath:
        return "Operator wiring: none/unknown"

    return (
        f"Operator wiring: source={prov.source}, sha256={prov.sha256}, snapshot={prov.snapshot_relpath}"
    )
