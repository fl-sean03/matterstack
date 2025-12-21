"""
Config snapshot utilities for HPC operators.

This module provides functionality for creating attempt-scoped configuration
snapshots with deterministic hashing for reproducibility.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _compute_combined_config_hash(
    *,
    files_meta: List[Dict[str, Any]],
    missing_meta: List[Dict[str, Any]],
) -> str:
    """
    Deterministic combined hash over snapshot contents.

    Rules:
    - Per-file sha256 is computed over raw bytes.
    - Combined hash is sha256 over stable, sorted lines derived from snapshot metadata.
    """
    lines: List[str] = []

    for f in sorted(files_meta, key=lambda x: str(x.get("snapshot_path", ""))):
        lines.append(
            "FILE\t"
            + str(f.get("snapshot_path", ""))
            + "\t"
            + str(f.get("sha256", ""))
            + "\t"
            + str(f.get("size_bytes", ""))
            + "\n"
        )

    for m in sorted(missing_meta, key=lambda x: str(x.get("snapshot_path", ""))):
        lines.append(
            "MISSING\t"
            + str(m.get("snapshot_path", ""))
            + "\t"
            + str(m.get("source", ""))
            + "\n"
        )

    return _sha256_bytes("".join(lines).encode("utf-8"))


def write_attempt_config_snapshot(run_root: Path, attempt_dir: Path) -> Dict[str, Any]:
    """
    Create attempt-scoped config snapshot directory and compute deterministic config_hash.

    Snapshot directory:
        <attempt_dir>/config_snapshot/

    Files (best-effort; missing does not fail):
    - run_root/config.json              -> config.json
    - run_root/campaign_state.json      -> campaign_state.json
    - attempt_dir/manifest.json         -> task_manifest.json

    Writes:
    - config_snapshot/manifest.json (hash manifest)
    """
    snapshot_dir = attempt_dir / "config_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    expected = [
        (
            "config.json",
            run_root / "config.json",
            "run_root/config.json",
        ),
        (
            "campaign_state.json",
            run_root / "campaign_state.json",
            "run_root/campaign_state.json",
        ),
        (
            "task_manifest.json",
            attempt_dir / "manifest.json",
            "attempt/manifest.json",
        ),
    ]

    files_meta: List[Dict[str, Any]] = []
    missing_meta: List[Dict[str, Any]] = []

    for snapshot_name, src_path, src_label in expected:
        if src_path.exists() and src_path.is_file():
            data = src_path.read_bytes()
            dest_path = snapshot_dir / snapshot_name
            dest_path.write_bytes(data)

            files_meta.append(
                {
                    "snapshot_path": snapshot_name,
                    "source": src_label,
                    "sha256": _sha256_bytes(data),
                    "size_bytes": len(data),
                }
            )
        else:
            missing_meta.append(
                {
                    "snapshot_path": snapshot_name,
                    "source": src_label,
                }
            )

    combined_hash = _compute_combined_config_hash(files_meta=files_meta, missing_meta=missing_meta)

    hash_manifest = {
        "spec_version": 1,
        "files": files_meta,
        "missing": missing_meta,
        "combined_hash": combined_hash,
    }
    (snapshot_dir / "manifest.json").write_text(
        json.dumps(hash_manifest, indent=2, sort_keys=True) + "\n"
    )

    # This is what we persist in DB (small metadata only; no blobs).
    return {
        "config_hash": combined_hash,
        "config_snapshot": {
            "spec_version": 1,
            "relative_dir": "config_snapshot",
            "manifest_file": "config_snapshot/manifest.json",
            "files": files_meta,
            "missing": missing_meta,
        },
    }
