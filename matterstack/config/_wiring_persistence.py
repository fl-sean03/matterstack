"""
Internal persistence functions for operator wiring.

This module contains functions for persisting and managing operator wiring snapshots,
including history tracking and metadata management.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ._wiring_types import OperatorWiringSource, _ENV_OPERATORS_CONFIG


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hash of bytes data."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _ensure_explicit_path_exists(path: Path, *, what: str) -> None:
    """Raise FileNotFoundError if the path does not exist."""
    if not path.is_file():
        raise FileNotFoundError(f"{what} file not found: {path}")


def _snapshot_paths(run_root: Path) -> Tuple[Path, Path, Path, Path]:
    """
    Return the standard paths for operator wiring snapshot files.

    Returns: (snapshot_dir, snapshot_yaml, metadata_json, history_jsonl)
    """
    snap_dir = run_root / "operators_snapshot"
    snap_yaml = snap_dir / "operators.yaml"
    meta_json = snap_dir / "metadata.json"
    hist_jsonl = snap_dir / "history.jsonl"
    return snap_dir, snap_yaml, meta_json, hist_jsonl


def _append_history(
    history_path: Path,
    *,
    run_root: Path,
    event: str,
    source: OperatorWiringSource,
    sha256: Optional[str],
    resolved_path: Optional[str],
    snapshot_path: Optional[Path],
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Append an event to the history.jsonl file."""
    history_path.parent.mkdir(parents=True, exist_ok=True)
    line = {
        "at_utc": _utc_now_iso(),
        "event": event,
        "source": str(source.value),
        "sha256": sha256,
        "resolved_path": resolved_path,
        "snapshot_relpath": str(snapshot_path.relative_to(run_root)) if snapshot_path else None,
        "details": details or {},
    }
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, sort_keys=True) + "\n")


def _write_metadata(
    metadata_path: Path,
    *,
    run_root: Path,
    source: OperatorWiringSource,
    resolved_path: Optional[str],
    sha256: Optional[str],
    snapshot_path: Optional[Path],
    workspace_slug: str,
    cli_operators_config_path: Optional[str],
    force_override: bool,
    legacy_profile: Optional[str],
    legacy_hpc_config_path: Optional[str],
    profiles_config_path: Optional[str],
) -> None:
    """Write or update the metadata.json file."""
    created_at = _utc_now_iso()
    if metadata_path.is_file():
        try:
            existing = json.loads(metadata_path.read_text(encoding="utf-8") or "{}")
            created_at = existing.get("created_at_utc") or created_at
        except Exception:
            # If metadata is corrupt, we still want to be able to proceed; treat as new.
            created_at = created_at

    snap_rel = str(snapshot_path.relative_to(run_root)) if snapshot_path else None

    payload = {
        "schema_version": 1,
        "created_at_utc": created_at,
        "updated_at_utc": _utc_now_iso(),
        "effective": {
            "source": str(source.value),
            "resolved_path": resolved_path,
            "sha256": sha256,
            "snapshot_relpath": snap_rel,
        },
        "provenance": {
            "workspace_slug": workspace_slug,
            "env_var_name": _ENV_OPERATORS_CONFIG,
            "cli": {
                "operators_config": cli_operators_config_path,
                "force_wiring_override": bool(force_override),
            },
            "legacy": {
                "profile": legacy_profile,
                "hpc_config": legacy_hpc_config_path,
                "profiles_config_path": profiles_config_path,
            },
        },
        "history_relpath": str((run_root / "operators_snapshot" / "history.jsonl").relative_to(run_root)),
    }

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_snapshot_sha256_if_present(snapshot_path: Path) -> Optional[str]:
    """Load the SHA256 hash of a snapshot file if it exists."""
    if not snapshot_path.is_file():
        return None
    try:
        return _sha256_bytes(snapshot_path.read_bytes())
    except Exception:
        return None


def _persist_snapshot_bytes(
    *,
    run_root: Path,
    snapshot_dir: Path,
    snapshot_yaml_path: Path,
    metadata_path: Path,
    history_path: Path,
    source: OperatorWiringSource,
    resolved_path: Optional[str],
    snapshot_bytes: bytes,
    cli_operators_config_path: Optional[str],
    force_override: bool,
    legacy_profile: Optional[str],
    legacy_hpc_config_path: Optional[str],
    profiles_config_path: Optional[str],
    workspace_slug: str,
    allow_override: bool,
) -> Tuple[str, Path, bool]:
    """
    Persist `snapshot_bytes` into the run snapshot (idempotent), enforcing override safety.

    Returns: (sha256, snapshot_yaml_path, did_write)
    """
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    desired_sha = _sha256_bytes(snapshot_bytes)
    existing_sha = _load_snapshot_sha256_if_present(snapshot_yaml_path)

    if existing_sha is not None and existing_sha != desired_sha:
        if not allow_override:
            # Record refusal (no mutation).
            _append_history(
                history_path,
                run_root=run_root,
                event="WIRING_OVERRIDE_REFUSED",
                source=OperatorWiringSource.CLI_OVERRIDE,
                sha256=existing_sha,
                resolved_path=resolved_path,
                snapshot_path=snapshot_yaml_path,
                details={"attempted_sha256": desired_sha, "note": "Override refused; rerun with --force-wiring-override"},
            )
            raise ValueError(
                "Refusing to override persisted operator wiring for this run. "
                "Re-run with --force-wiring-override to replace the run snapshot."
            )

        # Forced override: overwrite snapshot + update metadata.
        snapshot_yaml_path.write_bytes(snapshot_bytes)
        _write_metadata(
            metadata_path,
            run_root=run_root,
            source=source,
            resolved_path=resolved_path,
            sha256=desired_sha,
            snapshot_path=snapshot_yaml_path,
            workspace_slug=workspace_slug,
            cli_operators_config_path=cli_operators_config_path,
            force_override=force_override,
            legacy_profile=legacy_profile,
            legacy_hpc_config_path=legacy_hpc_config_path,
            profiles_config_path=profiles_config_path,
        )
        _append_history(
            history_path,
            run_root=run_root,
            event="WIRING_OVERRIDE_FORCED",
            source=source,
            sha256=desired_sha,
            resolved_path=resolved_path,
            snapshot_path=snapshot_yaml_path,
            details={"prior_sha256": existing_sha},
        )
        return desired_sha, snapshot_yaml_path, True

    if existing_sha == desired_sha:
        # Already matches; ensure metadata/history exist for resilience (e.g., older runs).
        if not metadata_path.is_file():
            _write_metadata(
                metadata_path,
                run_root=run_root,
                source=OperatorWiringSource.RUN_PERSISTED,
                resolved_path=str(snapshot_yaml_path),
                sha256=desired_sha,
                snapshot_path=snapshot_yaml_path,
                workspace_slug=workspace_slug,
                cli_operators_config_path=cli_operators_config_path,
                force_override=force_override,
                legacy_profile=legacy_profile,
                legacy_hpc_config_path=legacy_hpc_config_path,
                profiles_config_path=profiles_config_path,
            )
            _append_history(
                history_path,
                run_root=run_root,
                event="WIRING_PERSISTED",
                source=OperatorWiringSource.RUN_PERSISTED,
                sha256=desired_sha,
                resolved_path=str(snapshot_yaml_path),
                snapshot_path=snapshot_yaml_path,
                details={"note": "Reconstructed metadata/history for existing snapshot"},
            )
        return desired_sha, snapshot_yaml_path, False

    # No existing snapshot: write initial snapshot + metadata + history.
    snapshot_yaml_path.write_bytes(snapshot_bytes)
    _write_metadata(
        metadata_path,
        run_root=run_root,
        source=source,
        resolved_path=resolved_path,
        sha256=desired_sha,
        snapshot_path=snapshot_yaml_path,
        workspace_slug=workspace_slug,
        cli_operators_config_path=cli_operators_config_path,
        force_override=force_override,
        legacy_profile=legacy_profile,
        legacy_hpc_config_path=legacy_hpc_config_path,
        profiles_config_path=profiles_config_path,
    )
    _append_history(
        history_path,
        run_root=run_root,
        event="WIRING_PERSISTED",
        source=source,
        sha256=desired_sha,
        resolved_path=resolved_path,
        snapshot_path=snapshot_yaml_path,
        details={"note": "Initial persistence"},
    )
    return desired_sha, snapshot_yaml_path, True
