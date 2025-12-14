from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

_ENV_OPERATORS_CONFIG = "MATTERSTACK_OPERATORS_CONFIG"


class OperatorWiringSource(str, Enum):
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _ensure_explicit_path_exists(path: Path, *, what: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{what} file not found: {path}")


def _snapshot_paths(run_root: Path) -> Tuple[Path, Path, Path, Path]:
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


def _generate_legacy_operators_yaml_bytes(
    *,
    legacy_hpc_config_path: Optional[str],
    legacy_profile: Optional[str],
) -> Tuple[OperatorWiringSource, Optional[str], bytes]:
    """
    Generate a minimal operators.yaml snapshot from legacy CLI inputs.

    Returns: (source, resolved_path, snapshot_bytes)
    """
    if legacy_hpc_config_path:
        source = OperatorWiringSource.LEGACY_HPC_CONFIG
        resolved = legacy_hpc_config_path
        hpc_backend: Dict[str, Any] = {"type": "hpc_yaml", "path": legacy_hpc_config_path}
    elif legacy_profile:
        source = OperatorWiringSource.LEGACY_PROFILE
        resolved = legacy_profile
        hpc_backend = {"type": "profile", "name": legacy_profile}
    else:
        raise ValueError("Legacy snapshot generation requested without legacy inputs.")

    operators_doc = {
        "operators": {
            "human.default": {"kind": "human"},
            "experiment.default": {"kind": "experiment"},
            "local.default": {"kind": "local", "backend": {"type": "local"}},
            "hpc.default": {"kind": "hpc", "backend": hpc_backend},
        }
    }

    # Stable, human-readable YAML for hashing/provenance.
    snapshot_text = yaml.safe_dump(operators_doc, sort_keys=True)
    return source, resolved, snapshot_text.encode("utf-8")


def resolve_operator_wiring(
    run_handle: Any,
    *,
    cli_operators_config_path: Optional[str] = None,
    force_override: bool = False,
    workspace_base_path: Optional[Path] = None,
    legacy_hpc_config_path: Optional[str] = None,
    legacy_profile: Optional[str] = None,
    profiles_config_path: Optional[str] = None,
) -> ResolvedOperatorWiring:
    """
    Resolve operator wiring by v0.2.7 precedence and ensure a run-local snapshot exists.

    Precedence (highest -> lowest):
      1) CLI `--operators-config`
      2) Run snapshot `<run_root>/operators_snapshot/operators.yaml`
      3) Workspace default `workspaces/<workspace_slug>/operators.yaml`
      4) Env var `MATTERSTACK_OPERATORS_CONFIG`
      5) Legacy fallback (`--hpc-config` / `--profile`) via generated operators.yaml snapshot

    Override safety:
      - If a run snapshot exists and CLI `--operators-config` is provided, refuse unless
        `force_override=True`. Refusals and forced overrides are recorded in history.jsonl.
    """
    run_root = Path(run_handle.root_path)
    workspace_slug = str(getattr(run_handle, "workspace_slug", ""))
    if not workspace_slug:
        # RunHandle should always have a workspace slug; but keep error deterministic.
        workspace_slug = "UNKNOWN_WORKSPACE"

    workspace_base = workspace_base_path or Path("workspaces")
    snapshot_dir, snapshot_yaml, metadata_json, history_jsonl = _snapshot_paths(run_root)

    warnings: list[str] = []

    # 1) CLI override: highest precedence.
    if cli_operators_config_path:
        p = Path(cli_operators_config_path)
        _ensure_explicit_path_exists(p, what="CLI --operators-config")
        snapshot_bytes = p.read_bytes()
        sha, snap_path, _did_write = _persist_snapshot_bytes(
            run_root=run_root,
            snapshot_dir=snapshot_dir,
            snapshot_yaml_path=snapshot_yaml,
            metadata_path=metadata_json,
            history_path=history_jsonl,
            source=OperatorWiringSource.CLI_OVERRIDE,
            resolved_path=str(p.resolve()),
            snapshot_bytes=snapshot_bytes,
            cli_operators_config_path=str(p.resolve()),
            force_override=force_override,
            legacy_profile=legacy_profile,
            legacy_hpc_config_path=legacy_hpc_config_path,
            profiles_config_path=profiles_config_path,
            workspace_slug=workspace_slug,
            allow_override=bool(force_override),
        )
        return ResolvedOperatorWiring(
            source=OperatorWiringSource.CLI_OVERRIDE,
            resolved_path=str(p.resolve()),
            sha256=sha,
            snapshot_path=str(snap_path),
            snapshot_dir=str(snapshot_dir),
            metadata_path=str(metadata_json),
            history_path=str(history_jsonl),
            is_persisted=True,
            warnings=warnings,
        )

    # 2) Run snapshot.
    if snapshot_yaml.is_file():
        sha = _load_snapshot_sha256_if_present(snapshot_yaml)
        if sha is None:
            warnings.append("Failed to compute sha256 for existing run snapshot; treating as unknown.")
        # Ensure metadata/history exist for resilience.
        if not metadata_json.is_file():
            _write_metadata(
                metadata_json,
                run_root=run_root,
                source=OperatorWiringSource.RUN_PERSISTED,
                resolved_path=str(snapshot_yaml),
                sha256=sha,
                snapshot_path=snapshot_yaml,
                workspace_slug=workspace_slug,
                cli_operators_config_path=None,
                force_override=False,
                legacy_profile=None,
                legacy_hpc_config_path=None,
                profiles_config_path=profiles_config_path,
            )
            _append_history(
                history_jsonl,
                run_root=run_root,
                event="WIRING_PERSISTED",
                source=OperatorWiringSource.RUN_PERSISTED,
                sha256=sha,
                resolved_path=str(snapshot_yaml),
                snapshot_path=snapshot_yaml,
                details={"note": "Reconstructed metadata/history for existing snapshot"},
            )
        return ResolvedOperatorWiring(
            source=OperatorWiringSource.RUN_PERSISTED,
            resolved_path=str(snapshot_yaml),
            sha256=sha,
            snapshot_path=str(snapshot_yaml),
            snapshot_dir=str(snapshot_dir),
            metadata_path=str(metadata_json),
            history_path=str(history_jsonl),
            is_persisted=True,
            warnings=warnings,
        )

    # 3) Workspace default.
    workspace_default = workspace_base / workspace_slug / "operators.yaml"
    if workspace_default.is_file():
        snapshot_bytes = workspace_default.read_bytes()
        sha, snap_path, _did_write = _persist_snapshot_bytes(
            run_root=run_root,
            snapshot_dir=snapshot_dir,
            snapshot_yaml_path=snapshot_yaml,
            metadata_path=metadata_json,
            history_path=history_jsonl,
            source=OperatorWiringSource.WORKSPACE_DEFAULT,
            resolved_path=str(workspace_default.resolve()),
            snapshot_bytes=snapshot_bytes,
            cli_operators_config_path=None,
            force_override=False,
            legacy_profile=legacy_profile,
            legacy_hpc_config_path=legacy_hpc_config_path,
            profiles_config_path=profiles_config_path,
            workspace_slug=workspace_slug,
            allow_override=False,
        )
        return ResolvedOperatorWiring(
            source=OperatorWiringSource.WORKSPACE_DEFAULT,
            resolved_path=str(workspace_default.resolve()),
            sha256=sha,
            snapshot_path=str(snap_path),
            snapshot_dir=str(snapshot_dir),
            metadata_path=str(metadata_json),
            history_path=str(history_jsonl),
            is_persisted=True,
            warnings=warnings,
        )

    # 4) Env var.
    env_path_raw = os.environ.get(_ENV_OPERATORS_CONFIG)
    if env_path_raw:
        env_path = Path(env_path_raw)
        _ensure_explicit_path_exists(env_path, what=f"Env var {_ENV_OPERATORS_CONFIG}")
        snapshot_bytes = env_path.read_bytes()
        sha, snap_path, _did_write = _persist_snapshot_bytes(
            run_root=run_root,
            snapshot_dir=snapshot_dir,
            snapshot_yaml_path=snapshot_yaml,
            metadata_path=metadata_json,
            history_path=history_jsonl,
            source=OperatorWiringSource.ENV_VAR,
            resolved_path=str(env_path.resolve()),
            snapshot_bytes=snapshot_bytes,
            cli_operators_config_path=None,
            force_override=False,
            legacy_profile=legacy_profile,
            legacy_hpc_config_path=legacy_hpc_config_path,
            profiles_config_path=profiles_config_path,
            workspace_slug=workspace_slug,
            allow_override=False,
        )
        return ResolvedOperatorWiring(
            source=OperatorWiringSource.ENV_VAR,
            resolved_path=str(env_path.resolve()),
            sha256=sha,
            snapshot_path=str(snap_path),
            snapshot_dir=str(snapshot_dir),
            metadata_path=str(metadata_json),
            history_path=str(history_jsonl),
            is_persisted=True,
            warnings=warnings,
        )

    # 5) Legacy fallback -> generate snapshot.
    if legacy_hpc_config_path or legacy_profile:
        if legacy_hpc_config_path and legacy_profile:
            raise ValueError("Cannot combine legacy --hpc-config and --profile; choose one.")

        if legacy_hpc_config_path:
            _ensure_explicit_path_exists(Path(legacy_hpc_config_path), what="Legacy --hpc-config")
        source, resolved, snapshot_bytes = _generate_legacy_operators_yaml_bytes(
            legacy_hpc_config_path=legacy_hpc_config_path,
            legacy_profile=legacy_profile,
        )
        sha, snap_path, _did_write = _persist_snapshot_bytes(
            run_root=run_root,
            snapshot_dir=snapshot_dir,
            snapshot_yaml_path=snapshot_yaml,
            metadata_path=metadata_json,
            history_path=history_jsonl,
            source=source,
            resolved_path=resolved,
            snapshot_bytes=snapshot_bytes,
            cli_operators_config_path=None,
            force_override=False,
            legacy_profile=legacy_profile,
            legacy_hpc_config_path=legacy_hpc_config_path,
            profiles_config_path=profiles_config_path,
            workspace_slug=workspace_slug,
            allow_override=False,
        )
        return ResolvedOperatorWiring(
            source=source,
            resolved_path=resolved,
            sha256=sha,
            snapshot_path=str(snap_path),
            snapshot_dir=str(snapshot_dir),
            metadata_path=str(metadata_json),
            history_path=str(history_jsonl),
            is_persisted=True,
            warnings=warnings,
        )

    # Nothing resolved.
    return ResolvedOperatorWiring(
        source=OperatorWiringSource.NONE,
        resolved_path=None,
        sha256=None,
        snapshot_path=None,
        snapshot_dir=str(snapshot_dir),
        metadata_path=str(metadata_json),
        history_path=str(history_jsonl),
        is_persisted=False,
        warnings=warnings,
    )