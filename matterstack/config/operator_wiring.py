"""
Operator wiring resolution and management.

This module provides the main entry point for resolving operator wiring configuration
for runs, implementing v0.2.7 precedence rules and ensuring run-local snapshot persistence.

Public API:
- OperatorWiringSource: Enum for wiring source types
- ResolvedOperatorWiring: Result dataclass from resolution
- OperatorWiringProvenance: Lightweight provenance view for diagnostics
- resolve_operator_wiring: Main resolution function
- load_wiring_provenance_from_run_root: Load provenance from run metadata
- format_operator_wiring_explain_line: Format provenance for explain command
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

# Import legacy support
from ._wiring_legacy import _generate_legacy_operators_yaml_bytes

# Import internal persistence functions
from ._wiring_persistence import (
    _append_history,
    _ensure_explicit_path_exists,
    _load_snapshot_sha256_if_present,
    _persist_snapshot_bytes,
    _snapshot_paths,
    _write_metadata,
)

# Re-export provenance functions
from ._wiring_provenance import (
    format_operator_wiring_explain_line,
    load_wiring_provenance_from_run_root,
)

# Re-export types from internal modules
from ._wiring_types import (
    _ENV_OPERATORS_CONFIG,
    OperatorWiringProvenance,
    OperatorWiringSource,
    ResolvedOperatorWiring,
)

__all__ = [
    "OperatorWiringSource",
    "ResolvedOperatorWiring",
    "OperatorWiringProvenance",
    "resolve_operator_wiring",
    "load_wiring_provenance_from_run_root",
    "format_operator_wiring_explain_line",
]


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
