from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Tuple, Union

from matterstack.config.operators import (
    HpcYamlBackendConfig,
    LocalBackendConfig,
    OperatorInstanceConfig,
    OperatorsConfig,
    ProfileBackendConfig,
    SlurmBackendConfig,
)
from matterstack.config.profiles import load_profile
from matterstack.core.operators import Operator
from matterstack.core.run import RunHandle
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.hpc.ssh import SSHConfig
from matterstack.runtime.backends.local import LocalBackend
from matterstack.runtime.operators.experiment import ExperimentOperator
from matterstack.runtime.operators.hpc import ComputeOperator
from matterstack.runtime.operators.human import HumanOperator


class OperatorFactory(Protocol):
    def create(self, *, operator_key: str, cfg: OperatorInstanceConfig, run_handle: RunHandle) -> Operator: ...


@dataclass(frozen=True)
class OperatorRegistryCacheKey:
    """
    Cache key capturing "this run + these config inputs".
    """

    workspace_slug: str
    run_id: str
    run_root: str
    operators_config_path: str
    operators_config_mtime_ns: int
    profiles_config_path: Optional[str]


_REGISTRY_CACHE: Dict[OperatorRegistryCacheKey, Dict[str, Operator]] = {}


def _mtime_ns(path: Union[str, Path]) -> int:
    try:
        return os.stat(path).st_mtime_ns
    except Exception:
        return 0


def _cache_key_for(
    run_handle: RunHandle, *, operators_config_path: Union[str, Path], profiles_config_path: Optional[str]
) -> OperatorRegistryCacheKey:
    p = Path(operators_config_path)
    return OperatorRegistryCacheKey(
        workspace_slug=str(run_handle.workspace_slug),
        run_id=str(run_handle.run_id),
        run_root=str(Path(run_handle.root_path).resolve()),
        operators_config_path=str(p.resolve()),
        operators_config_mtime_ns=_mtime_ns(p),
        profiles_config_path=str(profiles_config_path) if profiles_config_path else None,
    )


def _default_compute_metadata_for_kind(kind: str) -> Tuple[str, str]:
    """
    Defaults matching legacy behavior as closely as possible.
    """
    if kind == "hpc":
        return "hpc", "HPC"
    if kind == "local":
        return "local", "Local"
    return kind, kind


def _build_compute_operator_from_backend(
    *,
    operator_key: str,
    kind: str,
    backend_cfg: Any,
    run_handle: RunHandle,
    profiles_config_path: Optional[str],
    slug_override: Optional[str],
    operator_name_override: Optional[str],
) -> ComputeOperator:
    slug_default, operator_name_default = _default_compute_metadata_for_kind(kind)
    slug = slug_override or slug_default
    operator_name = operator_name_override or operator_name_default

    # NOTE: LocalBackend workspace_root defaults to run root to preserve evidence layout.
    if isinstance(backend_cfg, LocalBackendConfig):
        backend = LocalBackend(
            workspace_root=str(backend_cfg.workspace_root or run_handle.root_path),
            dry_run=bool(backend_cfg.dry_run),
        )
        return ComputeOperator(backend=backend, slug=slug, operator_name=operator_name)

    if isinstance(backend_cfg, SlurmBackendConfig):
        ssh_cfg = SSHConfig(
            host=backend_cfg.ssh.host,
            user=backend_cfg.ssh.user,
            port=int(backend_cfg.ssh.port),
            key_path=backend_cfg.ssh.key_path,
        )
        backend = SlurmBackend(
            ssh_config=ssh_cfg,
            workspace_root=str(backend_cfg.workspace_root),
            slurm_config=dict(backend_cfg.slurm or {}),
        )
        return ComputeOperator(backend=backend, slug=slug, operator_name=operator_name)

    if isinstance(backend_cfg, ProfileBackendConfig):
        prof = load_profile(backend_cfg.name, config_path=profiles_config_path)
        backend = prof.create_backend()
        return ComputeOperator(backend=backend, slug=slug, operator_name=operator_name)

    if isinstance(backend_cfg, HpcYamlBackendConfig):
        # Kept for backward compatibility: reuse the existing YAML adapter.
        from matterstack.cli.operator_registry import _profile_from_hpc_yaml

        prof = _profile_from_hpc_yaml(backend_cfg.path)
        backend = prof.create_backend()
        return ComputeOperator(backend=backend, slug=slug, operator_name=operator_name)

    raise ValueError(f"Unsupported compute backend config for {operator_key}: {type(backend_cfg)}")


def build_operator_registry_from_operators_config(
    run_handle: RunHandle,
    operators_config: OperatorsConfig,
    *,
    profiles_config_path: Optional[str] = None,
) -> Dict[str, Operator]:
    """
    Construct operator instances for orchestration from `operators.yaml`.

    Returns:
        Mapping canonical operator_key -> Operator instance

    Notes:
        - This function is pure (no caching). Use
          [`get_cached_operator_registry_from_operators_config()`](matterstack/runtime/operators/registry.py:1)
          for per-run cached construction.
    """
    reg: Dict[str, Operator] = {}

    for operator_key, cfg in operators_config.operators.items():
        if cfg.kind in ("hpc", "local"):
            backend_cfg = cfg.backend
            assert backend_cfg is not None  # validated in config layer
            reg[operator_key] = _build_compute_operator_from_backend(
                operator_key=operator_key,
                kind=cfg.kind,
                backend_cfg=backend_cfg,
                run_handle=run_handle,
                profiles_config_path=profiles_config_path,
                slug_override=cfg.slug,
                operator_name_override=cfg.operator_name,
            )
            continue

        if cfg.kind == "human":
            reg[operator_key] = HumanOperator()
            continue

        if cfg.kind == "experiment":
            reg[operator_key] = ExperimentOperator()
            continue

        raise ValueError(f"Unsupported operator kind: {cfg.kind!r} (key {operator_key!r})")

    return reg


def get_cached_operator_registry_from_operators_config(
    run_handle: RunHandle,
    operators_config: OperatorsConfig,
    *,
    profiles_config_path: Optional[str] = None,
) -> Dict[str, Operator]:
    """
    Per-process cache wrapper around `build_operator_registry_from_operators_config()`.

    This prevents reconstructing operators/backends (and re-connecting SSH) on each
    scheduler tick within the same CLI process.

    Cache key includes:
    - run_id + run_root
    - operators.yaml path + mtime
    - profiles config path (if used for backend.type=profile)
    """
    key = _cache_key_for(
        run_handle,
        operators_config_path=operators_config.path,
        profiles_config_path=profiles_config_path,
    )
    cached = _REGISTRY_CACHE.get(key)
    if cached is not None:
        return cached

    reg = build_operator_registry_from_operators_config(
        run_handle, operators_config, profiles_config_path=profiles_config_path
    )
    _REGISTRY_CACHE[key] = reg
    return reg
