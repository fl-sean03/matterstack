from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from matterstack.config.profiles import ExecutionProfile, SlurmProfile, load_profile
from matterstack.core.run import RunHandle
from matterstack.runtime.backends.hpc.ssh import SSHConfig
from matterstack.runtime.backends.local import LocalBackend
from matterstack.runtime.operators.experiment import ExperimentOperator
from matterstack.runtime.operators.hpc import ComputeOperator
from matterstack.runtime.operators.human import HumanOperator


@dataclass(frozen=True)
class RegistryConfig:
    """
    Configuration inputs for building an operator registry.

    Precedence:
    - If hpc_config_path is provided, it wins for the HPC operator backend.
    - Else if profile is provided, it is used for the HPC operator backend.
    - Else HPC operator falls back to LocalBackend (backward compatible).
    """

    config_path: Optional[str] = None
    profile: Optional[str] = None
    hpc_config_path: Optional[str] = None


def _profile_from_hpc_yaml(path: Union[str, Path]) -> ExecutionProfile:
    """
    Compatibility adapter for existing CURC HPC YAML config format.

    Expected schema (subset used):
      cluster:
        ssh: {host, user, key_path}
        paths: {remote_workspace}
        slurm: {account, partition, qos, time, ntasks, modules, ...}

    Returns:
      ExecutionProfile(type="slurm") ready to .create_backend().
    """
    p = Path(path)
    data = yaml.safe_load(p.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"HPC config {p} must contain a YAML mapping at top-level.")

    cluster = data.get("cluster") or {}
    if not isinstance(cluster, dict):
        raise ValueError(f"HPC config {p} must contain a 'cluster' mapping.")

    ssh = cluster.get("ssh") or {}
    if not isinstance(ssh, dict):
        raise ValueError(f"HPC config {p} must contain cluster.ssh mapping.")

    paths = cluster.get("paths") or {}
    if not isinstance(paths, dict):
        raise ValueError(f"HPC config {p} must contain cluster.paths mapping.")

    slurm = cluster.get("slurm") or {}
    if not isinstance(slurm, dict):
        raise ValueError(f"HPC config {p} must contain cluster.slurm mapping.")

    try:
        ssh_cfg = SSHConfig(
            host=str(ssh["host"]),
            user=str(ssh["user"]),
            port=int(ssh.get("port", 22)),
            key_path=str(ssh.get("key_path")) if ssh.get("key_path") else None,
        )
    except KeyError as exc:
        raise ValueError(f"Missing required SSH field {exc!s} in {p}.") from exc

    workspace_root_val = paths.get("remote_workspace")
    if not workspace_root_val:
        raise ValueError(f"Missing required cluster.paths.remote_workspace in {p}.")
    workspace_root = str(workspace_root_val)

    return ExecutionProfile(
        name=f"hpc_yaml:{p.name}",
        type="slurm",
        raw={
            "type": "slurm",
            "workspace_root": workspace_root,
            "ssh": {"host": ssh_cfg.host, "user": ssh_cfg.user, "port": ssh_cfg.port, "key_path": ssh_cfg.key_path},
            "slurm": slurm,
            "source": str(p),
        },
        slurm=SlurmProfile(
            workspace_root=workspace_root,
            ssh=ssh_cfg,
            slurm=slurm,
        ),
    )


def build_operator_registry(
    run_handle: RunHandle,
    *,
    registry_config: RegistryConfig,
) -> Dict[str, Any]:
    """
    Construct operator instances for orchestration, keyed by operator_type strings.

    Keys MUST match attempt.operator_type values stored in DB and used by orchestrator.
    """
    # Always keep Local operator rooted at the run root for backward-compatible evidence layout.
    local_backend = LocalBackend(workspace_root=str(run_handle.root_path))

    # Decide HPC backend (Slurm when configured; else local fallback).
    hpc_backend: Any = local_backend

    if registry_config.hpc_config_path:
        hpc_profile = _profile_from_hpc_yaml(registry_config.hpc_config_path)
        hpc_backend = hpc_profile.create_backend()
    elif registry_config.profile:
        prof = load_profile(registry_config.profile, config_path=registry_config.config_path)
        hpc_backend = prof.create_backend()

    return {
        "Human": HumanOperator(),
        "Experiment": ExperimentOperator(),
        "Local": ComputeOperator(backend=local_backend, slug="local", operator_name="Local"),
        "HPC": ComputeOperator(backend=hpc_backend, slug="hpc", operator_name="HPC"),
    }