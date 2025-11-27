from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import os

import yaml

from matterstack.core.backend import ComputeBackend
# Moved imports to create_backend to avoid circular dependencies
from matterstack.runtime.backends.hpc.ssh import SSHConfig


@dataclass
class LocalProfile:
    """Configuration for executing tasks on the local machine."""

    workspace_root: str
    dry_run: bool = False


@dataclass
class SlurmProfile:
    """Configuration for executing tasks on an HPC cluster via Slurm."""

    workspace_root: str
    ssh: SSHConfig
    slurm: Dict[str, Any]


@dataclass
class ExecutionProfile:
    """
    High-level execution profile resolved from configuration.

    This is the object that orchestration code works with. It captures the
    original raw configuration as well as any backend-specific structured
    data needed to construct a ComputeBackend.
    """

    name: str
    type: str            # "local", "slurm", etc.
    raw: Dict[str, Any]  # Original config dict for debugging
    local: Optional[LocalProfile] = None
    slurm: Optional[SlurmProfile] = None

    def create_backend(self) -> ComputeBackend:
        """
        Instantiate a ComputeBackend for this profile.

        The backend type is determined by the ``type`` field. Currently
        supported types:

        * ``"local"`` -> LocalBackend
        * ``"slurm"`` -> SlurmBackend
        """

        if self.type == "local":
            from matterstack.runtime.backends.local import LocalBackend
            if self.local is None:
                raise ValueError("Local profile data must be provided for type 'local'.")
            return LocalBackend(
                workspace_root=self.local.workspace_root,
                dry_run=self.local.dry_run,
            )

        if self.type == "slurm":
            from matterstack.runtime.backends.hpc.backend import SlurmBackend
            if self.slurm is None:
                raise ValueError("Slurm profile data must be provided for type 'slurm'.")
            return SlurmBackend(
                ssh_config=self.slurm.ssh,
                workspace_root=self.slurm.workspace_root,
                slurm_config=self.slurm.slurm,
            )

        raise ValueError(f"Unknown profile type: {self.type!r}")


def _load_yaml(path: Path) -> Dict[str, Any]:
    """
    Load a YAML file as a top-level mapping.

    Missing files are treated as empty configuration.
    """

    if not path.is_file():
        return {}
    text = path.read_text()
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a mapping at top-level.")
    return data


def _profiles_from_dict(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Extract the ``profiles`` mapping from a loaded config dictionary.
    """

    profiles = data.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise ValueError("'profiles' section must be a mapping.")
    # Ensure nested profiles are all dicts for type-safety at construction time.
    result: Dict[str, Dict[str, Any]] = {}
    for name, value in profiles.items():
        if not isinstance(value, dict):
            raise ValueError(f"Profile {name!r} must be a mapping.")
        result[str(name)] = value
    return result


def _find_project_config_file() -> Optional[Path]:
    """
    Search upwards from the current working directory for a matterstack.yaml/yml file.

    The first match encountered while walking towards the filesystem root is used.
    """

    current = Path.cwd()

    while True:
        for filename in ("matterstack.yaml", "matterstack.yml"):
            candidate = current / filename
            if candidate.is_file():
                return candidate
        if current.parent == current:
            break
        current = current.parent
    return None


def _build_local_profile(name: str, data: Dict[str, Any]) -> ExecutionProfile:
    """
    Construct an ExecutionProfile for a local backend.
    """
    workspace_root = str(data.get("workspace_root", "./results"))
    dry_run = bool(data.get("dry_run", False))
    local_profile = LocalProfile(workspace_root=workspace_root, dry_run=dry_run)
    return ExecutionProfile(
        name=name,
        type="local",
        raw=data,
        local=local_profile,
    )


def _build_slurm_profile(name: str, data: Dict[str, Any]) -> ExecutionProfile:
    """
    Construct an ExecutionProfile for a Slurm backend.
    """
    workspace_root_value = data.get("workspace_root")
    if workspace_root_value is None:
        raise ValueError(f"Slurm profile {name!r} must define 'workspace_root'.")
    workspace_root = str(workspace_root_value)

    ssh_data = data.get("ssh") or {}
    if not isinstance(ssh_data, dict):
        raise ValueError(f"Slurm profile {name!r} 'ssh' section must be a mapping.")

    try:
        ssh_cfg = SSHConfig(
            host=ssh_data["host"],
            user=ssh_data["user"],
            port=int(ssh_data.get("port", 22)),
            key_path=ssh_data.get("key_path"),
        )
    except KeyError as exc:
        raise ValueError(f"Missing SSH field {exc!s} for Slurm profile {name!r}.") from exc

    slurm_cfg = data.get("slurm") or {}
    if not isinstance(slurm_cfg, dict):
        raise ValueError(f"Slurm profile {name!r} 'slurm' section must be a mapping.")

    slurm_profile = SlurmProfile(
        workspace_root=workspace_root,
        ssh=ssh_cfg,
        slurm=slurm_cfg,
    )

    return ExecutionProfile(
        name=name,
        type="slurm",
        raw=data,
        slurm=slurm_profile,
    )


def load_profiles(config_path: Optional[str] = None) -> Dict[str, ExecutionProfile]:
    """
    Load all execution profiles from configuration.

    Resolution rules:

    * If ``config_path`` is provided, only that file is used.
    * Otherwise, user-level config is loaded from
      ``~/.matterstack/config.yaml`` and project-level config is searched for
      by walking upwards from the current working directory looking for
      ``matterstack.yaml`` or ``matterstack.yml``.
    * Project profiles override user profiles of the same name on a
    per-field basis (shallow merge).
    """

    if config_path is not None:
        cfg_path = Path(os.path.expanduser(config_path))
        cfg_data = _load_yaml(cfg_path)
        profile_dicts = _profiles_from_dict(cfg_data)
    else:
        user_cfg_path = Path.home() / ".matterstack" / "config.yaml"
        user_cfg = _load_yaml(user_cfg_path)
        user_profiles = _profiles_from_dict(user_cfg)

        project_cfg: Dict[str, Any] = {}
        project_path = _find_project_config_file()
        if project_path is not None:
            project_cfg = _load_yaml(project_path)
        project_profiles = _profiles_from_dict(project_cfg)

        # Merge by profile name, with project values overlaying user values.
        merged: Dict[str, Dict[str, Any]] = {}
        for name, pdata in user_profiles.items():
            merged[name] = dict(pdata)
        for name, pdata in project_profiles.items():
            base = merged.get(name, {}).copy()
            base.update(pdata)
            merged[name] = base

        profile_dicts = merged

    profiles: Dict[str, ExecutionProfile] = {}
    for name, pdata in profile_dicts.items():
        type_value = pdata.get("type")
        if not isinstance(type_value, str):
            raise ValueError(f"Profile {name!r} must define a string 'type'.")
        type_str = type_value.lower()

        if type_str == "local":
            profiles[name] = _build_local_profile(name, pdata)
        elif type_str == "slurm":
            profiles[name] = _build_slurm_profile(name, pdata)
        else:
            # Preserve raw type in case of typos for easier debugging.
            raise ValueError(f"Unknown profile type {type_value!r} for profile {name!r}.")

    return profiles


def load_profile(name: str, config_path: Optional[str] = None) -> ExecutionProfile:
    """
    Load a single named ExecutionProfile.
    """

    profiles = load_profiles(config_path=config_path)
    try:
        return profiles[name]
    except KeyError as exc:
        raise KeyError(f"Profile {name!r} not found in config.") from exc


def get_default_profile(config_path: Optional[str] = None) -> ExecutionProfile:
    """
    Resolve a default ExecutionProfile.

    Preference order:

    * If any profiles are configured, prefer the first profile whose type is
      ``"local"``.
    * If no local profiles exist but others do, return an arbitrary profile.
    * If no profiles exist at all, fall back to a built-in local_default that
      writes results to ``./results``.
    """

    profiles = load_profiles(config_path=config_path)
    if not profiles:
        # Fallback built-in local_default
        return ExecutionProfile(
            name="local_default",
            type="local",
            raw={"type": "local", "workspace_root": "./results", "dry_run": False},
            local=LocalProfile(workspace_root="./results", dry_run=False),
        )

    # Prefer any "local" profile if present
    for profile in profiles.values():
        if profile.type.lower() == "local":
            return profile

    # Otherwise return arbitrary first profile
    return next(iter(profiles.values()))