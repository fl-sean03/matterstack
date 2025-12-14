from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from matterstack.core.operator_keys import normalize_operator_key, split_operator_key


class OperatorsConfigError(ValueError):
    """
    Raised when operators.yaml cannot be parsed or validated.

    Prefer raising this over raw ValidationError/KeyError so callers can surface
    a clean, user-friendly message.
    """


class SSHConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    user: str
    port: int = 22
    key_path: Optional[str] = None


class LocalBackendConfig(BaseModel):
    """
    Inline config for LocalBackend.

    workspace_root:
      - If omitted, callers MAY default this to the current run root (recommended).
      - Keeping it optional supports portable configs that don't bake run paths.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["local"]
    workspace_root: Optional[str] = None
    dry_run: bool = False


class SlurmBackendConfig(BaseModel):
    """
    Inline config for SlurmBackend over SSH.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["slurm"]
    workspace_root: str
    ssh: SSHConfigModel
    slurm: Dict[str, Any] = Field(default_factory=dict)


class ProfileBackendConfig(BaseModel):
    """
    Backend config that references an execution profile by name, loaded via
    [`matterstack/config/profiles.py`](matterstack/config/profiles.py:1).
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["profile"]
    name: str


class HpcYamlBackendConfig(BaseModel):
    """
    Backend config that references the legacy CURC HPC YAML file format, using
    the adapter in [`matterstack/cli/operator_registry.py`](matterstack/cli/operator_registry.py:34).

    This exists for backward compatibility and migration.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["hpc_yaml"]
    path: str


ComputeBackendConfig = Union[
    LocalBackendConfig,
    SlurmBackendConfig,
    ProfileBackendConfig,
    HpcYamlBackendConfig,
]


class OperatorInstanceConfig(BaseModel):
    """
    Config for one operator instance addressed by a canonical operator key.

    The canonical key is stored outside the instance config as the mapping key:
      operators:
        hpc.default:
          kind: hpc
          ...

    Validation rules:
    - operator key is validated separately (canonical format + kind match)
    - kind determines required/allowed fields

    Supported kinds for v0.2.6:
    - hpc.*       -> Compute operator
    - local.*     -> Compute operator
    - human.*     -> Human operator
    - experiment.*-> Experiment operator
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["hpc", "local", "human", "experiment"]

    # Only used for compute kinds (hpc/local). Defaults to LocalBackend if omitted.
    backend: Optional[ComputeBackendConfig] = Field(default=None, discriminator="type")

    # Optional metadata / overrides for operator objects.
    slug: Optional[str] = None
    operator_name: Optional[str] = None

    # Future-facing; not used by current ExperimentOperator implementation.
    # We keep it as a strictly-validated mapping for forward compatibility.
    api: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _validate_kind_semantics(self) -> "OperatorInstanceConfig":
        if self.kind in ("hpc", "local"):
            if self.backend is None:
                # Default compute backend: local (resolved later to run-root by factory if workspace_root is None).
                return self.model_copy(update={"backend": LocalBackendConfig(type="local")})

            # Enforce that a legacy hpc_yaml backend only makes sense for hpc.*
            if isinstance(self.backend, HpcYamlBackendConfig) and self.kind != "hpc":
                raise ValueError("backend.type='hpc_yaml' is only valid for kind='hpc'")

            return self

        # Non-compute kinds must not specify compute backend settings.
        if self.backend is not None:
            raise ValueError(f"kind={self.kind!r} must not define 'backend'")

        return self


@dataclass(frozen=True)
class OperatorsConfig:
    """
    Parsed operators.yaml content.

    operators: mapping of canonical operator_key -> validated instance config
    path: path to the file that produced this config (for error messages)
    """

    operators: Dict[str, OperatorInstanceConfig]
    path: Path


def _ensure_mapping(value: Any, *, what: str, path: Path) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OperatorsConfigError(f"{path}: {what} must be a YAML mapping/object")
    return value


def parse_operators_config_dict(data: Any, *, path: Union[str, Path]) -> OperatorsConfig:
    """
    Parse a loaded YAML object into an OperatorsConfig.

    Raises:
        OperatorsConfigError on any validation/shape error.
    """
    p = Path(path)

    top = _ensure_mapping(data or {}, what="top-level", path=p)
    operators_raw = top.get("operators")
    if operators_raw is None:
        raise OperatorsConfigError(f"{p}: missing required top-level key 'operators'")
    operators_map = _ensure_mapping(operators_raw, what="'operators'", path=p)

    parsed: Dict[str, OperatorInstanceConfig] = {}

    for raw_key, raw_cfg in operators_map.items():
        if not isinstance(raw_key, str):
            raise OperatorsConfigError(f"{p}: operator key must be a string, got {type(raw_key)}")

        # Enforce canonical keys in config (strict; no implicit normalization).
        if raw_key != raw_key.strip():
            raise OperatorsConfigError(f"{p}: operator key has leading/trailing whitespace: {raw_key!r}")
        if raw_key.lower() != raw_key:
            raise OperatorsConfigError(f"{p}: operator key must be lowercase canonical form: {raw_key!r}")

        try:
            normalized_key = normalize_operator_key(raw_key)
        except Exception as exc:
            raise OperatorsConfigError(f"{p}: invalid operator key {raw_key!r}: {exc}") from exc

        if normalized_key != raw_key:
            # normalize_operator_key lowercases; we already enforce lowercase. This is for safety.
            raise OperatorsConfigError(f"{p}: operator key must be canonical: {raw_key!r}")

        try:
            key_kind, _key_name = split_operator_key(normalized_key)
        except Exception as exc:
            raise OperatorsConfigError(f"{p}: invalid operator key {raw_key!r}: {exc}") from exc

        if normalized_key in parsed:
            raise OperatorsConfigError(f"{p}: duplicate operator key {normalized_key!r}")

        if not isinstance(raw_cfg, Mapping):
            raise OperatorsConfigError(f"{p}: operators.{normalized_key} must be a mapping/object")

        try:
            inst = OperatorInstanceConfig.model_validate(raw_cfg)
        except ValidationError as exc:
            raise OperatorsConfigError(
                f"{p}: invalid config for operators.{normalized_key}: {exc}"
            ) from exc

        if inst.kind != key_kind:
            raise OperatorsConfigError(
                f"{p}: operators.{normalized_key}: key kind {key_kind!r} does not match config kind {inst.kind!r}"
            )

        parsed[normalized_key] = inst

    return OperatorsConfig(operators=parsed, path=p)


def load_operators_config(path: Union[str, Path]) -> OperatorsConfig:
    """
    Load and validate operators.yaml.

    The expected file shape is:

      operators:
        hpc.default:
          kind: hpc
          backend:
            type: slurm
            workspace_root: /scratch/...
            ssh: {host: ..., user: ..., key_path: ...}
            slurm: {...}

    Raises:
        OperatorsConfigError
    """
    p = Path(path)
    if not p.is_file():
        raise OperatorsConfigError(f"{p}: file not found")

    try:
        data = yaml.safe_load(p.read_text()) or {}
    except Exception as exc:
        raise OperatorsConfigError(f"{p}: failed to parse YAML: {exc}") from exc

    return parse_operators_config_dict(data, path=p)