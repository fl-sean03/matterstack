from __future__ import annotations

from pathlib import Path

from matterstack.config.operators import parse_operators_config_dict
from matterstack.core.run import RunHandle
from matterstack.runtime.backends.local import LocalBackend
from matterstack.runtime.operators.hpc import ComputeOperator
from matterstack.runtime.operators.registry import (
    build_operator_registry_from_operators_config,
    get_cached_operator_registry_from_operators_config,
)


def test_build_operator_registry_from_operators_config_constructs_multi_instance(tmp_path: Path) -> None:
    operators_cfg = parse_operators_config_dict(
        {
            "operators": {
                "hpc.default": {"kind": "hpc", "backend": {"type": "local"}},
                "hpc.dev": {"kind": "hpc", "backend": {"type": "local", "workspace_root": str(tmp_path / "hpc_dev")}},
                "human.default": {"kind": "human"},
            }
        },
        path=tmp_path / "operators.yaml",
    )

    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)
    handle = RunHandle(workspace_slug="ws", run_id="r1", root_path=run_root)

    reg = build_operator_registry_from_operators_config(handle, operators_cfg)

    assert set(reg.keys()) == {"hpc.default", "hpc.dev", "human.default"}
    assert isinstance(reg["hpc.default"], ComputeOperator)
    assert isinstance(reg["hpc.dev"], ComputeOperator)

    # Default local compute should root at run root when workspace_root omitted
    assert isinstance(reg["hpc.default"].backend, LocalBackend)
    assert Path(reg["hpc.default"].backend.workspace_root).resolve() == run_root.resolve()

    # hpc.dev should respect its workspace_root override
    assert isinstance(reg["hpc.dev"].backend, LocalBackend)
    assert Path(reg["hpc.dev"].backend.workspace_root).resolve() == (tmp_path / "hpc_dev").resolve()

    # Different operator instances (multi-instance) must be distinct objects
    assert reg["hpc.default"] is not reg["hpc.dev"]


def test_get_cached_operator_registry_returns_same_object_for_same_key(tmp_path: Path) -> None:
    # Need a real file path + mtime for the cache key.
    ops_path = tmp_path / "operators.yaml"
    ops_path.write_text(
        """operators:
  hpc.default:
    kind: hpc
    backend:
      type: local
"""
    )

    operators_cfg = parse_operators_config_dict(
        {"operators": {"hpc.default": {"kind": "hpc", "backend": {"type": "local"}}}},
        path=ops_path,
    )

    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)
    handle = RunHandle(workspace_slug="ws", run_id="r1", root_path=run_root)

    reg1 = get_cached_operator_registry_from_operators_config(handle, operators_cfg)
    reg2 = get_cached_operator_registry_from_operators_config(handle, operators_cfg)

    assert reg1 is reg2
    assert reg1["hpc.default"] is reg2["hpc.default"]
