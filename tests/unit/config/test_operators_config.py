from __future__ import annotations

from pathlib import Path

import pytest

from matterstack.config.operators import (
    OperatorsConfigError,
    load_operators_config,
    parse_operators_config_dict,
)


def test_parse_operators_config_accepts_multi_instance_happy_path(tmp_path: Path) -> None:
    cfg = {
        "operators": {
            "hpc.default": {
                "kind": "hpc",
                # backend omitted -> defaults to local
            },
            "local.dev": {
                "kind": "local",
                "backend": {"type": "local", "workspace_root": str(tmp_path / "local_ws")},
            },
            "human.default": {"kind": "human"},
            "experiment.default": {"kind": "experiment"},
        }
    }

    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert set(parsed.operators.keys()) == {
        "hpc.default",
        "local.dev",
        "human.default",
        "experiment.default",
    }

    # Compute kinds default to a local backend if omitted
    assert parsed.operators["hpc.default"].backend is not None
    assert parsed.operators["hpc.default"].backend.type == "local"


def test_load_operators_config_requires_top_level_operators_key(tmp_path: Path) -> None:
    p = tmp_path / "operators.yaml"
    p.write_text("not_operators: {}\n")

    with pytest.raises(OperatorsConfigError, match="missing required top-level key 'operators'"):
        load_operators_config(p)


def test_parse_operators_config_rejects_non_mapping_top_level(tmp_path: Path) -> None:
    with pytest.raises(OperatorsConfigError, match="top-level must be a YAML mapping"):
        parse_operators_config_dict(["nope"], path=tmp_path / "operators.yaml")  # type: ignore[arg-type]


def test_parse_operators_config_rejects_invalid_operator_key_format(tmp_path: Path) -> None:
    cfg = {"operators": {"HPC.default": {"kind": "hpc"}}}  # uppercase key
    with pytest.raises(OperatorsConfigError, match="must be lowercase canonical form"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_operator_key_kind_mismatch(tmp_path: Path) -> None:
    cfg = {"operators": {"hpc.default": {"kind": "local"}}}
    with pytest.raises(OperatorsConfigError, match="does not match config kind"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_unknown_kind(tmp_path: Path) -> None:
    cfg = {"operators": {"hpc.default": {"kind": "cloud"}}}
    with pytest.raises(OperatorsConfigError, match="Input should be"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_non_compute_kind_with_backend(tmp_path: Path) -> None:
    cfg = {"operators": {"human.default": {"kind": "human", "backend": {"type": "local"}}}}
    with pytest.raises(OperatorsConfigError, match="must not define 'backend'"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_local_kind_with_hpc_yaml_backend(tmp_path: Path) -> None:
    cfg = {"operators": {"local.default": {"kind": "local", "backend": {"type": "hpc_yaml", "path": "x.yaml"}}}}
    with pytest.raises(OperatorsConfigError, match="only valid for kind='hpc'"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_unknown_backend_type(tmp_path: Path) -> None:
    cfg = {"operators": {"hpc.default": {"kind": "hpc", "backend": {"type": "unknown"}}}}
    # Pydantic v2 discriminator message (stable-enough substring)
    with pytest.raises(OperatorsConfigError, match="does not match any of the expected tags"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_rejects_extra_fields(tmp_path: Path) -> None:
    cfg = {"operators": {"hpc.default": {"kind": "hpc", "nope": 1}}}
    # Pydantic v2 extra-forbid message
    with pytest.raises(OperatorsConfigError, match="Extra inputs are not permitted"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")