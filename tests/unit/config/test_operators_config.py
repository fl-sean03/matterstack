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


# ============================================================================
# Per-Operator Concurrency Limit Tests (v0.2.6+)
# ============================================================================


def test_parse_operators_config_with_max_concurrent(tmp_path: Path) -> None:
    """Test that max_concurrent is parsed correctly on operators."""
    cfg = {
        "operators": {
            "hpc.gpu": {
                "kind": "hpc",
                "max_concurrent": 5,
            },
            "hpc.cpu": {
                "kind": "hpc",
                "max_concurrent": 30,
            },
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert parsed.operators["hpc.gpu"].max_concurrent == 5
    assert parsed.operators["hpc.cpu"].max_concurrent == 30


def test_parse_operators_config_max_concurrent_null(tmp_path: Path) -> None:
    """Test that max_concurrent=null is allowed (means inherit from global)."""
    cfg = {
        "operators": {
            "human.default": {
                "kind": "human",
                "max_concurrent": None,  # Inherit from global
            },
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert parsed.operators["human.default"].max_concurrent is None


def test_parse_operators_config_max_concurrent_omitted(tmp_path: Path) -> None:
    """Test that omitting max_concurrent defaults to None (inherit from global)."""
    cfg = {
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert parsed.operators["hpc.default"].max_concurrent is None


def test_parse_operators_config_max_concurrent_zero_rejected(tmp_path: Path) -> None:
    """Test that max_concurrent=0 is rejected (must be positive or null)."""
    cfg = {
        "operators": {
            "hpc.default": {
                "kind": "hpc",
                "max_concurrent": 0,
            },
        }
    }
    with pytest.raises(OperatorsConfigError, match="must be a positive integer"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_max_concurrent_negative_rejected(tmp_path: Path) -> None:
    """Test that negative max_concurrent is rejected."""
    cfg = {
        "operators": {
            "hpc.default": {
                "kind": "hpc",
                "max_concurrent": -5,
            },
        }
    }
    with pytest.raises(OperatorsConfigError, match="must be a positive integer"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_with_defaults_section(tmp_path: Path) -> None:
    """Test that defaults section with max_concurrent_global is parsed correctly."""
    cfg = {
        "defaults": {
            "max_concurrent_global": 50,
        },
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert parsed.defaults.max_concurrent_global == 50


def test_parse_operators_config_missing_defaults_uses_empty(tmp_path: Path) -> None:
    """Test that missing defaults section creates empty DefaultsConfig."""
    cfg = {
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")
    assert parsed.defaults.max_concurrent_global is None


def test_parse_operators_config_defaults_global_zero_rejected(tmp_path: Path) -> None:
    """Test that max_concurrent_global=0 is rejected."""
    cfg = {
        "defaults": {
            "max_concurrent_global": 0,
        },
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    with pytest.raises(OperatorsConfigError, match="must be a positive integer"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_defaults_global_negative_rejected(tmp_path: Path) -> None:
    """Test that negative max_concurrent_global is rejected."""
    cfg = {
        "defaults": {
            "max_concurrent_global": -10,
        },
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    with pytest.raises(OperatorsConfigError, match="must be a positive integer"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_defaults_extra_fields_rejected(tmp_path: Path) -> None:
    """Test that extra fields in defaults section are rejected."""
    cfg = {
        "defaults": {
            "max_concurrent_global": 50,
            "unknown_field": "bad",
        },
        "operators": {
            "hpc.default": {"kind": "hpc"},
        }
    }
    with pytest.raises(OperatorsConfigError, match="Extra inputs are not permitted"):
        parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")


def test_parse_operators_config_complete_example(tmp_path: Path) -> None:
    """Test a complete example with defaults and multiple operators with different limits."""
    cfg = {
        "defaults": {
            "max_concurrent_global": 50,
        },
        "operators": {
            "hpc.gpu": {
                "kind": "hpc",
                "max_concurrent": 5,  # GPU queue limited
            },
            "hpc.cpu": {
                "kind": "hpc",
                "max_concurrent": 30,  # CPU queue higher capacity
            },
            "human.default": {
                "kind": "human",
                "max_concurrent": 1000,  # High capacity for human operators
            },
            "local.default": {
                "kind": "local",
                # No max_concurrent -> inherits global (50)
            },
        }
    }
    parsed = parse_operators_config_dict(cfg, path=tmp_path / "operators.yaml")

    # Verify defaults
    assert parsed.defaults.max_concurrent_global == 50

    # Verify operator limits
    assert parsed.operators["hpc.gpu"].max_concurrent == 5
    assert parsed.operators["hpc.cpu"].max_concurrent == 30
    assert parsed.operators["human.default"].max_concurrent == 1000  # High capacity
    assert parsed.operators["local.default"].max_concurrent is None  # Will inherit global
