from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest

from matterstack.core.operator_keys import (
    is_canonical_operator_key,
    legacy_operator_type_to_key,
    normalize_operator_key,
    resolve_operator_key_for_attempt,
    split_operator_key,
)


@dataclass
class _Attempt:
    operator_key: Optional[str] = None
    operator_type: Optional[str] = None
    operator_data: Optional[Dict[str, Any]] = None


def test_is_canonical_operator_key_accepts_expected_examples() -> None:
    assert is_canonical_operator_key("hpc.default")
    assert is_canonical_operator_key("human.default")
    assert is_canonical_operator_key("local.dev")
    assert is_canonical_operator_key("hpc.clusterA.dev".lower())  # kind=hpc, name=clustera.dev


def test_is_canonical_operator_key_rejects_invalid_examples() -> None:
    assert not is_canonical_operator_key("HPC.default")  # uppercase not canonical
    assert not is_canonical_operator_key("hpc")  # missing dot
    assert not is_canonical_operator_key(".default")
    assert not is_canonical_operator_key("hpc.")
    assert not is_canonical_operator_key("hpc default")  # whitespace
    assert not is_canonical_operator_key("hpc/default")  # invalid char
    assert not is_canonical_operator_key("hpc..default")  # double dot


def test_normalize_operator_key_lowercases_and_trims() -> None:
    assert normalize_operator_key("  HPC.default  ") == "hpc.default"
    assert normalize_operator_key("local.DEV") == "local.dev"


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "hpc",  # missing dot
        "hpc.",  # missing name
        ".default",  # missing kind
        "hpc default",  # whitespace
        "hpc..default",  # ambiguous
        "hpc/default",  # invalid char
        "HPC.DEFAULT!!",  # invalid chars
    ],
)
def test_normalize_operator_key_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        normalize_operator_key(value)


def test_split_operator_key_splits_on_first_dot() -> None:
    kind, name = split_operator_key("hpc.default")
    assert kind == "hpc"
    assert name == "default"

    kind2, name2 = split_operator_key("hpc.clusterA.dev")
    assert kind2 == "hpc"
    assert name2 == "clustera.dev"


def test_legacy_operator_type_to_key_maps_known_legacy_types() -> None:
    assert legacy_operator_type_to_key("HPC") == "hpc.default"
    assert legacy_operator_type_to_key("Local") == "local.default"
    assert legacy_operator_type_to_key("Human") == "human.default"
    assert legacy_operator_type_to_key("Experiment") == "experiment.default"


def test_legacy_operator_type_to_key_accepts_canonical_key_input() -> None:
    assert legacy_operator_type_to_key("hpc.default") == "hpc.default"
    assert legacy_operator_type_to_key("HPC.DEFAULT") == "hpc.default"


def test_resolve_operator_key_for_attempt_precedence_operator_key_wins() -> None:
    a = _Attempt(
        operator_key="hpc.dev",
        operator_type="HPC",
        operator_data={"operator_key": "hpc.default"},
    )
    resolved = resolve_operator_key_for_attempt(a)
    assert resolved is not None
    assert resolved.operator_key == "hpc.dev"
    assert resolved.source == "attempt.operator_key"


def test_resolve_operator_key_for_attempt_precedence_operator_data_second() -> None:
    a = _Attempt(operator_key=None, operator_type="HPC", operator_data={"operator_key": "human.default"})
    resolved = resolve_operator_key_for_attempt(a)
    assert resolved is not None
    assert resolved.operator_key == "human.default"
    assert resolved.source == "attempt.operator_data.operator_key"


def test_resolve_operator_key_for_attempt_operator_type_canonical_passthrough() -> None:
    a = _Attempt(operator_key=None, operator_type="LOCAL.DEFAULT", operator_data={})
    resolved = resolve_operator_key_for_attempt(a)
    assert resolved is not None
    assert resolved.operator_key == "local.default"
    assert resolved.source == "attempt.operator_type"


def test_resolve_operator_key_for_attempt_operator_type_legacy_map() -> None:
    a = _Attempt(operator_key=None, operator_type="Experiment", operator_data={})
    resolved = resolve_operator_key_for_attempt(a)
    assert resolved is not None
    assert resolved.operator_key == "experiment.default"
    assert resolved.source == "attempt.operator_type"


def test_resolve_operator_key_for_attempt_returns_none_when_unresolvable() -> None:
    a = _Attempt(operator_key=None, operator_type=None, operator_data={})
    assert resolve_operator_key_for_attempt(a) is None