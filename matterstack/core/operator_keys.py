from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol, Tuple

# Canonical operator key: "{kind}.{name}"
# - kind: starts with [a-z], then [a-z0-9_]*
# - name: starts with [a-z0-9], then [a-z0-9_.-]*
# This permits names like "default", "dev", "prod", "clusterA.dev".
_OPERATOR_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*\.[a-z0-9][a-z0-9_.-]*$")


LEGACY_OPERATOR_TYPE_TO_KEY = {
    "hpc": "hpc.default",
    "local": "local.default",
    "human": "human.default",
    "experiment": "experiment.default",
}


class AttemptLike(Protocol):
    """
    Minimal protocol for resolving operator routing from attempts.

    Intended to match SQLAlchemy TaskAttemptModel fields without importing ORM types.
    """

    operator_key: Optional[str]
    operator_type: Optional[str]
    operator_data: Optional[Mapping[str, Any]]


@dataclass(frozen=True)
class ResolvedOperatorKey:
    operator_key: str
    source: str  # e.g. "attempt.operator_key", "attempt.operator_data.operator_key", "attempt.operator_type(mapped)"


def is_canonical_operator_key(value: str) -> bool:
    """
    Return True iff value matches the canonical operator key format.

    See: [`matterstack/core/operator_keys.py`](matterstack/core/operator_keys.py:1)
    """
    return bool(_OPERATOR_KEY_RE.match(value))


def normalize_operator_key(value: str) -> str:
    """
    Normalize and validate a canonical operator key.

    Rules:
    - trim whitespace
    - lowercase
    - must match canonical regex
    - reject internal whitespace
    - reject '..' to avoid ambiguous hierarchical names

    Raises:
        ValueError if invalid.
    """
    raw = str(value).strip().lower()

    if not raw:
        raise ValueError("operator_key is empty")

    if any(ch.isspace() for ch in raw):
        raise ValueError(f"operator_key must not contain whitespace: {value!r}")

    if ".." in raw:
        raise ValueError(f"operator_key must not contain '..': {value!r}")

    if not is_canonical_operator_key(raw):
        raise ValueError(f"operator_key must match kind.name with allowed characters; got {value!r}")

    return raw


def split_operator_key(operator_key: str) -> Tuple[str, str]:
    """
    Split a canonical operator key into (kind, name) using the first '.'.

    Raises:
        ValueError if the key is invalid.
    """
    normalized = normalize_operator_key(operator_key)
    kind, name = normalized.split(".", 1)
    if not kind or not name:
        raise ValueError(f"operator_key missing kind or name: {operator_key!r}")
    return kind, name


def legacy_operator_type_to_key(operator_type: Optional[str]) -> Optional[str]:
    """
    Convert legacy operator_type (v0.2.5) to canonical operator_key.

    - Case-insensitive mapping for "HPC", "Local", "Human", "Experiment".
    - If operator_type already matches canonical key format, return it normalized.
    """
    if operator_type is None:
        return None

    raw = str(operator_type).strip()
    if not raw:
        return None

    # If operator_type is already a canonical key, treat it as such.
    lowered = raw.lower()
    if is_canonical_operator_key(lowered):
        return normalize_operator_key(lowered)

    return LEGACY_OPERATOR_TYPE_TO_KEY.get(lowered)


def resolve_operator_key_for_attempt(attempt: AttemptLike) -> Optional[ResolvedOperatorKey]:
    """
    Resolve canonical operator_key for an attempt, implementing the v0.2.6 precedence:

      1) attempt.operator_key (schema v3)
      2) attempt.operator_data["operator_key"] (transition)
      3) attempt.operator_type:
         - if canonical key => use it
         - else legacy map (HPC => hpc.default)

    Returns:
        ResolvedOperatorKey or None if not resolvable.

    Notes:
        This function intentionally does NOT consult workspace/CLI defaults; those are
        registry/config responsibilities in downstream subtasks.
    """
    # 1) Schema v3 column
    if getattr(attempt, "operator_key", None):
        try:
            ok = normalize_operator_key(str(attempt.operator_key))
            return ResolvedOperatorKey(operator_key=ok, source="attempt.operator_key")
        except Exception:
            # Treat invalid as not present; orchestrator will surface error at a higher level.
            pass

    # 2) JSON payload
    op_data = getattr(attempt, "operator_data", None) or {}
    if isinstance(op_data, Mapping):
        from_json = op_data.get("operator_key")
        if isinstance(from_json, str) and from_json.strip():
            try:
                ok = normalize_operator_key(from_json)
                return ResolvedOperatorKey(operator_key=ok, source="attempt.operator_data.operator_key")
            except Exception:
                pass

    # 3) Legacy operator_type
    derived = legacy_operator_type_to_key(getattr(attempt, "operator_type", None))
    if derived:
        return ResolvedOperatorKey(operator_key=derived, source="attempt.operator_type")

    return None
