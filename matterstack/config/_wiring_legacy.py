"""
Internal legacy support for operator wiring.

This module contains functions for generating operator wiring configurations
from legacy CLI inputs (--hpc-config, --profile).
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import yaml

from ._wiring_types import OperatorWiringSource


def _generate_legacy_operators_yaml_bytes(
    *,
    legacy_hpc_config_path: Optional[str],
    legacy_profile: Optional[str],
) -> Tuple[OperatorWiringSource, Optional[str], bytes]:
    """
    Generate a minimal operators.yaml snapshot from legacy CLI inputs.

    Returns: (source, resolved_path, snapshot_bytes)
    """
    if legacy_hpc_config_path:
        source = OperatorWiringSource.LEGACY_HPC_CONFIG
        resolved = legacy_hpc_config_path
        hpc_backend: Dict[str, Any] = {"type": "hpc_yaml", "path": legacy_hpc_config_path}
    elif legacy_profile:
        source = OperatorWiringSource.LEGACY_PROFILE
        resolved = legacy_profile
        hpc_backend = {"type": "profile", "name": legacy_profile}
    else:
        raise ValueError("Legacy snapshot generation requested without legacy inputs.")

    operators_doc = {
        "operators": {
            "human.default": {"kind": "human"},
            "experiment.default": {"kind": "experiment"},
            "local.default": {"kind": "local", "backend": {"type": "local"}},
            "hpc.default": {"kind": "hpc", "backend": hpc_backend},
        }
    }

    # Stable, human-readable YAML for hashing/provenance.
    snapshot_text = yaml.safe_dump(operators_doc, sort_keys=True)
    return source, resolved, snapshot_text.encode("utf-8")
