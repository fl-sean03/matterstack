from __future__ import annotations
from typing import Optional

from ...core.backend import ComputeBackend

def create_backend_from_profile(profile_name: str, config_path: Optional[str] = None) -> ComputeBackend:
    """
    Factory function to create a ComputeBackend instance from a named profile.

    Args:
        profile_name: The name of the profile to load.
        config_path: Optional path to a configuration file. If not provided,
                     defaults are used (see matterstack.config.profiles.load_profile).

    Returns:
        An instance of a ComputeBackend (e.g., LocalBackend, SlurmBackend).

    Raises:
        KeyError: If the profile is not found.
        ValueError: If the profile configuration is invalid.
    """
    from ...config.profiles import load_profile
    profile = load_profile(profile_name, config_path)
    return profile.create_backend()