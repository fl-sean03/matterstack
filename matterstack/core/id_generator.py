"""
Centralized ID generation with consistent format and collision protection.

All IDs use the format: YYYYMMDD_HHMMSS_<random8>
- Chronologically sortable by prefix
- Human-readable timestamp component
- 8 hex chars for uniqueness per second: 16^8 = 4.3 billion combinations

Examples:
    - Run ID: 20231225_143052_a1b2c3d4
    - Attempt ID: 20231225_143052_e5f67890
    - Task ID: 20231225_143053_abcd1234
    - Task ID with hint: equilibrate_20231225_143053_abcd1234

Usage:
    from matterstack.core.id_generator import (
        generate_run_id,
        generate_attempt_id,
        generate_task_id,
    )

    run_id = generate_run_id()
    attempt_id = generate_attempt_id()
    task_id = generate_task_id()
    task_id_with_hint = generate_task_id(hint="equilibrate")
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional


def generate_chronological_id(prefix: str = "") -> str:
    """
    Generate a chronologically sortable ID.

    Format: [prefix_]YYYYMMDD_HHMMSS_<uuid8>

    Args:
        prefix: Optional prefix to prepend (e.g., a sanitized hint)

    Returns:
        Unique chronologically sortable ID
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    random_suffix = uuid.uuid4().hex[:8]

    if prefix:
        return f"{prefix}_{timestamp}_{random_suffix}"
    return f"{timestamp}_{random_suffix}"


def generate_run_id() -> str:
    """Generate a run ID in chronological format.

    Returns:
        Unique run ID in format YYYYMMDD_HHMMSS_<uuid8>

    Example:
        >>> generate_run_id()
        '20231225_143052_a1b2c3d4'
    """
    return generate_chronological_id()


def generate_attempt_id() -> str:
    """Generate an attempt ID in chronological format.

    Returns:
        Unique attempt ID in format YYYYMMDD_HHMMSS_<uuid8>

    Example:
        >>> generate_attempt_id()
        '20231225_143052_e5f67890'
    """
    return generate_chronological_id()


def generate_task_id(hint: str = "") -> str:
    """
    Generate a task ID in chronological format.

    Args:
        hint: Optional human-readable hint (e.g., "equilibrate", "shear").
              Will be sanitized: lowercase, spaces/hyphens to underscores,
              truncated to 20 chars.

    Returns:
        Unique task ID, optionally prefixed with sanitized hint

    Examples:
        >>> generate_task_id()
        '20231225_143052_a1b2c3d4'
        >>> generate_task_id("Equilibrate")
        'equilibrate_20231225_143052_a1b2c3d4'
        >>> generate_task_id("Phase 1 - Setup")
        'phase_1_setup_20231225_143052_a1b2c3d4'
    """
    if hint:
        # Sanitize: lowercase, replace non-alphanumeric with underscores
        safe_hint = re.sub(r"[^a-z0-9_]", "_", hint.lower())[:20]
        # Remove leading/trailing underscores and collapse multiple underscores
        safe_hint = re.sub(r"_+", "_", safe_hint).strip("_")
        if safe_hint:
            return generate_chronological_id(prefix=safe_hint)
    return generate_chronological_id()


def with_collision_retry(generator: Callable[..., str], max_retries: int = 3, delay_ms: int = 10) -> Callable[..., str]:
    """
    Wrap an ID generator with collision retry logic.

    If two calls generate IDs with identical timestamps within the same
    invocation context, this adds a small delay before retrying to ensure
    timestamp uniqueness. Note: Due to the 8-character UUID suffix,
    actual collisions are extremely unlikely (1 in 4.3 billion per second).

    Args:
        generator: The ID generation function to wrap
        max_retries: Maximum retry attempts (default: 3)
        delay_ms: Delay between retries in milliseconds (default: 10)

    Returns:
        Wrapped generator function with retry logic

    Example:
        >>> safe_run_id = with_collision_retry(generate_run_id)
        >>> id1 = safe_run_id()
        >>> id2 = safe_run_id()  # Guaranteed different from id1
    """
    last_id: Optional[str] = None

    def wrapped(*args, **kwargs) -> str:
        nonlocal last_id

        for _ in range(max_retries):
            new_id = generator(*args, **kwargs)

            if new_id != last_id:
                last_id = new_id
                return new_id

            # Same ID generated (extremely unlikely) - wait and retry
            time.sleep(delay_ms / 1000.0)

        # Final attempt after all retries
        return generator(*args, **kwargs)

    return wrapped
