"""
Attempt resolution utilities for operators.

Extracts attempt discovery and path resolution logic from operators to reduce
complexity and improve testability.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from matterstack.runtime.fs_safety import attempt_evidence_dir, operator_run_dir

if TYPE_CHECKING:
    from matterstack.storage.state_store import SQLiteStateStore

logger = logging.getLogger(__name__)


@dataclass
class AttemptContext:
    """
    Resolved attempt context for a task execution.

    Attributes:
        attempt_id: The resolved attempt ID, or None if using legacy layout.
        store: The state store instance used for resolution (may be None).
        full_path: The absolute path for storing execution evidence.
        relative_path: The path relative to run root.
        operator_uuid: For legacy layout, the unique operator instance ID.
    """

    attempt_id: Optional[str]
    store: Optional["SQLiteStateStore"]
    full_path: Path
    relative_path: Path
    operator_uuid: Optional[str]

    @property
    def is_attempt_aware(self) -> bool:
        """Returns True if using attempt-aware layout, False for legacy."""
        return self.attempt_id is not None


def resolve_attempt_id(
    db_path: Path,
    task_id: str,
) -> tuple[Optional[str], Optional["SQLiteStateStore"]]:
    """
    Resolve the current attempt ID for a task from the state store.

    Args:
        db_path: Path to the SQLite database file.
        task_id: The task ID to resolve the attempt for.

    Returns:
        A tuple of (attempt_id, store) where:
        - attempt_id is the resolved ID or None if unavailable
        - store is the SQLiteStateStore instance (for reuse) or None on failure
    """
    from matterstack.storage.state_store import SQLiteStateStore

    try:
        store = SQLiteStateStore(db_path)
        attempt = store.get_current_attempt(task_id)
        if attempt is not None:
            return attempt.attempt_id, store
        return None, store
    except Exception as e:
        # Back-compat: do not fail if state store is unavailable
        logger.debug(f"Could not resolve attempt_id for task {task_id}: {e}")
        return None, None


def resolve_attempt_context(
    run_root: Path,
    db_path: Path,
    task_id: str,
    operator_slug: str,
) -> AttemptContext:
    """
    Resolve the full attempt context for task execution.

    This determines whether to use attempt-aware layout or legacy operator layout,
    and resolves the appropriate paths.

    Args:
        run_root: The root path of the run.
        db_path: Path to the SQLite database file.
        task_id: The task ID to resolve context for.
        operator_slug: The operator slug (used for legacy layout).

    Returns:
        AttemptContext containing all resolved information.

    Layout behavior:
        - Attempt-aware (preferred): runs/<run_id>/tasks/<task_id>/attempts/<attempt_id>/
        - Legacy fallback: runs/<run_id>/operators/<slug>/<operator_uuid>/
    """
    attempt_id, store = resolve_attempt_id(db_path, task_id)

    operator_uuid: Optional[str] = None

    if attempt_id:
        full_path = attempt_evidence_dir(run_root, task_id, attempt_id)
    else:
        # Legacy behavior: unique operator instance dir
        operator_uuid = str(uuid.uuid4())
        full_path = operator_run_dir(run_root, operator_slug, operator_uuid)

    relative_path = full_path.relative_to(run_root.resolve())

    return AttemptContext(
        attempt_id=attempt_id,
        store=store,
        full_path=full_path,
        relative_path=relative_path,
        operator_uuid=operator_uuid,
    )


def get_or_create_store(
    context: AttemptContext,
    db_path: Path,
) -> Optional["SQLiteStateStore"]:
    """
    Get the store from context or create a new one if needed.

    Args:
        context: The attempt context (may contain a cached store).
        db_path: Path to the SQLite database file.

    Returns:
        SQLiteStateStore instance or None on failure.
    """
    if context.store is not None:
        return context.store

    from matterstack.storage.state_store import SQLiteStateStore

    try:
        return SQLiteStateStore(db_path)
    except Exception as e:
        logger.debug(f"Could not create state store: {e}")
        return None
