"""
SQLite-based state store for MatterStack runs.

This module provides the SQLiteStateStore class, which is the main persistence
layer for MatterStack. The class is composed of several mixin classes that
provide specialized operations:

- _MigrationsMixin: Schema migrations (v1→v2→v3→v4)
- _RunOperationsMixin: Run CRUD operations
- _TaskOperationsMixin: Task CRUD operations
- _ExternalRunOperationsMixin: External run operations (v1 legacy)
- _AttemptOperationsMixin: Task attempt operations (v2)
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from matterstack.storage._attempt_operations import _AttemptOperationsMixin
from matterstack.storage._external_run_ops import _ExternalRunOperationsMixin
from matterstack.storage._migrations import (
    TASK_ATTEMPT_MIGRATION_NAMESPACE,
    _MigrationsMixin,
)
from matterstack.storage._run_operations import _RunOperationsMixin
from matterstack.storage._task_operations import _TaskOperationsMixin
from matterstack.storage.schema import Base, SchemaInfo

CURRENT_SCHEMA_VERSION = "4"

# Re-export for backward compatibility
__all__ = ["SQLiteStateStore", "CURRENT_SCHEMA_VERSION", "TASK_ATTEMPT_MIGRATION_NAMESPACE"]

logger = logging.getLogger(__name__)


class SQLiteStateStore(
    _MigrationsMixin,
    _RunOperationsMixin,
    _TaskOperationsMixin,
    _ExternalRunOperationsMixin,
    _AttemptOperationsMixin,
):
    """
    Persistence layer for MatterStack runs using SQLite.
    
    This class provides methods for:
    - Run management: create, get, update status
    - Task management: add workflow, get tasks, update status
    - External run tracking (v1 legacy): register, update, get active
    - Task attempts (v2): create, list, update, get current
    - Schema migrations: automatic upgrades from v1 to v4
    - File locking: exclusive access for concurrent process safety
    """

    def __init__(self, db_path: Path):
        """
        Initialize the state store.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        # Ensure parent directory exists
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.engine = create_engine(f"sqlite:///{self.db_path}", echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Initialize schema if file is new. For existing DBs, this is additive only.
        Base.metadata.create_all(self.engine)

        # Check schema version (and migrate if needed)
        self._check_schema()

        logger.debug(f"Initialized SQLiteStateStore at {self.db_path}")

    def _check_schema(self) -> None:
        """
        Check that the database schema version matches the code.
        If missing, initialize it.
        If mismatch, migrate if supported, else raise error.

        Schema versions:
        - v1: external_runs only
        - v2: task_attempts + tasks.current_attempt_id
        - v3: task_attempts.operator_key (canonical routing key)
        - v4: tasks.operator_key (first-class operator routing)
        """
        with self.SessionLocal() as session:
            stmt = select(SchemaInfo).where(SchemaInfo.key == "version")
            info = session.scalar(stmt)

            if not info:
                # Initialize version
                logger.info(
                    f"Initializing database schema v{CURRENT_SCHEMA_VERSION} at {self.db_path}"
                )
                info = SchemaInfo(key="version", value=CURRENT_SCHEMA_VERSION)
                session.add(info)
                session.commit()
                return

            if info.value == CURRENT_SCHEMA_VERSION:
                return

            # Supported additive migrations
            if info.value == "1" and CURRENT_SCHEMA_VERSION in {"2", "3", "4"}:
                self._migrate_schema_v1_to_v2(session, info)
                # If code expects v3+, immediately migrate onward.
                if CURRENT_SCHEMA_VERSION in {"3", "4"}:
                    self._migrate_schema_v2_to_v3(session, info)
                if CURRENT_SCHEMA_VERSION == "4":
                    self._migrate_schema_v3_to_v4(session, info)
                return

            if info.value == "2" and CURRENT_SCHEMA_VERSION in {"3", "4"}:
                self._migrate_schema_v2_to_v3(session, info)
                if CURRENT_SCHEMA_VERSION == "4":
                    self._migrate_schema_v3_to_v4(session, info)
                return

            if info.value == "3" and CURRENT_SCHEMA_VERSION == "4":
                self._migrate_schema_v3_to_v4(session, info)
                return

            raise RuntimeError(
                f"Schema version mismatch: Database is v{info.value}, "
                f"Code expects v{CURRENT_SCHEMA_VERSION}"
            )

    @contextlib.contextmanager
    def lock(self) -> Generator[None, None, None]:
        """
        Acquire an exclusive lock on the run directory.
        This prevents multiple processes from modifying the state concurrently.
        """
        lock_path = self.db_path.parent / "run.lock"
        # Open in append mode to ensure creation if not exists, but don't truncate
        with open(lock_path, "a") as f:
            try:
                # Try to acquire exclusive lock. Non-blocking.
                logger.debug(f"Acquiring lock on {lock_path}")
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug(f"Lock acquired on {lock_path}")
                yield
            except BlockingIOError:
                logger.warning(f"Failed to acquire lock on {lock_path}")
                raise RuntimeError(f"Could not acquire lock on {lock_path}. Another process is running.")
            finally:
                # Always unlock
                try:
                    logger.debug(f"Releasing lock on {lock_path}")
                    fcntl.flock(f, fcntl.LOCK_UN)
                except Exception:
                    pass
