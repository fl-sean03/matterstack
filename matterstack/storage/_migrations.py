"""
Schema migration mixin for SQLiteStateStore.

This module contains migration logic for upgrading database schemas:
- v1 → v2: Adds task_attempts table and tasks.current_attempt_id
- v2 → v3: Adds task_attempts.operator_key column
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Dict

from sqlalchemy import insert, select, text, update
from sqlalchemy.orm import Session

from matterstack.storage.schema import (
    Base,
    ExternalRunModel,
    RunModel,
    SchemaInfo,
    TaskAttemptModel,
    TaskModel,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

# Deterministic namespace for v1 -> v2 migration attempt_id backfill.
# attempt_id = uuid5(namespace, f"{run_id}:{task_id}")
TASK_ATTEMPT_MIGRATION_NAMESPACE = uuid.UUID("6df7afdd-8f9f-4b0c-9a2c-6f2b0c2b2b1a")

logger = logging.getLogger(__name__)


class _MigrationsMixin:
    """
    Mixin class providing schema migration methods for SQLiteStateStore.
    
    Expects the following attributes on self:
    - db_path: Path to the SQLite database file
    - engine: SQLAlchemy Engine instance
    """

    # Type hints for attributes provided by SQLiteStateStore
    if TYPE_CHECKING:
        from pathlib import Path
        from sqlalchemy.orm import sessionmaker
        db_path: Path
        engine: Engine
        SessionLocal: sessionmaker

    def _sqlite_table_has_column(self, session: Session, table: str, column: str) -> bool:
        """Check if a SQLite table has a specific column."""
        rows = session.execute(text(f"PRAGMA table_info({table})")).all()
        # PRAGMA table_info returns rows where column name is index 1
        return any(r[1] == column for r in rows)

    def _migrate_schema_v1_to_v2(self, session: Session, info: SchemaInfo) -> None:
        """
        Additive, non-destructive migration from schema v1 -> v2.

        - Creates `task_attempts` table if missing (create_all is additive)
        - Adds `tasks.current_attempt_id` if missing
        - Backfills one attempt per existing `external_runs` row
        - Sets tasks.current_attempt_id for tasks that had an external_run
        - Leaves v1 tables/data intact
        """
        logger.info(f"Migrating database schema v1 -> v2 at {self.db_path}")

        # Ensure v2 tables exist (additive)
        Base.metadata.create_all(self.engine)

        # Ensure v2 column exists on tasks table (SQLite requires ALTER TABLE)
        if not self._sqlite_table_has_column(session, "tasks", "current_attempt_id"):
            session.execute(text("ALTER TABLE tasks ADD COLUMN current_attempt_id VARCHAR"))

        # Build run_id -> created_at map (stable created_at for migrated attempts)
        run_created_at: Dict[str, datetime] = {
            run_id: created_at
            for run_id, created_at in session.execute(
                select(RunModel.run_id, RunModel.created_at)
            ).all()
        }

        external_runs = session.execute(select(ExternalRunModel)).scalars().all()

        for er in external_runs:
            attempt_id = str(
                uuid.uuid5(
                    TASK_ATTEMPT_MIGRATION_NAMESPACE, f"{er.run_id}:{er.task_id}"
                )
            )

            created_at = run_created_at.get(er.run_id)

            ins = (
                insert(TaskAttemptModel)
                .values(
                    attempt_id=attempt_id,
                    task_id=er.task_id,
                    run_id=er.run_id,
                    attempt_index=1,
                    status=er.status,
                    operator_type=er.operator_type,
                    external_id=er.external_id,
                    operator_data=er.operator_data,
                    relative_path=er.relative_path,
                    created_at=created_at,
                    submitted_at=None,
                    ended_at=None,
                    status_reason=None,
                )
                .prefix_with("OR IGNORE")
            )
            session.execute(ins)

            # Set current attempt pointer (safe after ALTER TABLE)
            session.execute(
                update(TaskModel)
                .where(TaskModel.task_id == er.task_id)
                .values(current_attempt_id=attempt_id)
            )

        info.value = "2"
        session.commit()

    def _migrate_schema_v2_to_v3(self, session: Session, info: SchemaInfo) -> None:
        """
        Additive, non-destructive migration from schema v2 -> v3.

        v3 adds `task_attempts.operator_key` (nullable) to persist canonical operator routing keys
        (e.g. "hpc.default") for provenance and CLI/evidence stability.
        """
        # Import here to avoid circular imports - CURRENT_SCHEMA_VERSION is in state_store.py
        from matterstack.storage.state_store import CURRENT_SCHEMA_VERSION

        logger.info(f"Migrating database schema v2 -> v3 at {self.db_path}")

        # Ensure latest tables exist for *new* DBs (additive; does not add columns on existing tables).
        Base.metadata.create_all(self.engine)

        if not self._sqlite_table_has_column(session, "task_attempts", "operator_key"):
            session.execute(text("ALTER TABLE task_attempts ADD COLUMN operator_key VARCHAR"))

        info.value = CURRENT_SCHEMA_VERSION
        session.commit()
