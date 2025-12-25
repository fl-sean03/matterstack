"""
Run CRUD operations mixin for SQLiteStateStore.

This module contains methods for creating, retrieving, and updating run records.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from sqlalchemy import select

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.storage.schema import RunModel

if TYPE_CHECKING:
    from sqlalchemy.orm import sessionmaker


class _RunOperationsMixin:
    """
    Mixin class providing run CRUD operations for SQLiteStateStore.

    Expects the following attributes on self:
    - SessionLocal: SQLAlchemy sessionmaker instance
    """

    # Type hints for attributes provided by SQLiteStateStore
    if TYPE_CHECKING:
        SessionLocal: sessionmaker

    def create_run(self, handle: RunHandle, metadata: Optional[RunMetadata] = None) -> None:
        """
        Create a new run record.
        """
        if metadata is None:
            metadata = RunMetadata()

        run_model = RunModel(
            run_id=handle.run_id,
            workspace_slug=handle.workspace_slug,
            root_path=str(handle.root_path),
            created_at=metadata.created_at,
            status=metadata.status,
            tags=metadata.tags,
            description=metadata.description,
        )

        with self.SessionLocal() as session:
            session.add(run_model)
            session.commit()

    def get_run(self, run_id: str) -> Optional[RunHandle]:
        """
        Retrieve a run handle by ID.
        Note: Returns just the handle part, not the full metadata for now.
        """
        with self.SessionLocal() as session:
            stmt = select(RunModel).where(RunModel.run_id == run_id)
            run_model = session.scalar(stmt)

            if not run_model:
                return None

            return RunHandle(
                run_id=run_model.run_id, workspace_slug=run_model.workspace_slug, root_path=Path(run_model.root_path)
            )

    def get_run_metadata(self, run_id: str) -> Optional[RunMetadata]:
        """
        Retrieve full run metadata by ID.
        """
        with self.SessionLocal() as session:
            stmt = select(RunModel).where(RunModel.run_id == run_id)
            run_model = session.scalar(stmt)

            if not run_model:
                return None

            return RunMetadata(
                created_at=run_model.created_at,
                status=run_model.status,
                tags=run_model.tags,
                description=run_model.description,
            )

    def get_run_status_reason(self, run_id: str) -> Optional[str]:
        """
        Get the status reason (e.g., error message) for a run.
        """
        with self.SessionLocal() as session:
            stmt = select(RunModel.status_reason).where(RunModel.run_id == run_id)
            return session.scalar(stmt)

    def set_run_status(self, run_id: str, status: str, reason: Optional[str] = None) -> None:
        """
        Update the status of a run.
        """
        with self.SessionLocal() as session:
            stmt = select(RunModel).where(RunModel.run_id == run_id)
            run_model = session.scalar(stmt)

            if not run_model:
                raise ValueError(f"Run {run_id} not found.")

            run_model.status = status
            if reason is not None:
                run_model.status_reason = reason

            session.commit()

    def get_run_status(self, run_id: str) -> Optional[str]:
        """
        Get the current status of a run.
        """
        with self.SessionLocal() as session:
            stmt = select(RunModel.status).where(RunModel.run_id == run_id)
            return session.scalar(stmt)
