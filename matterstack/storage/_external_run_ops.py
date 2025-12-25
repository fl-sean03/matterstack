"""
External run operations mixin for SQLiteStateStore (v1 legacy).

This module contains methods for managing external run records.
Note: This is the v1 schema approach; v2 uses task_attempts instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import select, update

from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.storage.schema import ExternalRunModel

if TYPE_CHECKING:
    from sqlalchemy.orm import sessionmaker


class _ExternalRunOperationsMixin:
    """
    Mixin class providing external run operations for SQLiteStateStore (v1 legacy).

    Expects the following attributes on self:
    - SessionLocal: SQLAlchemy sessionmaker instance
    """

    # Type hints for attributes provided by SQLiteStateStore
    if TYPE_CHECKING:
        SessionLocal: sessionmaker

    def register_external_run(self, handle: ExternalRunHandle, run_id: str) -> None:
        """
        Register a new external run (operator execution).
        """
        with self.SessionLocal() as session:
            # Check if exists first to avoid PK violation if re-registering
            stmt = select(ExternalRunModel).where(ExternalRunModel.task_id == handle.task_id)
            existing = session.scalar(stmt)

            if existing:
                # Update logic could go here if needed, but usually we just update status later
                # For register, if it exists, we might want to ensure properties match or error out
                # Here we'll just update the fields
                existing.operator_type = handle.operator_type
                existing.external_id = handle.external_id
                existing.status = handle.status.value
                existing.operator_data = handle.operator_data
                existing.relative_path = str(handle.relative_path) if handle.relative_path else None
            else:
                model = ExternalRunModel(
                    task_id=handle.task_id,
                    run_id=run_id,
                    operator_type=handle.operator_type,
                    external_id=handle.external_id,
                    status=handle.status.value,
                    operator_data=handle.operator_data,
                    relative_path=str(handle.relative_path) if handle.relative_path else None,
                )
                session.add(model)
            session.commit()

    def update_external_run(self, handle: ExternalRunHandle) -> None:
        """
        Update an existing external run.
        """
        with self.SessionLocal() as session:
            stmt = select(ExternalRunModel).where(ExternalRunModel.task_id == handle.task_id)
            model = session.scalar(stmt)

            if not model:
                raise ValueError(f"External run for task {handle.task_id} not found.")

            model.external_id = handle.external_id
            model.status = handle.status.value
            model.operator_data = handle.operator_data
            model.relative_path = str(handle.relative_path) if handle.relative_path else None

            session.commit()

    def get_external_run(self, task_id: str) -> Optional[ExternalRunHandle]:
        """
        Get external run handle by task ID.
        """
        with self.SessionLocal() as session:
            stmt = select(ExternalRunModel).where(ExternalRunModel.task_id == task_id)
            model = session.scalar(stmt)

            if not model:
                return None

            return ExternalRunHandle(
                task_id=model.task_id,
                operator_type=model.operator_type,
                external_id=model.external_id,
                status=ExternalRunStatus(model.status),
                operator_data=model.operator_data,
                relative_path=Path(model.relative_path) if model.relative_path else None,
            )

    def get_active_external_runs(self, run_id: str) -> List[ExternalRunHandle]:
        """
        Get all external runs that are not in a terminal state.
        Terminal states: COMPLETED, FAILED, CANCELLED
        """
        terminal_states = [
            ExternalRunStatus.COMPLETED.value,
            ExternalRunStatus.FAILED.value,
            ExternalRunStatus.CANCELLED.value,
        ]

        with self.SessionLocal() as session:
            stmt = select(ExternalRunModel).where(
                ExternalRunModel.run_id == run_id, ExternalRunModel.status.not_in(terminal_states)
            )
            models = session.scalars(stmt).all()

            return [
                ExternalRunHandle(
                    task_id=m.task_id,
                    operator_type=m.operator_type,
                    external_id=m.external_id,
                    status=ExternalRunStatus(m.status),
                    operator_data=m.operator_data,
                    relative_path=Path(m.relative_path) if m.relative_path else None,
                )
                for m in models
            ]

    def cancel_external_runs(self, task_id: str) -> None:
        """
        Cancel all active external runs for a task.
        """
        active_states = [
            ExternalRunStatus.CREATED.value,
            ExternalRunStatus.SUBMITTED.value,
            ExternalRunStatus.RUNNING.value,
            ExternalRunStatus.WAITING_EXTERNAL.value,
        ]

        with self.SessionLocal() as session:
            stmt = (
                update(ExternalRunModel)
                .where(
                    ExternalRunModel.task_id == task_id,
                    ExternalRunModel.status.in_(active_states),
                )
                .values(status=ExternalRunStatus.CANCELLED.value)
            )

            session.execute(stmt)
            session.commit()
