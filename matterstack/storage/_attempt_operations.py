"""
Task attempt operations mixin for SQLiteStateStore (v2 schema).

This module contains methods for managing task attempts, which provide
provenance-safe reruns with 1:N execution history per logical task.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from sqlalchemy import func, select, update

from matterstack.core.operators import ExternalRunStatus
from matterstack.storage.schema import TaskAttemptModel, TaskModel

if TYPE_CHECKING:
    from sqlalchemy.orm import sessionmaker


class _AttemptOperationsMixin:
    """
    Mixin class providing task attempt operations for SQLiteStateStore (v2 schema).
    
    Expects the following attributes on self:
    - SessionLocal: SQLAlchemy sessionmaker instance
    """

    # Type hints for attributes provided by SQLiteStateStore
    if TYPE_CHECKING:
        SessionLocal: sessionmaker

    def create_attempt(
        self,
        run_id: str,
        task_id: str,
        operator_type: Optional[str] = None,
        operator_key: Optional[str] = None,
        status: str = ExternalRunStatus.CREATED.value,
        operator_data: Optional[Dict[str, Any]] = None,
        relative_path: Optional[Path] = None,
    ) -> str:
        """
        Create a new task attempt (schema v2) and set tasks.current_attempt_id.
        """
        with self.SessionLocal() as session:
            task = session.scalar(
                select(TaskModel).where(TaskModel.task_id == task_id, TaskModel.run_id == run_id)
            )
            if not task:
                raise ValueError(f"Task {task_id} not found in run {run_id}.")

            max_index = session.scalar(
                select(func.max(TaskAttemptModel.attempt_index)).where(
                    TaskAttemptModel.task_id == task_id
                )
            )
            next_index = int(max_index or 0) + 1

            attempt_id = str(uuid.uuid4())

            model = TaskAttemptModel(
                attempt_id=attempt_id,
                task_id=task_id,
                run_id=run_id,
                attempt_index=next_index,
                status=status,
                operator_key=operator_key,
                operator_type=operator_type,
                external_id=None,
                operator_data=operator_data or {},
                relative_path=str(relative_path) if relative_path else None,
                created_at=datetime.utcnow(),
                submitted_at=None,
                ended_at=None,
                status_reason=None,
            )

            session.add(model)
            session.execute(
                update(TaskModel)
                .where(TaskModel.task_id == task_id)
                .values(current_attempt_id=attempt_id)
            )
            session.commit()

            return attempt_id

    def list_attempts(self, task_id: str) -> List[TaskAttemptModel]:
        """
        List all attempts for a task (ordered by attempt_index).
        """
        with self.SessionLocal() as session:
            stmt = (
                select(TaskAttemptModel)
                .where(TaskAttemptModel.task_id == task_id)
                .order_by(TaskAttemptModel.attempt_index.asc())
            )
            return list(session.scalars(stmt).all())

    def get_active_attempts(self, run_id: str) -> List[TaskAttemptModel]:
        """
        Get all attempts that are not in a terminal state.
        Terminal states: COMPLETED, FAILED, CANCELLED
        """
        terminal_states = [
            ExternalRunStatus.COMPLETED.value,
            ExternalRunStatus.FAILED.value,
            ExternalRunStatus.CANCELLED.value,
        ]

        with self.SessionLocal() as session:
            stmt = select(TaskAttemptModel).where(
                TaskAttemptModel.run_id == run_id,
                TaskAttemptModel.status.not_in(terminal_states),
            )
            return list(session.scalars(stmt).all())

    def get_current_attempt(self, task_id: str) -> Optional[TaskAttemptModel]:
        """
        Get the current attempt for a task via tasks.current_attempt_id.
        """
        with self.SessionLocal() as session:
            attempt_id = session.scalar(
                select(TaskModel.current_attempt_id).where(TaskModel.task_id == task_id)
            )
            if not attempt_id:
                return None

            return session.scalar(
                select(TaskAttemptModel).where(TaskAttemptModel.attempt_id == attempt_id)
            )

    def get_attempt(self, attempt_id: str) -> Optional[TaskAttemptModel]:
        """
        Get a task attempt by attempt_id.
        """
        with self.SessionLocal() as session:
            return session.scalar(
                select(TaskAttemptModel).where(TaskAttemptModel.attempt_id == attempt_id)
            )

    def get_attempt_task_ids(self, run_id: str) -> Set[str]:
        """
        Return the set of task_ids that have *any* attempts in this run.

        Used by the orchestrator to prefer attempts over legacy `external_runs` for those tasks.
        """
        with self.SessionLocal() as session:
            rows = session.execute(
                select(TaskAttemptModel.task_id)
                .where(TaskAttemptModel.run_id == run_id)
                .distinct()
            ).all()
            return {r[0] for r in rows}

    def update_attempt(
        self,
        attempt_id: str,
        *,
        status: Optional[str] = None,
        operator_type: Optional[str] = None,
        operator_key: Optional[str] = None,
        external_id: Optional[str] = None,
        operator_data: Optional[Dict[str, Any]] = None,
        relative_path: Optional[Path] = None,
        status_reason: Optional[str] = None,
    ) -> None:
        """
        Update an existing task attempt.

        This is the v2 equivalent of `update_external_run()`: orchestrator calls this after
        operator prepare/submit/poll/collect.
        """
        with self.SessionLocal() as session:
            model = session.scalar(
                select(TaskAttemptModel).where(TaskAttemptModel.attempt_id == attempt_id)
            )
            if not model:
                raise ValueError(f"Attempt {attempt_id} not found.")

            old_status = model.status

            if status is not None:
                model.status = status

            if operator_type is not None:
                model.operator_type = operator_type

            if operator_key is not None:
                model.operator_key = operator_key

            if external_id is not None:
                model.external_id = external_id

            if operator_data is not None:
                model.operator_data = operator_data

            if relative_path is not None:
                model.relative_path = str(relative_path)

            if status_reason is not None:
                model.status_reason = status_reason

            # Timestamp heuristics (best-effort; keep minimal semantics for now)
            now = datetime.utcnow()
            if status is not None and status != old_status:
                if status != ExternalRunStatus.CREATED.value and model.submitted_at is None:
                    model.submitted_at = now

                terminal = {
                    ExternalRunStatus.COMPLETED.value,
                    ExternalRunStatus.FAILED.value,
                    ExternalRunStatus.CANCELLED.value,
                }
                if status in terminal and model.ended_at is None:
                    model.ended_at = now

            session.commit()
