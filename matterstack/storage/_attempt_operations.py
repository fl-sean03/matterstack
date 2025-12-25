"""
Task attempt operations mixin for SQLiteStateStore (v2 schema).

This module contains methods for managing task attempts, which provide
provenance-safe reruns with 1:N execution history per logical task.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from sqlalchemy import func, select, update

from matterstack.core.id_generator import generate_attempt_id
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

            attempt_id = generate_attempt_id()

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

    def get_attempt_count(self, run_id: str, task_id: str) -> int:
        """
        Get the count of attempts for a task in a run.
        
        This is the current attempt_index value (1-based count of attempts).
        
        Args:
            run_id: The run ID.
            task_id: The task ID.
        
        Returns:
            The count of attempts for this task, or 0 if none exist.
        """
        with self.SessionLocal() as session:
            count = session.scalar(
                select(func.count(TaskAttemptModel.attempt_id)).where(
                    TaskAttemptModel.run_id == run_id,
                    TaskAttemptModel.task_id == task_id,
                )
            )
            return int(count or 0)

    def get_active_attempts(self, run_id: str) -> List[TaskAttemptModel]:
        """
        Get all attempts that are not in a terminal state.
        Terminal states: COMPLETED, FAILED, FAILED_INIT, CANCELLED
        """
        terminal_states = [
            ExternalRunStatus.COMPLETED.value,
            ExternalRunStatus.FAILED.value,
            ExternalRunStatus.FAILED_INIT.value,
            ExternalRunStatus.CANCELLED.value,
        ]

        with self.SessionLocal() as session:
            stmt = select(TaskAttemptModel).where(
                TaskAttemptModel.run_id == run_id,
                TaskAttemptModel.status.not_in(terminal_states),
            )
            return list(session.scalars(stmt).all())

    def count_active_attempts_by_operator(self, run_id: str) -> Dict[str, int]:
        """
        Count active (non-terminal) attempts grouped by operator_key.

        Used for per-operator concurrency limit enforcement. Returns a dict
        mapping each operator_key to the number of currently active attempts.

        Terminal states: COMPLETED, FAILED, FAILED_INIT, CANCELLED

        Returns:
            Dict mapping operator_key -> count of active attempts.
            Attempts with None operator_key are mapped to empty string "".
        """
        terminal_states = [
            ExternalRunStatus.COMPLETED.value,
            ExternalRunStatus.FAILED.value,
            ExternalRunStatus.FAILED_INIT.value,
            ExternalRunStatus.CANCELLED.value,
        ]

        with self.SessionLocal() as session:
            stmt = (
                select(
                    TaskAttemptModel.operator_key,
                    func.count(TaskAttemptModel.attempt_id),
                )
                .where(
                    TaskAttemptModel.run_id == run_id,
                    TaskAttemptModel.status.not_in(terminal_states),
                )
                .group_by(TaskAttemptModel.operator_key)
            )
            rows = session.execute(stmt).all()
            # Map None operator_key to empty string for easier handling
            return {key or "": count for key, count in rows}

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
                    ExternalRunStatus.FAILED_INIT.value,
                    ExternalRunStatus.CANCELLED.value,
                }
                if status in terminal and model.ended_at is None:
                    model.ended_at = now

            session.commit()

    def find_orphaned_attempts(
        self,
        run_id: str,
        timeout_seconds: int = 3600,
    ) -> List[TaskAttemptModel]:
        """
        Find orphaned attempts that are stuck in CREATED state.

        An attempt is orphaned if:
        - Status is CREATED (never submitted)
        - No external_id assigned
        - created_at is older than timeout_seconds

        Args:
            run_id: The run ID to search.
            timeout_seconds: Age threshold in seconds (default 1 hour).

        Returns:
            List of orphaned attempt models.
        """
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(seconds=timeout_seconds)

        with self.SessionLocal() as session:
            stmt = select(TaskAttemptModel).where(
                TaskAttemptModel.run_id == run_id,
                TaskAttemptModel.status == ExternalRunStatus.CREATED.value,
                TaskAttemptModel.external_id.is_(None),
                TaskAttemptModel.created_at < cutoff,
            )
            return list(session.scalars(stmt).all())

    def mark_attempts_failed_init(
        self,
        attempt_ids: List[str],
        reason: str = "Orphan cleanup",
    ) -> int:
        """
        Mark multiple attempts as FAILED_INIT.

        Args:
            attempt_ids: List of attempt IDs to mark.
            reason: Status reason to record.

        Returns:
            Number of attempts updated.
        """
        from sqlalchemy import update as sql_update

        if not attempt_ids:
            return 0

        with self.SessionLocal() as session:
            now = datetime.utcnow()
            stmt = (
                sql_update(TaskAttemptModel)
                .where(TaskAttemptModel.attempt_id.in_(attempt_ids))
                .values(
                    status=ExternalRunStatus.FAILED_INIT.value,
                    status_reason=reason,
                    ended_at=now,
                )
            )
            result = session.execute(stmt)
            session.commit()
            return result.rowcount
