from __future__ import annotations

import contextlib
import fcntl
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set

from sqlalchemy import create_engine, delete, func, insert, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.schema import (
    Base,
    ExternalRunModel,
    RunModel,
    SchemaInfo,
    TaskAttemptModel,
    TaskModel,
)

CURRENT_SCHEMA_VERSION = "2"

# Deterministic namespace for v1 -> v2 migration attempt_id backfill.
# attempt_id = uuid5(namespace, f"{run_id}:{task_id}")
TASK_ATTEMPT_MIGRATION_NAMESPACE = uuid.UUID("6df7afdd-8f9f-4b0c-9a2c-6f2b0c2b2b1a")

logger = logging.getLogger(__name__)


class SQLiteStateStore:
    """
    Persistence layer for MatterStack runs using SQLite.
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

    def _sqlite_table_has_column(self, session: Session, table: str, column: str) -> bool:
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

        info.value = CURRENT_SCHEMA_VERSION
        session.commit()

    def _check_schema(self) -> None:
        """
        Check that the database schema version matches the code.
        If missing, initialize it.
        If mismatch, migrate if supported, else raise error.
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

            if info.value == "1" and CURRENT_SCHEMA_VERSION == "2":
                self._migrate_schema_v1_to_v2(session, info)
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
            description=metadata.description
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
                run_id=run_model.run_id,
                workspace_slug=run_model.workspace_slug,
                root_path=Path(run_model.root_path)
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
                description=run_model.description
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

    def add_workflow(self, workflow: Workflow, run_id: str) -> None:
        """
        Persist all tasks in a workflow.
        Upserts tasks (updates if exists, inserts if new).
        """
        with self.SessionLocal() as session:
            for task in workflow.tasks.values():
                # Check if task exists
                stmt = select(TaskModel).where(TaskModel.task_id == task.task_id)
                existing_task = session.scalar(stmt)

                task_data = {
                    "run_id": run_id,
                    "image": task.image,
                    "command": task.command,
                    "files": {str(k): str(v) for k, v in task.files.items()}, # Convert paths to strings
                    "env": task.env,
                    "dependencies": list(task.dependencies),
                    "cores": task.cores,
                    "memory_gb": task.memory_gb,
                    "gpus": task.gpus,
                    "time_limit_minutes": task.time_limit_minutes,
                    "allow_dependency_failure": task.allow_dependency_failure,
                    "allow_failure": task.allow_failure,
                    "download_patterns": task.download_patterns,
                    "task_type": task.__class__.__name__
                }

                if existing_task:
                    # Update existing task
                    for key, value in task_data.items():
                        setattr(existing_task, key, value)
                else:
                    # Create new task
                    new_task = TaskModel(task_id=task.task_id, **task_data)
                    session.add(new_task)
            
            session.commit()

    def get_tasks(self, run_id: str) -> List[Task]:
        """
        Retrieve all tasks for a run.
        """
        with self.SessionLocal() as session:
            stmt = select(TaskModel).where(TaskModel.run_id == run_id)
            task_models = session.scalars(stmt).all()
            
            tasks = []
            for tm in task_models:
                # Determine class based on task_type
                cls = Task
                if tm.task_type == "ExternalTask":
                    cls = ExternalTask
                elif tm.task_type == "GateTask":
                    cls = GateTask
                
                # Note: We rely on default values for fields specific to External/GateTask
                # that are not stored in TaskModel (e.g. request_path).
                # Ideally, we should serialize those into 'files' or a new field.
                # But for now, we just restore the class identity.
                
                task = cls(
                    task_id=tm.task_id,
                    image=tm.image,
                    command=tm.command,
                    files=tm.files,
                    env=tm.env,
                    dependencies=set(tm.dependencies),
                    cores=tm.cores,
                    memory_gb=tm.memory_gb,
                    gpus=tm.gpus,
                    time_limit_minutes=tm.time_limit_minutes,
                    allow_dependency_failure=tm.allow_dependency_failure,
                    allow_failure=tm.allow_failure,
                    download_patterns=tm.download_patterns
                )
                tasks.append(task)
            return tasks
            
    def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Get the internal status of a task.
        """
        with self.SessionLocal() as session:
            stmt = select(TaskModel.status).where(TaskModel.task_id == task_id)
            return session.scalar(stmt)

    def update_task_status(self, task_id: str, status: str) -> None:
        """
        Update the internal status of a task.
        """
        with self.SessionLocal() as session:
            stmt = update(TaskModel).where(TaskModel.task_id == task_id).values(status=status)
            session.execute(stmt)
            session.commit()

    def delete_task(self, task_id: str) -> None:
        """
        Delete a task from the database.
        This cascades to external_runs due to the relationship definition.
        """
        with self.SessionLocal() as session:
            # Manually delete related external runs first to ensure cleanup
            # (In case foreign key constraints aren't enforcing cascade)
            stmt_ext = delete(ExternalRunModel).where(ExternalRunModel.task_id == task_id)
            session.execute(stmt_ext)
            
            stmt = delete(TaskModel).where(TaskModel.task_id == task_id)
            session.execute(stmt)
            session.commit()

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
                    relative_path=str(handle.relative_path) if handle.relative_path else None
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
                relative_path=Path(model.relative_path) if model.relative_path else None
            )

    def get_active_external_runs(self, run_id: str) -> List[ExternalRunHandle]:
        """
        Get all external runs that are not in a terminal state.
        Terminal states: COMPLETED, FAILED, CANCELLED
        """
        terminal_states = [
            ExternalRunStatus.COMPLETED.value,
            ExternalRunStatus.FAILED.value,
            ExternalRunStatus.CANCELLED.value
        ]
        
        with self.SessionLocal() as session:
            stmt = select(ExternalRunModel).where(
                ExternalRunModel.run_id == run_id,
                ExternalRunModel.status.not_in(terminal_states)
            )
            models = session.scalars(stmt).all()
            
            return [
                ExternalRunHandle(
                    task_id=m.task_id,
                    operator_type=m.operator_type,
                    external_id=m.external_id,
                    status=ExternalRunStatus(m.status),
                    operator_data=m.operator_data,
                    relative_path=Path(m.relative_path) if m.relative_path else None
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

    # ---- v2: Task Attempts (minimal scaffolding) ----

    def create_attempt(
        self,
        run_id: str,
        task_id: str,
        operator_type: Optional[str] = None,
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