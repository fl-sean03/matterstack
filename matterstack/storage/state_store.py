from __future__ import annotations
import fcntl
import logging
import contextlib
from pathlib import Path
from typing import List, Optional, Dict, Any, Generator

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session, sessionmaker

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.workflow import Workflow, Task
from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.storage.schema import Base, RunModel, TaskModel, ExternalRunModel, SchemaInfo

CURRENT_SCHEMA_VERSION = "1"

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
        
        # Initialize schema if file is new
        Base.metadata.create_all(self.engine)
        
        # Check schema version
        self._check_schema()
        
        logger.debug(f"Initialized SQLiteStateStore at {self.db_path}")

    def _check_schema(self) -> None:
        """
        Check that the database schema version matches the code.
        If missing, initialize it.
        If mismatch, raise error.
        """
        with self.SessionLocal() as session:
            stmt = select(SchemaInfo).where(SchemaInfo.key == "version")
            info = session.scalar(stmt)
            
            if not info:
                # Initialize version
                logger.info(f"Initializing database schema v{CURRENT_SCHEMA_VERSION} at {self.db_path}")
                info = SchemaInfo(key="version", value=CURRENT_SCHEMA_VERSION)
                session.add(info)
                session.commit()
            elif info.value != CURRENT_SCHEMA_VERSION:
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
                    allow_failure=tm.allow_failure
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