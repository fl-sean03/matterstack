"""
Task CRUD operations mixin for SQLiteStateStore.

This module contains methods for managing tasks within workflows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List

from sqlalchemy import delete, select, update

from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.schema import ExternalRunModel, TaskModel

if TYPE_CHECKING:
    from sqlalchemy.orm import sessionmaker


class _TaskOperationsMixin:
    """
    Mixin class providing task CRUD operations for SQLiteStateStore.

    Expects the following attributes on self:
    - SessionLocal: SQLAlchemy sessionmaker instance
    """

    # Type hints for attributes provided by SQLiteStateStore
    if TYPE_CHECKING:
        SessionLocal: sessionmaker

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
                    "files": {str(k): str(v) for k, v in task.files.items()},  # Convert paths to strings
                    "env": task.env,
                    "dependencies": list(task.dependencies),
                    "cores": task.cores,
                    "memory_gb": task.memory_gb,
                    "gpus": task.gpus,
                    "time_limit_minutes": task.time_limit_minutes,
                    "allow_dependency_failure": task.allow_dependency_failure,
                    "allow_failure": task.allow_failure,
                    "download_patterns": task.download_patterns,
                    "task_type": task.__class__.__name__,
                    "operator_key": getattr(task, "operator_key", None),  # v0.2.6+ first-class routing
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
                    download_patterns=tm.download_patterns,
                    operator_key=tm.operator_key,  # v0.2.6+ first-class routing
                )
                tasks.append(task)
            return tasks

    def get_task_status(self, task_id: str) -> str | None:
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
