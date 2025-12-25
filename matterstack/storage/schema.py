from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SchemaInfo(Base):
    """
    Tracks the database schema version.
    """

    __tablename__ = "schema_info"

    key: Mapped[str] = mapped_column(String, primary_key=True)  # e.g. "version"
    value: Mapped[str] = mapped_column(String, nullable=False)  # e.g. "1"


class RunModel(Base):
    """
    SQLAlchemy model for Run state.
    Combines RunHandle and RunMetadata information.
    """

    __tablename__ = "runs"

    # Identity
    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    workspace_slug: Mapped[str] = mapped_column(String, nullable=False)
    root_path: Mapped[str] = mapped_column(String, nullable=False)  # Store as string

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Dict[str, str]] = mapped_column(JSON, default=dict)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    tasks: Mapped[List["TaskModel"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    external_runs: Mapped[List["ExternalRunModel"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    task_attempts: Mapped[List["TaskAttemptModel"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class TaskModel(Base):
    """
    SQLAlchemy model for a Task within a Workflow.
    """

    __tablename__ = "tasks"

    task_id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)

    # Task definition
    image: Mapped[str] = mapped_column(String, nullable=False)
    command: Mapped[str] = mapped_column(Text, nullable=False)
    files: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    env: Mapped[Dict[str, str]] = mapped_column(JSON, default=dict)
    dependencies: Mapped[List[str]] = mapped_column(JSON, default=list)  # Store set as list

    # Resources
    cores: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    memory_gb: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    gpus: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    time_limit_minutes: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )

    # Behavior
    allow_dependency_failure: Mapped[bool] = mapped_column(Boolean, default=False)
    allow_failure: Mapped[bool] = mapped_column(Boolean, default=False)

    # Selective Download
    download_patterns: Mapped[Optional[Dict[str, List[str]]]] = mapped_column(
        JSON, nullable=True
    )

    # Execution State
    # Note: We might want to track local execution status here if it differs from ExternalRunStatus
    # For now, we rely on ExternalRunModel for execution tracking of operators
    # But internal status (PENDING, RUNNING, COMPLETED) for the orchestrator logic is useful
    status: Mapped[str] = mapped_column(String, default="PENDING")

    # Polymorphism
    task_type: Mapped[str] = mapped_column(String, default="Task")

    # v4: explicit operator routing key (e.g. "hpc.atesting", "local.default")
    operator_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # v2: convenience pointer to the current attempt (soft reference; may not have FK constraint
    # in migrated DBs because SQLite ALTER TABLE cannot add FKs)
    current_attempt_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Relationship
    run: Mapped["RunModel"] = relationship(back_populates="tasks")
    external_run: Mapped[Optional["ExternalRunModel"]] = relationship(
        back_populates="task", uselist=False, cascade="all, delete-orphan"
    )
    attempts: Mapped[List["TaskAttemptModel"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class TaskAttemptModel(Base):
    """
    SQLAlchemy model for a Task Attempt (schema v2).

    This is a 1:N execution history for a logical task, enabling provenance-safe reruns.
    """

    __tablename__ = "task_attempts"
    __table_args__ = (
        UniqueConstraint("task_id", "attempt_index", name="uq_task_attempts_task_index"),
        Index("ix_task_attempts_run_id_status", "run_id", "status"),
    )

    attempt_id: Mapped[str] = mapped_column(String, primary_key=True)

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False, index=True)

    attempt_index: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    status: Mapped[str] = mapped_column(String, nullable=False)

    # v0.2.6+ canonical routing key (e.g. "hpc.default", "human.default")
    # Persisted for provenance and CLI/evidence stability, even if operator_type later
    # transitions to legacy values like "HPC"/"Local" during operator status updates.
    operator_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    operator_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    external_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    operator_data: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    relative_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    run: Mapped["RunModel"] = relationship(back_populates="task_attempts")
    task: Mapped["TaskModel"] = relationship(back_populates="attempts")


class ExternalRunModel(Base):
    """
    SQLAlchemy model for ExternalRunHandle (schema v1).
    Tracks execution status of a Task on an Operator.

    NOTE: In schema v2, this table is retained for backward compatibility / migration.
    """

    __tablename__ = "external_runs"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)

    operator_type: Mapped[str] = mapped_column(String, nullable=False)
    external_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    operator_data: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    relative_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Relationships
    run: Mapped["RunModel"] = relationship(back_populates="external_runs")
    task: Mapped["TaskModel"] = relationship(back_populates="external_run")