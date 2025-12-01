from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import String, Integer, DateTime, Boolean, JSON, ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class SchemaInfo(Base):
    """
    Tracks the database schema version.
    """
    __tablename__ = "schema_info"

    key: Mapped[str] = mapped_column(String, primary_key=True) # e.g. "version"
    value: Mapped[str] = mapped_column(String, nullable=False) # e.g. "1"

class RunModel(Base):
    """
    SQLAlchemy model for Run state.
    Combines RunHandle and RunMetadata information.
    """
    __tablename__ = "runs"

    # Identity
    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    workspace_slug: Mapped[str] = mapped_column(String, nullable=False)
    root_path: Mapped[str] = mapped_column(String, nullable=False) # Store as string

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Dict[str, str]] = mapped_column(JSON, default=dict)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    tasks: Mapped[List["TaskModel"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    external_runs: Mapped[List["ExternalRunModel"]] = relationship(back_populates="run", cascade="all, delete-orphan")

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
    dependencies: Mapped[List[str]] = mapped_column(JSON, default=list) # Store set as list
    
    # Resources
    cores: Mapped[int] = mapped_column(Integer, default=1)
    memory_gb: Mapped[int] = mapped_column(Integer, default=1)
    gpus: Mapped[int] = mapped_column(Integer, default=0)
    time_limit_minutes: Mapped[int] = mapped_column(Integer, default=60)
    
    # Behavior
    allow_dependency_failure: Mapped[bool] = mapped_column(Boolean, default=False)
    allow_failure: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Selective Download
    download_patterns: Mapped[Optional[Dict[str, List[str]]]] = mapped_column(JSON, nullable=True)

    # Execution State
    # Note: We might want to track local execution status here if it differs from ExternalRunStatus
    # For now, we rely on ExternalRunModel for execution tracking of operators
    # But internal status (PENDING, RUNNING, COMPLETED) for the orchestrator logic is useful
    status: Mapped[str] = mapped_column(String, default="PENDING")

    # Polymorphism
    task_type: Mapped[str] = mapped_column(String, default="Task")

    # Relationship
    run: Mapped["RunModel"] = relationship(back_populates="tasks")
    external_run: Mapped[Optional["ExternalRunModel"]] = relationship(back_populates="task", uselist=False, cascade="all, delete-orphan")

class ExternalRunModel(Base):
    """
    SQLAlchemy model for ExternalRunHandle.
    Tracks execution status of a Task on an Operator.
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