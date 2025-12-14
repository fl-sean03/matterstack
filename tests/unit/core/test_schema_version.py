import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from matterstack.storage.schema import Base, ExternalRunModel, SchemaInfo, TaskAttemptModel, TaskModel
from matterstack.storage.state_store import (
    CURRENT_SCHEMA_VERSION,
    TASK_ATTEMPT_MIGRATION_NAMESPACE,
    SQLiteStateStore,
)

def test_schema_initialization(tmp_path):
    """
    Test that a fresh database is initialized with the correct schema version.
    """
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    # Check that file exists
    assert db_path.exists()
    
    # Check content manually
    engine = create_engine(f"sqlite:///{db_path}")
    with Session(engine) as session:
        stmt = select(SchemaInfo).where(SchemaInfo.key == "version")
        info = session.scalar(stmt)
        assert info is not None
        assert info.value == CURRENT_SCHEMA_VERSION

def test_schema_check_pass(tmp_path):
    """
    Test that opening an existing valid database passes.
    """
    db_path = tmp_path / "state.sqlite"
    
    # 1. Create and init
    store1 = SQLiteStateStore(db_path)
    
    # 2. Re-open
    store2 = SQLiteStateStore(db_path)
    # Should not raise

def test_schema_mismatch_error(tmp_path):
    """
    Test that a version mismatch raises a RuntimeError.
    """
    db_path = tmp_path / "state.sqlite"

    # 1. Create a dummy DB manually with wrong version
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        info = SchemaInfo(key="version", value="999")
        session.add(info)
        session.commit()

    # 2. Try to open with StateStore
    with pytest.raises(RuntimeError) as excinfo:
        SQLiteStateStore(db_path)

    assert "Schema version mismatch" in str(excinfo.value)
    assert "v999" in str(excinfo.value)


def test_schema_migration_v1_to_v2_task_attempts(tmp_path):
    """
    Create a v1-style DB (runs/tasks/external_runs + schema_info=1) and verify that opening
    SQLiteStateStore performs an additive v1 -> v2 migration:

    - schema_info.version becomes CURRENT_SCHEMA_VERSION ("2")
    - one task_attempt is created for each external_runs row (deterministic uuid5)
    - tasks.current_attempt_id is set
    - v1 external_runs data remains intact
    - repeated open is idempotent (no duplicate task_attempts)
    """
    db_path = tmp_path / "state.sqlite"
    engine = create_engine(f"sqlite:///{db_path}")

    run_id = "run_1"
    task_with_er = "task_a"
    task_without_er = "task_b"

    created_at = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Create v1 tables ONLY (not Base.metadata.create_all), to ensure migration has work to do.
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE schema_info (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE runs (
                    run_id VARCHAR PRIMARY KEY,
                    workspace_slug VARCHAR NOT NULL,
                    root_path VARCHAR NOT NULL,
                    created_at DATETIME NOT NULL,
                    status VARCHAR NOT NULL,
                    status_reason TEXT,
                    tags JSON,
                    description TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE tasks (
                    task_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    image VARCHAR NOT NULL,
                    command TEXT NOT NULL,
                    files JSON,
                    env JSON,
                    dependencies JSON,
                    cores INTEGER,
                    memory_gb INTEGER,
                    gpus INTEGER,
                    time_limit_minutes INTEGER,
                    allow_dependency_failure BOOLEAN,
                    allow_failure BOOLEAN,
                    download_patterns JSON,
                    status VARCHAR,
                    task_type VARCHAR
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE external_runs (
                    task_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    operator_type VARCHAR NOT NULL,
                    external_id VARCHAR,
                    status VARCHAR NOT NULL,
                    operator_data JSON,
                    relative_path VARCHAR
                )
                """
            )
        )

        # Seed schema v1
        conn.execute(
            text("INSERT INTO schema_info (key, value) VALUES (:key, :value)"),
            {"key": "version", "value": "1"},
        )

        # Seed run + tasks
        conn.execute(
            text(
                """
                INSERT INTO runs (run_id, workspace_slug, root_path, created_at, status, status_reason, tags, description)
                VALUES (:run_id, :workspace_slug, :root_path, :created_at, :status, :status_reason, :tags, :description)
                """
            ),
            {
                "run_id": run_id,
                "workspace_slug": "ws",
                "root_path": "/tmp/ws/runs/run_1",
                "created_at": created_at,
                "status": "RUNNING",
                "status_reason": None,
                "tags": "{}",
                "description": None,
            },
        )

        minimal_task_payload = {
            "image": "ubuntu:22.04",
            "command": "echo hi",
            "files": "{}",
            "env": "{}",
            "dependencies": "[]",
            "cores": None,
            "memory_gb": None,
            "gpus": None,
            "time_limit_minutes": None,
            "allow_dependency_failure": 0,
            "allow_failure": 0,
            "download_patterns": None,
            "status": "PENDING",
            "task_type": "Task",
        }

        conn.execute(
            text(
                """
                INSERT INTO tasks (
                    task_id, run_id, image, command, files, env, dependencies,
                    cores, memory_gb, gpus, time_limit_minutes,
                    allow_dependency_failure, allow_failure, download_patterns, status, task_type
                ) VALUES (
                    :task_id, :run_id, :image, :command, :files, :env, :dependencies,
                    :cores, :memory_gb, :gpus, :time_limit_minutes,
                    :allow_dependency_failure, :allow_failure, :download_patterns, :status, :task_type
                )
                """
            ),
            {"task_id": task_with_er, "run_id": run_id, **minimal_task_payload},
        )
        conn.execute(
            text(
                """
                INSERT INTO tasks (
                    task_id, run_id, image, command, files, env, dependencies,
                    cores, memory_gb, gpus, time_limit_minutes,
                    allow_dependency_failure, allow_failure, download_patterns, status, task_type
                ) VALUES (
                    :task_id, :run_id, :image, :command, :files, :env, :dependencies,
                    :cores, :memory_gb, :gpus, :time_limit_minutes,
                    :allow_dependency_failure, :allow_failure, :download_patterns, :status, :task_type
                )
                """
            ),
            {"task_id": task_without_er, "run_id": run_id, **minimal_task_payload},
        )

        # Seed external_run for one task
        conn.execute(
            text(
                """
                INSERT INTO external_runs (
                    task_id, run_id, operator_type, external_id, status, operator_data, relative_path
                ) VALUES (
                    :task_id, :run_id, :operator_type, :external_id, :status, :operator_data, :relative_path
                )
                """
            ),
            {
                "task_id": task_with_er,
                "run_id": run_id,
                "operator_type": "hpc",
                "external_id": "job-123",
                "status": "COMPLETED",
                "operator_data": '{"absolute_path": "/abs/path/ignored"}',
                "relative_path": "operators/hpc/uuid",
            },
        )

    # Opening should migrate v1 -> v2
    SQLiteStateStore(db_path)

    expected_attempt_id = str(
        uuid.uuid5(TASK_ATTEMPT_MIGRATION_NAMESPACE, f"{run_id}:{task_with_er}")
    )

    # Validate migration output
    engine2 = create_engine(f"sqlite:///{db_path}")
    with Session(engine2) as session:
        info = session.scalar(select(SchemaInfo).where(SchemaInfo.key == "version"))
        assert info is not None
        assert info.value == CURRENT_SCHEMA_VERSION

        attempts = session.scalars(select(TaskAttemptModel)).all()
        assert len(attempts) == 1
        assert attempts[0].attempt_id == expected_attempt_id
        assert attempts[0].task_id == task_with_er
        assert attempts[0].run_id == run_id
        assert attempts[0].status == "COMPLETED"
        assert attempts[0].operator_type == "hpc"
        assert attempts[0].external_id == "job-123"
        assert attempts[0].relative_path == "operators/hpc/uuid"

        task_a = session.scalar(select(TaskModel).where(TaskModel.task_id == task_with_er))
        task_b = session.scalar(select(TaskModel).where(TaskModel.task_id == task_without_er))
        assert task_a is not None
        assert task_b is not None
        assert task_a.current_attempt_id == expected_attempt_id
        assert task_b.current_attempt_id is None

        # v1 table preserved
        ers = session.scalars(select(ExternalRunModel)).all()
        assert len(ers) == 1
        assert ers[0].task_id == task_with_er
        assert ers[0].status == "COMPLETED"

    # Idempotency: re-open must not create a second attempt row
    SQLiteStateStore(db_path)
    with Session(engine2) as session:
        attempts2 = session.scalars(select(TaskAttemptModel)).all()
        assert len(attempts2) == 1
        assert attempts2[0].attempt_id == expected_attempt_id