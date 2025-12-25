"""Characterization tests for schema migrations.

These tests capture existing behavior of v1→v2→v3→v4 migrations
to prevent regressions during refactoring.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from matterstack.storage.schema import (
    Base,
    ExternalRunModel,
    RunModel,
    SchemaInfo,
    TaskAttemptModel,
    TaskModel,
)
from matterstack.storage.state_store import CURRENT_SCHEMA_VERSION, SQLiteStateStore


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


def create_v1_database(db_path: Path) -> None:
    """Create a v1 schema database (external_runs only, no task_attempts)."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    
    # Create only the v1 tables: runs, tasks, external_runs, schema_info
    # We need to manually create without task_attempts to simulate v1
    with engine.begin() as conn:
        # Create runs table
        conn.execute(text("""
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
        """))
        
        # Create tasks table (v1 - no current_attempt_id, no operator_key)
        conn.execute(text("""
            CREATE TABLE tasks (
                task_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                image VARCHAR NOT NULL,
                command TEXT NOT NULL,
                files JSON,
                env JSON,
                dependencies JSON,
                cores INTEGER,
                memory_gb INTEGER,
                gpus INTEGER,
                time_limit_minutes INTEGER,
                allow_dependency_failure BOOLEAN DEFAULT 0,
                allow_failure BOOLEAN DEFAULT 0,
                download_patterns JSON,
                status VARCHAR DEFAULT 'PENDING',
                task_type VARCHAR DEFAULT 'Task'
            )
        """))
        
        # Create external_runs table (v1)
        conn.execute(text("""
            CREATE TABLE external_runs (
                task_id VARCHAR PRIMARY KEY REFERENCES tasks(task_id),
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                operator_type VARCHAR NOT NULL,
                external_id VARCHAR,
                status VARCHAR NOT NULL,
                operator_data JSON,
                relative_path VARCHAR
            )
        """))
        
        # Create schema_info with version 1
        conn.execute(text("""
            CREATE TABLE schema_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            )
        """))
        conn.execute(text("INSERT INTO schema_info (key, value) VALUES ('version', '1')"))


def create_v2_database(db_path: Path) -> None:
    """Create a v2 schema database (has task_attempts, but no operator_key columns)."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    
    with engine.begin() as conn:
        # Create runs table
        conn.execute(text("""
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
        """))
        
        # Create tasks table (v2 - has current_attempt_id, no operator_key)
        conn.execute(text("""
            CREATE TABLE tasks (
                task_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                image VARCHAR NOT NULL,
                command TEXT NOT NULL,
                files JSON,
                env JSON,
                dependencies JSON,
                cores INTEGER,
                memory_gb INTEGER,
                gpus INTEGER,
                time_limit_minutes INTEGER,
                allow_dependency_failure BOOLEAN DEFAULT 0,
                allow_failure BOOLEAN DEFAULT 0,
                download_patterns JSON,
                status VARCHAR DEFAULT 'PENDING',
                task_type VARCHAR DEFAULT 'Task',
                current_attempt_id VARCHAR
            )
        """))
        
        # Create external_runs table
        conn.execute(text("""
            CREATE TABLE external_runs (
                task_id VARCHAR PRIMARY KEY REFERENCES tasks(task_id),
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                operator_type VARCHAR NOT NULL,
                external_id VARCHAR,
                status VARCHAR NOT NULL,
                operator_data JSON,
                relative_path VARCHAR
            )
        """))
        
        # Create task_attempts table (v2 - no operator_key)
        conn.execute(text("""
            CREATE TABLE task_attempts (
                attempt_id VARCHAR PRIMARY KEY,
                task_id VARCHAR NOT NULL REFERENCES tasks(task_id),
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                attempt_index INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                operator_type VARCHAR,
                external_id VARCHAR,
                operator_data JSON,
                relative_path VARCHAR,
                created_at DATETIME,
                submitted_at DATETIME,
                ended_at DATETIME,
                status_reason TEXT,
                UNIQUE(task_id, attempt_index)
            )
        """))
        conn.execute(text("""
            CREATE INDEX ix_task_attempts_run_id_status ON task_attempts(run_id, status)
        """))
        
        # Create schema_info with version 2
        conn.execute(text("""
            CREATE TABLE schema_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            )
        """))
        conn.execute(text("INSERT INTO schema_info (key, value) VALUES ('version', '2')"))


def create_v3_database(db_path: Path) -> None:
    """Create a v3 schema database (has task_attempts.operator_key, no tasks.operator_key)."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    
    with engine.begin() as conn:
        # Create runs table
        conn.execute(text("""
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
        """))
        
        # Create tasks table (v3 - has current_attempt_id, no operator_key)
        conn.execute(text("""
            CREATE TABLE tasks (
                task_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                image VARCHAR NOT NULL,
                command TEXT NOT NULL,
                files JSON,
                env JSON,
                dependencies JSON,
                cores INTEGER,
                memory_gb INTEGER,
                gpus INTEGER,
                time_limit_minutes INTEGER,
                allow_dependency_failure BOOLEAN DEFAULT 0,
                allow_failure BOOLEAN DEFAULT 0,
                download_patterns JSON,
                status VARCHAR DEFAULT 'PENDING',
                task_type VARCHAR DEFAULT 'Task',
                current_attempt_id VARCHAR
            )
        """))
        
        # Create external_runs table
        conn.execute(text("""
            CREATE TABLE external_runs (
                task_id VARCHAR PRIMARY KEY REFERENCES tasks(task_id),
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                operator_type VARCHAR NOT NULL,
                external_id VARCHAR,
                status VARCHAR NOT NULL,
                operator_data JSON,
                relative_path VARCHAR
            )
        """))
        
        # Create task_attempts table (v3 - has operator_key)
        conn.execute(text("""
            CREATE TABLE task_attempts (
                attempt_id VARCHAR PRIMARY KEY,
                task_id VARCHAR NOT NULL REFERENCES tasks(task_id),
                run_id VARCHAR NOT NULL REFERENCES runs(run_id),
                attempt_index INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                operator_key VARCHAR,
                operator_type VARCHAR,
                external_id VARCHAR,
                operator_data JSON,
                relative_path VARCHAR,
                created_at DATETIME,
                submitted_at DATETIME,
                ended_at DATETIME,
                status_reason TEXT,
                UNIQUE(task_id, attempt_index)
            )
        """))
        conn.execute(text("""
            CREATE INDEX ix_task_attempts_run_id_status ON task_attempts(run_id, status)
        """))
        
        # Create schema_info with version 3
        conn.execute(text("""
            CREATE TABLE schema_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            )
        """))
        conn.execute(text("INSERT INTO schema_info (key, value) VALUES ('version', '3')"))


def table_has_column(engine, table: str, column: str) -> bool:
    """Check if a SQLite table has a specific column."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).all()
        return any(r[1] == column for r in rows)


def table_exists(engine, table: str) -> bool:
    """Check if a table exists in the database."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
            {"name": table}
        ).fetchone()
        return result is not None


def get_schema_version(db_path: Path) -> str:
    """Get the schema version from a database."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT value FROM schema_info WHERE key='version'")
        ).fetchone()
        return result[0] if result else None


class TestMigrationV1ToV2:
    """Tests for v1 -> v2 migration."""

    def test_v1_to_v2_creates_task_attempts_table(self, temp_db_path):
        """Migration should create task_attempts table."""
        create_v1_database(temp_db_path)
        
        # Opening SQLiteStateStore triggers migration
        store = SQLiteStateStore(temp_db_path)
        
        # Verify task_attempts table exists
        assert table_exists(store.engine, "task_attempts")

    def test_v1_to_v2_adds_current_attempt_id_column(self, temp_db_path):
        """Migration should add current_attempt_id column to tasks."""
        create_v1_database(temp_db_path)
        
        store = SQLiteStateStore(temp_db_path)
        
        # Verify column exists
        assert table_has_column(store.engine, "tasks", "current_attempt_id")

    def test_v1_to_v2_backfills_external_runs(self, temp_db_path):
        """Migration should backfill attempts from existing external_runs."""
        create_v1_database(temp_db_path)
        engine = create_engine(f"sqlite:///{temp_db_path}", echo=False)
        
        # Insert test data in v1 format
        from datetime import datetime
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO runs (run_id, workspace_slug, root_path, created_at, status)
                VALUES ('run_001', 'test_ws', '/tmp/test', :created, 'RUNNING')
            """), {"created": datetime.utcnow()})
            
            conn.execute(text("""
                INSERT INTO tasks (task_id, run_id, image, command, status)
                VALUES ('task_001', 'run_001', 'test:latest', 'echo test', 'RUNNING')
            """))
            
            conn.execute(text("""
                INSERT INTO external_runs (task_id, run_id, operator_type, status)
                VALUES ('task_001', 'run_001', 'HPC', 'RUNNING')
            """))
        
        # Run migration by opening store
        store = SQLiteStateStore(temp_db_path)
        
        # Verify attempt was created from external_run
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 1
        assert attempts[0].operator_type == "HPC"
        assert attempts[0].status == "RUNNING"

    def test_v1_to_v2_is_idempotent(self, temp_db_path):
        """Running migration twice should not cause errors or duplicate data."""
        create_v1_database(temp_db_path)
        engine = create_engine(f"sqlite:///{temp_db_path}", echo=False)
        
        # Insert test data
        from datetime import datetime
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO runs (run_id, workspace_slug, root_path, created_at, status)
                VALUES ('run_001', 'test_ws', '/tmp/test', :created, 'RUNNING')
            """), {"created": datetime.utcnow()})
            
            conn.execute(text("""
                INSERT INTO tasks (task_id, run_id, image, command, status)
                VALUES ('task_001', 'run_001', 'test:latest', 'echo test', 'RUNNING')
            """))
            
            conn.execute(text("""
                INSERT INTO external_runs (task_id, run_id, operator_type, status)
                VALUES ('task_001', 'run_001', 'HPC', 'RUNNING')
            """))
        
        # First migration
        store1 = SQLiteStateStore(temp_db_path)
        attempts_after_first = len(store1.list_attempts("task_001"))
        
        # Second "migration" (opening store again)
        store2 = SQLiteStateStore(temp_db_path)
        attempts_after_second = len(store2.list_attempts("task_001"))
        
        # Should have same number of attempts
        assert attempts_after_first == attempts_after_second == 1


class TestMigrationV2ToV3:
    """Tests for v2 -> v3 migration."""

    def test_v2_to_v3_adds_operator_key_to_attempts(self, temp_db_path):
        """Migration should add operator_key column to task_attempts."""
        create_v2_database(temp_db_path)
        
        store = SQLiteStateStore(temp_db_path)
        
        # Verify column exists
        assert table_has_column(store.engine, "task_attempts", "operator_key")

    def test_v2_to_v3_is_idempotent(self, temp_db_path):
        """Running migration twice should not cause errors."""
        create_v2_database(temp_db_path)
        
        # First migration
        store1 = SQLiteStateStore(temp_db_path)
        
        # Second "migration"
        store2 = SQLiteStateStore(temp_db_path)
        
        # Should complete without error
        assert get_schema_version(temp_db_path) == CURRENT_SCHEMA_VERSION


class TestMigrationV3ToV4:
    """Tests for v3 -> v4 migration."""

    def test_v3_to_v4_adds_operator_key_to_tasks(self, temp_db_path):
        """Migration should add operator_key column to tasks."""
        create_v3_database(temp_db_path)
        
        store = SQLiteStateStore(temp_db_path)
        
        # Verify column exists
        assert table_has_column(store.engine, "tasks", "operator_key")

    def test_v3_to_v4_is_idempotent(self, temp_db_path):
        """Running migration twice should not cause errors."""
        create_v3_database(temp_db_path)
        
        # First migration
        store1 = SQLiteStateStore(temp_db_path)
        
        # Second "migration"
        store2 = SQLiteStateStore(temp_db_path)
        
        # Should complete without error and be at current version
        assert get_schema_version(temp_db_path) == CURRENT_SCHEMA_VERSION


class TestFullMigrationChain:
    """Tests for complete migration chain v1 -> v4."""

    def test_v1_migrates_to_current_version(self, temp_db_path):
        """v1 database should migrate all the way to current version."""
        create_v1_database(temp_db_path)
        
        store = SQLiteStateStore(temp_db_path)
        
        assert get_schema_version(temp_db_path) == CURRENT_SCHEMA_VERSION

    def test_fresh_database_has_current_version(self, temp_db_path):
        """New database should start at current version."""
        store = SQLiteStateStore(temp_db_path)
        
        assert get_schema_version(temp_db_path) == CURRENT_SCHEMA_VERSION

    def test_full_migration_preserves_data(self, temp_db_path):
        """Full migration chain should preserve existing data."""
        create_v1_database(temp_db_path)
        engine = create_engine(f"sqlite:///{temp_db_path}", echo=False)
        
        # Insert comprehensive test data with proper JSON for lists
        from datetime import datetime
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO runs (run_id, workspace_slug, root_path, created_at, status, description)
                VALUES ('run_001', 'test_ws', '/tmp/test', :created, 'COMPLETED', 'Test run')
            """), {"created": datetime.utcnow()})
            
            conn.execute(text("""
                INSERT INTO tasks (task_id, run_id, image, command, status, cores, memory_gb, dependencies, files, env)
                VALUES ('task_001', 'run_001', 'test:latest', 'echo test', 'COMPLETED', 4, 8, '[]', '{}', '{}')
            """))
            
            conn.execute(text("""
                INSERT INTO external_runs (task_id, run_id, operator_type, external_id, status)
                VALUES ('task_001', 'run_001', 'HPC', 'job_123', 'COMPLETED')
            """))
        
        # Run full migration
        store = SQLiteStateStore(temp_db_path)
        
        # Verify run data preserved
        run = store.get_run("run_001")
        assert run is not None
        assert run.workspace_slug == "test_ws"
        
        # Verify task data preserved
        tasks = store.get_tasks("run_001")
        assert len(tasks) == 1
        assert tasks[0].cores == 4
        assert tasks[0].memory_gb == 8
        
        # Verify attempt was created from external_run
        attempts = store.list_attempts("task_001")
        assert len(attempts) == 1
        assert attempts[0].external_id == "job_123"


class TestMigrationHelpers:
    """Tests for migration helper methods."""

    def test_sqlite_table_has_column_returns_true_for_existing(self, temp_db_path):
        """Helper should return True for existing columns."""
        store = SQLiteStateStore(temp_db_path)
        
        with store.SessionLocal() as session:
            result = store._sqlite_table_has_column(session, "runs", "run_id")
            assert result is True

    def test_sqlite_table_has_column_returns_false_for_missing(self, temp_db_path):
        """Helper should return False for non-existent columns."""
        store = SQLiteStateStore(temp_db_path)
        
        with store.SessionLocal() as session:
            result = store._sqlite_table_has_column(session, "runs", "nonexistent_column")
            assert result is False
