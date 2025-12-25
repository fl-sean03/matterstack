"""Characterization tests for dispatch concurrency.

These tests capture existing behavior of concurrency slot calculation
and operator dispatch to prevent regressions during refactoring.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, Mock, patch

import pytest

from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.dispatch import (
    calculate_concurrency_slots,
    determine_operator_type,
    get_execution_mode,
    get_max_hpc_jobs,
    resolve_operator_key_for_dispatch,
)
from matterstack.storage.state_store import SQLiteStateStore


@pytest.fixture
def temp_store():
    """Create a temporary SQLite store for testing."""
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = SQLiteStateStore(db_path)
        yield store


@pytest.fixture
def run_handle(tmp_path):
    """Create a run handle for testing."""
    return RunHandle(
        run_id="test_run_001",
        workspace_slug="test_workspace",
        root_path=tmp_path,
    )


@pytest.fixture
def store_with_run(temp_store, run_handle):
    """Create a store with a run and tasks ready for attempts."""
    temp_store.create_run(run_handle)
    
    # Create tasks using workflow
    task1 = Task(task_id="task_001", image="test:latest", command="echo test")
    task2 = Task(task_id="task_002", image="test:latest", command="echo test2")
    task3 = Task(task_id="task_003", image="test:latest", command="echo test3")
    workflow = Workflow()
    workflow.add_task(task1)
    workflow.add_task(task2)
    workflow.add_task(task3)
    temp_store.add_workflow(workflow, run_handle.run_id)
    
    return temp_store, run_handle


class TestResolveOperatorKeyForDispatch:
    """Tests for resolve_operator_key_for_dispatch()."""

    def test_returns_none_for_empty_input(self):
        """Should return None for empty/None input."""
        assert resolve_operator_key_for_dispatch(None) is None
        assert resolve_operator_key_for_dispatch("") is None

    def test_normalizes_canonical_key(self):
        """Should normalize canonical operator keys."""
        assert resolve_operator_key_for_dispatch("hpc.default") == "hpc.default"
        assert resolve_operator_key_for_dispatch("local.default") == "local.default"
        assert resolve_operator_key_for_dispatch("human.default") == "human.default"

    def test_handles_legacy_operator_types(self):
        """Should map legacy operator types to canonical keys."""
        assert resolve_operator_key_for_dispatch("HPC") == "hpc.default"
        assert resolve_operator_key_for_dispatch("Local") == "local.default"
        assert resolve_operator_key_for_dispatch("Human") == "human.default"

    def test_handles_case_insensitivity(self):
        """Should handle different case variations."""
        assert resolve_operator_key_for_dispatch("hpc") == "hpc.default"
        assert resolve_operator_key_for_dispatch("HPC") == "hpc.default"
        assert resolve_operator_key_for_dispatch("Hpc") == "hpc.default"

    def test_handles_whitespace(self):
        """Should handle leading/trailing whitespace."""
        assert resolve_operator_key_for_dispatch("  hpc.default  ") == "hpc.default"
        assert resolve_operator_key_for_dispatch("  HPC  ") == "hpc.default"


class TestCalculateConcurrencySlots:
    """Tests for calculate_concurrency_slots()."""

    def test_all_slots_available_when_no_active(self, store_with_run):
        """Should return all slots when no active executions."""
        store, handle = store_with_run
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 0
        assert slots_available == 10

    def test_counts_submitted_attempts(self, store_with_run):
        """Should count SUBMITTED attempts as active."""
        store, handle = store_with_run
        
        # Create a SUBMITTED attempt
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.SUBMITTED.value,
        )
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 1
        assert slots_available == 9

    def test_counts_running_attempts(self, store_with_run):
        """Should count RUNNING attempts as active."""
        store, handle = store_with_run
        
        # Create a RUNNING attempt
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 1
        assert slots_available == 9

    def test_counts_waiting_external_attempts(self, store_with_run):
        """Should count WAITING_EXTERNAL attempts as active."""
        store, handle = store_with_run
        
        # Create a WAITING_EXTERNAL attempt
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="Human",
            status=ExternalRunStatus.WAITING_EXTERNAL.value,
        )
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 1
        assert slots_available == 9

    def test_does_not_count_completed_attempts(self, store_with_run):
        """Should not count COMPLETED attempts as active."""
        store, handle = store_with_run
        
        # Create a COMPLETED attempt
        attempt_id = store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.COMPLETED.value)
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 0
        assert slots_available == 10

    def test_does_not_count_failed_attempts(self, store_with_run):
        """Should not count FAILED attempts as active."""
        store, handle = store_with_run
        
        # Create a FAILED attempt
        attempt_id = store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.CREATED.value,
        )
        store.update_attempt(attempt_id, status=ExternalRunStatus.FAILED.value)
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 0
        assert slots_available == 10

    def test_multiple_active_slots(self, store_with_run):
        """Should correctly count multiple active executions."""
        store, handle = store_with_run
        
        # Create multiple active attempts
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.SUBMITTED.value,
        )
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_002",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_003",
            operator_type="Human",
            status=ExternalRunStatus.WAITING_EXTERNAL.value,
        )
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=10)
        
        assert active_count == 3
        assert slots_available == 7

    def test_zero_slots_when_all_used(self, store_with_run):
        """Should return 0 slots when all are used."""
        store, handle = store_with_run
        
        # Create 3 active attempts with limit of 3
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_001",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_002",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        store.create_attempt(
            run_id=handle.run_id,
            task_id="task_003",
            operator_type="HPC",
            status=ExternalRunStatus.RUNNING.value,
        )
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=3)
        
        assert active_count == 3
        assert slots_available == 0

    def test_handles_zero_max_jobs(self, store_with_run):
        """Should handle edge case of zero max jobs."""
        store, handle = store_with_run
        
        active_count, slots_available = calculate_concurrency_slots(handle, store, max_hpc_jobs=0)
        
        assert active_count == 0
        assert slots_available == 0


class TestGetMaxHpcJobs:
    """Tests for get_max_hpc_jobs()."""

    def test_returns_default_when_no_config(self, run_handle):
        """Should return default 10 when no config.json exists."""
        result = get_max_hpc_jobs(run_handle)
        assert result == 10

    def test_reads_from_config(self, run_handle):
        """Should read max_hpc_jobs_per_run from config.json."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"max_hpc_jobs_per_run": 25}))
        
        result = get_max_hpc_jobs(run_handle)
        assert result == 25

    def test_uses_default_when_key_missing(self, run_handle):
        """Should use default when key is not in config."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"other_setting": "value"}))
        
        result = get_max_hpc_jobs(run_handle)
        assert result == 10


class TestGetExecutionMode:
    """Tests for get_execution_mode()."""

    def test_returns_simulation_when_no_config(self, run_handle):
        """Should return 'Simulation' when no config.json exists."""
        result = get_execution_mode(run_handle)
        assert result == "Simulation"

    def test_reads_hpc_mode_from_config(self, run_handle):
        """Should read execution_mode from config.json."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"execution_mode": "HPC"}))
        
        result = get_execution_mode(run_handle)
        assert result == "HPC"

    def test_reads_local_mode_from_config(self, run_handle):
        """Should read Local mode from config.json."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"execution_mode": "Local"}))
        
        result = get_execution_mode(run_handle)
        assert result == "Local"


class TestDetermineOperatorType:
    """Tests for determine_operator_type()."""

    def test_priority_1_task_operator_key(self, run_handle):
        """Should use task.operator_key as first priority."""
        task = Task(task_id="test", image="test:latest", command="echo", operator_key="hpc.custom")
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "hpc.custom"

    def test_priority_2_matterstack_operator_env(self, run_handle):
        """Should use MATTERSTACK_OPERATOR env var as second priority."""
        task = Task(task_id="test", image="test:latest", command="echo")
        task.env["MATTERSTACK_OPERATOR"] = "Custom"
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "Custom"

    def test_priority_3_gate_task_maps_to_human(self, run_handle):
        """GateTask should map to Human operator."""
        from matterstack.core.gate import GateTask
        
        task = GateTask(task_id="gate_test", image="test:latest", command="echo gate", gate_path="test/path")
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "Human"

    def test_priority_4_external_task_returns_none(self, run_handle):
        """ExternalTask should return None."""
        from matterstack.core.external import ExternalTask
        
        task = ExternalTask(task_id="ext_test", image="test:latest", command="echo ext", external_path="test/path")
        
        result = determine_operator_type(task, run_handle)
        
        assert result is None

    def test_priority_5_uses_config_default_hpc(self, run_handle):
        """Should use execution_mode from config as fallback."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"execution_mode": "HPC"}))
        
        task = Task(task_id="test", image="test:latest", command="echo")
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "HPC"

    def test_priority_5_uses_config_default_local(self, run_handle):
        """Should use Local mode from config."""
        import json
        
        config_path = run_handle.root_path / "config.json"
        config_path.write_text(json.dumps({"execution_mode": "Local"}))
        
        task = Task(task_id="test", image="test:latest", command="echo")
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "Local"

    def test_returns_none_for_simulation_mode(self, run_handle):
        """Should return None for Simulation mode (no config)."""
        task = Task(task_id="test", image="test:latest", command="echo")
        
        result = determine_operator_type(task, run_handle)
        
        assert result is None

    def test_operator_key_takes_precedence_over_env(self, run_handle):
        """operator_key should take precedence over MATTERSTACK_OPERATOR env."""
        task = Task(task_id="test", image="test:latest", command="echo", operator_key="hpc.priority")
        task.env["MATTERSTACK_OPERATOR"] = "EnvOperator"
        
        result = determine_operator_type(task, run_handle)
        
        assert result == "hpc.priority"
