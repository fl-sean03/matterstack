"""
Integration tests for per-operator concurrency limit enforcement.

These tests verify that the orchestrator respects per-operator max_concurrent
limits configured in operators.yaml.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from matterstack.config.operators import parse_operators_config_dict
from matterstack.core.campaign import Campaign
from matterstack.core.operators import ExternalRunStatus
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task
from matterstack.orchestration.step_execution import step_run


@patch("matterstack.orchestration.step_execution.SQLiteStateStore")
def test_per_operator_limit_respected(mock_store_cls: MagicMock, tmp_path: Path) -> None:
    """
    Verify that per-operator limits are respected independently.
    
    Setup:
    - hpc.gpu: max_concurrent=2, 1 active -> 1 slot available
    - hpc.cpu: max_concurrent=10, 0 active -> 10 slots available
    
    Submit 3 tasks for each operator -> only 1 gpu task, all 3 cpu tasks submitted.
    """
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)
    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    mock_store.get_run_status.return_value = "RUNNING"

    # Operators config with per-operator limits
    operators_config = parse_operators_config_dict({
        "defaults": {"max_concurrent_global": 50},
        "operators": {
            "hpc.gpu": {"kind": "hpc", "max_concurrent": 2},
            "hpc.cpu": {"kind": "hpc", "max_concurrent": 10},
        }
    }, path=tmp_path / "operators.yaml")

    # 1 active attempt for hpc.gpu, 0 for hpc.cpu
    mock_store.count_active_attempts_by_operator.return_value = {"hpc.gpu": 1}
    mock_store.get_active_attempts.return_value = []
    mock_store.get_attempt_task_ids.return_value = set()
    mock_store.get_active_external_runs.return_value = []

    # 3 GPU tasks + 3 CPU tasks - using Task.operator_key for routing
    gpu_tasks = [
        Task(task_id=f"gpu_{i}", image="test:latest", command="gpu", operator_key="hpc.gpu")
        for i in range(3)
    ]
    cpu_tasks = [
        Task(task_id=f"cpu_{i}", image="test:latest", command="cpu", operator_key="hpc.cpu")
        for i in range(3)
    ]
    mock_store.get_tasks.return_value = gpu_tasks + cpu_tasks
    mock_store.get_task_status.return_value = None  # All pending

    campaign = MagicMock(spec=Campaign)

    # Build a mock operator registry
    mock_gpu_op = MagicMock()
    mock_gpu_op.prepare_run.return_value = MagicMock(
        status=ExternalRunStatus.SUBMITTED,
        operator_type="hpc",
        operator_data={},
        external_id="job-1",
        relative_path=None,
    )
    mock_gpu_op.submit.return_value = mock_gpu_op.prepare_run.return_value

    mock_cpu_op = MagicMock()
    mock_cpu_op.prepare_run.return_value = MagicMock(
        status=ExternalRunStatus.SUBMITTED,
        operator_type="hpc",
        operator_data={},
        external_id="job-2",
        relative_path=None,
    )
    mock_cpu_op.submit.return_value = mock_cpu_op.prepare_run.return_value

    operator_registry = {
        "hpc.gpu": mock_gpu_op,
        "hpc.cpu": mock_cpu_op,
    }

    step_run(run_handle, campaign, operator_registry=operator_registry, operators_config=operators_config)

    # Verify: 1 GPU task submitted (2 limit - 1 active = 1 slot)
    # Verify: 3 CPU tasks submitted (10 limit - 0 active = 10 slots)
    create_calls = mock_store.create_attempt.call_args_list
    submitted_task_ids = [call.kwargs["task_id"] for call in create_calls]

    gpu_submitted = [t for t in submitted_task_ids if t.startswith("gpu_")]
    cpu_submitted = [t for t in submitted_task_ids if t.startswith("cpu_")]

    assert len(gpu_submitted) == 1, f"Expected 1 GPU task, got {len(gpu_submitted)}"
    assert len(cpu_submitted) == 3, f"Expected 3 CPU tasks, got {len(cpu_submitted)}"


@patch("matterstack.orchestration.step_execution.SQLiteStateStore")
def test_high_limit_operator_submits_all(mock_store_cls: MagicMock, tmp_path: Path) -> None:
    """
    Verify that operators with a high max_concurrent limit can submit many tasks.
    
    Note: max_concurrent=null means inherit from global, not unlimited.
    For high-capacity operators, set a large explicit limit.
    """
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)
    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    mock_store.get_run_status.return_value = "RUNNING"

    # human.default with high capacity (1000)
    operators_config = parse_operators_config_dict({
        "operators": {
            "human.default": {"kind": "human", "max_concurrent": 1000},
        }
    }, path=tmp_path / "operators.yaml")

    # Even with 100 active attempts, high limit allows more
    mock_store.count_active_attempts_by_operator.return_value = {"human.default": 100}
    mock_store.get_active_attempts.return_value = []
    mock_store.get_attempt_task_ids.return_value = set()
    mock_store.get_active_external_runs.return_value = []

    # 5 human tasks
    human_tasks = [
        Task(task_id=f"human_{i}", image="test:latest", command="review", operator_key="human.default")
        for i in range(5)
    ]
    mock_store.get_tasks.return_value = human_tasks
    mock_store.get_task_status.return_value = None  # All pending

    campaign = MagicMock(spec=Campaign)

    mock_human_op = MagicMock()
    mock_human_op.prepare_run.return_value = MagicMock(
        status=ExternalRunStatus.WAITING_EXTERNAL,
        operator_type="human",
        operator_data={},
        external_id=None,
        relative_path=None,
    )
    mock_human_op.submit.return_value = mock_human_op.prepare_run.return_value

    operator_registry = {"human.default": mock_human_op}

    step_run(run_handle, campaign, operator_registry=operator_registry, operators_config=operators_config)

    # All 5 tasks should be submitted (high limit of 1000)
    create_calls = mock_store.create_attempt.call_args_list
    assert len(create_calls) == 5, f"Expected 5 tasks submitted, got {len(create_calls)}"


@patch("matterstack.orchestration.step_execution.SQLiteStateStore")
def test_global_limit_fallback(mock_store_cls: MagicMock, tmp_path: Path) -> None:
    """
    Verify that operators without max_concurrent fall back to global limit.
    """
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)
    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    mock_store.get_run_status.return_value = "RUNNING"

    # Global limit of 2, operator with no explicit limit
    operators_config = parse_operators_config_dict({
        "defaults": {"max_concurrent_global": 2},
        "operators": {
            "local.default": {"kind": "local"},  # No max_concurrent -> uses global
        }
    }, path=tmp_path / "operators.yaml")

    # 1 active attempt for local.default
    mock_store.count_active_attempts_by_operator.return_value = {"local.default": 1}
    mock_store.get_active_attempts.return_value = []
    mock_store.get_attempt_task_ids.return_value = set()
    mock_store.get_active_external_runs.return_value = []

    # 3 local tasks
    local_tasks = [
        Task(task_id=f"local_{i}", image="test:latest", command="run", operator_key="local.default")
        for i in range(3)
    ]
    mock_store.get_tasks.return_value = local_tasks
    mock_store.get_task_status.return_value = None  # All pending

    campaign = MagicMock(spec=Campaign)

    mock_local_op = MagicMock()
    mock_local_op.prepare_run.return_value = MagicMock(
        status=ExternalRunStatus.SUBMITTED,
        operator_type="local",
        operator_data={},
        external_id=None,
        relative_path=None,
    )
    mock_local_op.submit.return_value = mock_local_op.prepare_run.return_value

    operator_registry = {"local.default": mock_local_op}

    step_run(run_handle, campaign, operator_registry=operator_registry, operators_config=operators_config)

    # Only 1 task should be submitted (2 global limit - 1 active = 1 slot)
    create_calls = mock_store.create_attempt.call_args_list
    assert len(create_calls) == 1, f"Expected 1 task submitted, got {len(create_calls)}"


@patch("matterstack.orchestration.step_execution.SQLiteStateStore")
def test_legacy_mode_without_operators_config(mock_store_cls: MagicMock, tmp_path: Path) -> None:
    """
    Verify backward compatibility: when operators_config is None, use legacy global limit.
    """
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)

    # Create a config.json with legacy max_hpc_jobs_per_run
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"max_hpc_jobs_per_run": 2}))

    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    mock_store.get_run_status.return_value = "RUNNING"

    # 1 active attempt (uses global count in legacy mode)
    mock_store.count_active_attempts_by_operator.return_value = {"hpc.default": 1}
    mock_store.get_active_attempts.return_value = [
        MagicMock(status=ExternalRunStatus.RUNNING.value)
    ]
    mock_store.get_attempt_task_ids.return_value = {"existing_task"}
    mock_store.get_active_external_runs.return_value = []

    # 3 HPC tasks
    hpc_tasks = [
        Task(task_id=f"hpc_{i}", image="test:latest", command="compute", operator_key="hpc.default")
        for i in range(3)
    ]
    mock_store.get_tasks.return_value = hpc_tasks
    mock_store.get_task_status.return_value = None  # All pending

    campaign = MagicMock(spec=Campaign)

    mock_hpc_op = MagicMock()
    mock_hpc_op.prepare_run.return_value = MagicMock(
        status=ExternalRunStatus.SUBMITTED,
        operator_type="hpc",
        operator_data={},
        external_id="job-1",
        relative_path=None,
    )
    mock_hpc_op.submit.return_value = mock_hpc_op.prepare_run.return_value

    operator_registry = {"hpc.default": mock_hpc_op}

    # No operators_config -> legacy mode
    step_run(run_handle, campaign, operator_registry=operator_registry, operators_config=None)

    # Only 1 task should be submitted (2 legacy limit - 1 active = 1 slot)
    create_calls = mock_store.create_attempt.call_args_list
    assert len(create_calls) == 1, f"Expected 1 task submitted in legacy mode, got {len(create_calls)}"
