import json
import pytest
from unittest.mock import MagicMock, patch

from matterstack.orchestration.run_lifecycle import step_run, RunHandle
from matterstack.core.campaign import Campaign
from matterstack.core.operators import ExternalRunStatus
from matterstack.core.external import ExternalTask

# Mock config.json
@pytest.fixture
def mock_config(tmp_path):
    config_file = tmp_path / "config.json"
    config_data = {"max_hpc_jobs_per_run": 2}
    config_file.write_text(json.dumps(config_data))
    return config_file

@patch("matterstack.orchestration.step_execution.SQLiteStateStore")
def test_concurrency_limit_applied(mock_store_cls, mock_config, tmp_path):
    # Setup
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)
    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    
    # 0. Run Status
    mock_store.get_run_status.return_value = "RUNNING"
    
    # 1. Active attempts: 1 active (occupies a slot)
    # Provide concrete scalar values to avoid pydantic validation errors in attempt polling.
    active_attempt = MagicMock()
    active_attempt.attempt_id = "attempt-task1-1"
    active_attempt.task_id = "task1"
    active_attempt.run_id = "run1"
    active_attempt.operator_type = "HPC"
    active_attempt.external_id = "job-1"
    active_attempt.operator_data = {}
    active_attempt.relative_path = None
    active_attempt.status = ExternalRunStatus.RUNNING.value
    mock_store.get_active_attempts.return_value = [active_attempt]
    mock_store.get_attempt_task_ids.return_value = {"task1"}
    
    # Per-operator concurrency tracking (v0.2.6+)
    # ExternalTask without operator_key resolves to "" (empty string)
    mock_store.count_active_attempts_by_operator.return_value = {"": 1}

    # Legacy external runs: none for this test
    mock_store.get_active_external_runs.return_value = []

    # 2. Tasks: 3 ready to run (ExternalTasks)
    # Use ExternalTask with no explicit MATTERSTACK_OPERATOR so orchestrator goes through the
    # ExternalTask fallback path (operator_type="stub") and does not invoke real operators/backends.
    task_ready_1 = ExternalTask(task_id="task2", command="echo 1", image="ubuntu:latest")
    task_ready_2 = ExternalTask(task_id="task3", command="echo 2", image="ubuntu:latest")
    task_ready_3 = ExternalTask(task_id="task4", command="echo 3", image="ubuntu:latest")
    
    tasks = [task_ready_1, task_ready_2, task_ready_3]
    mock_store.get_tasks.return_value = tasks
    
    # All tasks are "PENDING" (None status in store)
    mock_store.get_task_status.side_effect = lambda tid: None
    
    # Campaign Mock
    campaign = MagicMock(spec=Campaign)
    
    # Execute step_run
    step_run(run_handle, campaign)
    
    # Verification
    # Limit is 2. Active is 1. Slots available = 1.
    # Only 1 new task should be submitted.
    
    # Check attempt creation calls (v2)
    assert mock_store.create_attempt.call_count == 1

    # The first ready task should be submitted (slots_available=1)
    _, kwargs = mock_store.create_attempt.call_args
    assert kwargs["task_id"] == "task2"

    # Verify update_task_status called for WAITING_EXTERNAL
    mock_store.update_task_status.assert_called_with("task2", "WAITING_EXTERNAL")