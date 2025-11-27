import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from matterstack.orchestration.run_lifecycle import step_run, RunHandle
from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.core.external import ExternalTask

# Mock config.json
@pytest.fixture
def mock_config(tmp_path):
    config_file = tmp_path / "config.json"
    config_data = {"max_hpc_jobs_per_run": 2}
    config_file.write_text(json.dumps(config_data))
    return config_file

@patch("matterstack.orchestration.run_lifecycle.SQLiteStateStore")
def test_concurrency_limit_applied(mock_store_cls, mock_config, tmp_path):
    # Setup
    run_handle = RunHandle(workspace_slug="test", run_id="run1", root_path=tmp_path)
    mock_store = mock_store_cls.return_value
    mock_store.lock.return_value.__enter__.return_value = None
    
    # 0. Run Status
    mock_store.get_run_status.return_value = "RUNNING"
    
    # 1. External runs: 1 active
    active_ext = ExternalRunHandle(
        task_id="task1", 
        operator_type="DirectHPC", 
        status=ExternalRunStatus.RUNNING,
        external_id="100"
    )
    mock_store.get_active_external_runs.return_value = [active_ext]
    
    # 2. Tasks: 3 ready to run (ExternalTasks)
    task_ready_1 = ExternalTask(task_id="task2", operator_type="DirectHPC", command="echo 1", image="ubuntu:latest")
    task_ready_2 = ExternalTask(task_id="task3", operator_type="DirectHPC", command="echo 2", image="ubuntu:latest")
    task_ready_3 = ExternalTask(task_id="task4", operator_type="DirectHPC", command="echo 3", image="ubuntu:latest")
    
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
    
    # Check register_external_run calls
    assert mock_store.register_external_run.call_count == 1
    
    # Check which task was submitted
    args, _ = mock_store.register_external_run.call_args
    submitted_handle = args[0]
    assert submitted_handle.task_id == "task2" # Assumes order is preserved
    
    # Verify update_task_status
    # Should be called once for "WAITING_EXTERNAL"
    mock_store.update_task_status.assert_called_with("task2", "WAITING_EXTERNAL")