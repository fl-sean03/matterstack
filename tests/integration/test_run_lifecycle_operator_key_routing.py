"""
Integration test: Validates first-class operator_key routing.

Ensures that task.operator_key:
1. Takes priority over env-based routing (MATTERSTACK_OPERATOR)
2. Is persisted to and retrieved from the database correctly
3. Works with the dispatch routing system
"""

from pathlib import Path
from typing import Any, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.dispatch import determine_operator_type
from matterstack.orchestration.run_lifecycle import initialize_run
from matterstack.storage.state_store import SQLiteStateStore


class OperatorKeyRoutingCampaign(Campaign):
    """Campaign that creates tasks with operator_key for routing tests."""

    def __init__(self, operator_key: Optional[str] = None, env_operator: Optional[str] = None):
        self.operator_key = operator_key
        self.env_operator = env_operator

    def plan(self, state: Any) -> Optional[Workflow]:
        if state is not None:
            return None

        wf = Workflow()

        env = {}
        if self.env_operator:
            env["MATTERSTACK_OPERATOR"] = self.env_operator

        task = Task(
            task_id="routing_test_task",
            image="ubuntu:latest",
            command="echo test",
            operator_key=self.operator_key,
            env=env,
        )
        wf.add_task(task)

        return wf

    def analyze(self, state: Any, results: Any) -> Any:
        return {"done": True}


def test_operator_key_takes_priority_over_env(tmp_path: Path):
    """
    If both operator_key and env["MATTERSTACK_OPERATOR"] are set,
    operator_key should be used.
    """
    campaign = OperatorKeyRoutingCampaign(
        operator_key="hpc.primary",
        env_operator="hpc.secondary",
    )

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

    # Get the task from the store
    store = SQLiteStateStore(run_handle.db_path)
    tasks = store.get_tasks(run_handle.run_id)

    assert len(tasks) == 1
    task = tasks[0]

    # Verify operator_key is on the task
    assert task.operator_key == "hpc.primary"
    assert task.env.get("MATTERSTACK_OPERATOR") == "hpc.secondary"

    # Verify determine_operator_type returns operator_key (not env)
    result = determine_operator_type(task, run_handle)
    assert result == "hpc.primary", f"Expected 'hpc.primary', got '{result}'"


def test_operator_key_only_no_env(tmp_path: Path):
    """
    When only operator_key is set (no env), it should be used for routing.
    """
    campaign = OperatorKeyRoutingCampaign(
        operator_key="local.default",
        env_operator=None,
    )

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

    store = SQLiteStateStore(run_handle.db_path)
    tasks = store.get_tasks(run_handle.run_id)

    assert len(tasks) == 1
    task = tasks[0]

    assert task.operator_key == "local.default"
    assert "MATTERSTACK_OPERATOR" not in task.env

    result = determine_operator_type(task, run_handle)
    assert result == "local.default"


def test_env_fallback_when_operator_key_is_none(tmp_path: Path):
    """
    When operator_key is None, env["MATTERSTACK_OPERATOR"] should be used (legacy).
    """
    campaign = OperatorKeyRoutingCampaign(
        operator_key=None,
        env_operator="hpc.legacy",
    )

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

    store = SQLiteStateStore(run_handle.db_path)
    tasks = store.get_tasks(run_handle.run_id)

    assert len(tasks) == 1
    task = tasks[0]

    assert task.operator_key is None
    assert task.env.get("MATTERSTACK_OPERATOR") == "hpc.legacy"

    result = determine_operator_type(task, run_handle)
    assert result == "hpc.legacy"


def test_operator_key_persisted_to_database(tmp_path: Path):
    """
    Verify operator_key is correctly stored in the tasks table and retrieved.
    """
    campaign = OperatorKeyRoutingCampaign(
        operator_key="experiment.custom",
        env_operator=None,
    )

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

    # First store instance
    store1 = SQLiteStateStore(run_handle.db_path)
    tasks1 = store1.get_tasks(run_handle.run_id)
    assert len(tasks1) == 1
    assert tasks1[0].operator_key == "experiment.custom"

    # Create a new store instance to verify persistence
    store2 = SQLiteStateStore(run_handle.db_path)
    tasks2 = store2.get_tasks(run_handle.run_id)
    assert len(tasks2) == 1
    assert tasks2[0].operator_key == "experiment.custom"


def test_operator_key_none_persisted_correctly(tmp_path: Path):
    """
    Verify that None operator_key is correctly stored and retrieved.
    """
    campaign = OperatorKeyRoutingCampaign(
        operator_key=None,
        env_operator=None,
    )

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

    store = SQLiteStateStore(run_handle.db_path)
    tasks = store.get_tasks(run_handle.run_id)

    assert len(tasks) == 1
    assert tasks[0].operator_key is None
