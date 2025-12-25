"""
Integration tests for attempt lifecycle hooks.

Tests verify that hooks fire at the correct points in the dispatch and polling flow:
- on_create: After attempt record is created
- on_submit: After attempt is submitted to operator
- on_complete: When attempt reaches terminal success state during polling
- on_fail: When attempt fails during dispatch or polling

These tests use a mock operator to control the lifecycle stages.
"""

from typing import Any, List, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.lifecycle import (
    AttemptContext,
    AttemptLifecycleHook,
    CompositeLifecycleHook,
)
from matterstack.core.operators import (
    ExternalRunHandle,
    ExternalRunStatus,
    Operator,
    OperatorResult,
)
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.dispatch import submit_task_to_operator
from matterstack.orchestration.polling import poll_active_attempts
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore


class RecordingHook(AttemptLifecycleHook):
    """Hook that records all lifecycle events for testing."""

    def __init__(self):
        self.events: List[tuple] = []

    def on_create(self, context: AttemptContext) -> None:
        self.events.append(("on_create", context.run_id, context.task_id, context.attempt_id))

    def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
        self.events.append(("on_submit", context.run_id, context.task_id, external_id))

    def on_complete(self, context: AttemptContext, success: bool) -> None:
        self.events.append(("on_complete", context.run_id, context.task_id, success))

    def on_fail(self, context: AttemptContext, error: str) -> None:
        self.events.append(("on_fail", context.run_id, context.task_id, error))


class MockOperator(Operator):
    """Mock operator that tracks lifecycle states for testing."""

    def __init__(self, initial_status: ExternalRunStatus = ExternalRunStatus.SUBMITTED):
        self.initial_status = initial_status
        self.poll_status = initial_status
        self.prepare_count = 0
        self.submit_count = 0
        self.check_count = 0
        self.should_fail_prepare = False
        self.should_fail_submit = False
        self.external_id = "mock-external-123"

    def prepare_run(self, run: RunHandle, task: Any) -> ExternalRunHandle:
        self.prepare_count += 1
        if self.should_fail_prepare:
            raise RuntimeError("Prepare failed")
        return ExternalRunHandle(
            task_id=task.task_id,
            operator_type="MockOperator",
            status=ExternalRunStatus.CREATED,
            operator_data={},
        )

    def submit(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        self.submit_count += 1
        if self.should_fail_submit:
            raise RuntimeError("Submit failed")
        handle.status = self.initial_status
        handle.external_id = self.external_id
        return handle

    def check_status(self, handle: ExternalRunHandle) -> ExternalRunHandle:
        self.check_count += 1
        handle.status = self.poll_status
        return handle

    def collect_results(self, handle: ExternalRunHandle) -> OperatorResult:
        return OperatorResult(
            task_id=handle.task_id,
            status=handle.status,
            files={},
            data={},
        )


class SimpleCampaign(Campaign):
    """Simple campaign that creates a single task."""

    def __init__(self, task_id: str = "test_task", operator_key: str = "mock.default"):
        self.task_id = task_id
        self.operator_key = operator_key

    def plan(self, state):
        if state is None:
            wf = Workflow()
            t = Task(
                image="ubuntu",
                command="echo test",
                task_id=self.task_id,
                env={"MATTERSTACK_OPERATOR": self.operator_key},
            )
            wf.add_task(t)
            return wf
        return None

    def analyze(self, state, results):
        return {"done": True}


class TestLifecycleHooksInDispatch:
    """Tests verifying hooks fire correctly during dispatch."""

    def test_hooks_fire_on_create_and_submit(self, tmp_path):
        """Test that on_create and on_submit hooks fire during successful dispatch."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        mock_op = MockOperator()
        operators = {"mock.default": mock_op}

        recording_hook = RecordingHook()

        with store.lock():
            success = submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
                lifecycle_hooks=recording_hook,
            )

        assert success is True
        assert len(recording_hook.events) == 2

        # Verify on_create fired first
        assert recording_hook.events[0][0] == "on_create"
        assert recording_hook.events[0][1] == run_handle.run_id
        assert recording_hook.events[0][2] == task.task_id

        # Verify on_submit fired second
        assert recording_hook.events[1][0] == "on_submit"
        assert recording_hook.events[1][1] == run_handle.run_id
        assert recording_hook.events[1][2] == task.task_id
        assert recording_hook.events[1][3] == "mock-external-123"

    def test_hooks_fire_on_fail_when_submit_fails(self, tmp_path):
        """Test that on_fail hook fires when dispatch fails."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        mock_op = MockOperator()
        mock_op.should_fail_submit = True
        operators = {"mock.default": mock_op}

        recording_hook = RecordingHook()

        with store.lock():
            success = submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
                lifecycle_hooks=recording_hook,
            )

        assert success is False

        # on_create should still fire even if submit fails
        on_create_events = [e for e in recording_hook.events if e[0] == "on_create"]
        assert len(on_create_events) == 1

        # on_fail should fire
        on_fail_events = [e for e in recording_hook.events if e[0] == "on_fail"]
        assert len(on_fail_events) == 1
        assert "Submit failed" in on_fail_events[0][3]

    def test_hooks_none_does_not_break_dispatch(self, tmp_path):
        """Test that None lifecycle_hooks parameter doesn't break dispatch."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        mock_op = MockOperator()
        operators = {"mock.default": mock_op}

        with store.lock():
            # Pass None for lifecycle_hooks (default behavior)
            success = submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
                lifecycle_hooks=None,
            )

        assert success is True
        assert mock_op.prepare_count == 1
        assert mock_op.submit_count == 1


class TestLifecycleHooksInPolling:
    """Tests verifying hooks fire correctly during polling."""

    def test_hooks_fire_on_complete_during_poll(self, tmp_path):
        """Test that on_complete hook fires when polling detects completion."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        # First, submit the task
        mock_op = MockOperator(initial_status=ExternalRunStatus.SUBMITTED)
        operators = {"mock.default": mock_op}

        with store.lock():
            submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
            )

        # Now set the operator to return COMPLETED on poll
        mock_op.poll_status = ExternalRunStatus.COMPLETED

        recording_hook = RecordingHook()

        with store.lock():
            poll_active_attempts(
                run_handle.run_id, store, operators,
                lifecycle_hooks=recording_hook,
            )

        # Verify on_complete fired
        on_complete_events = [e for e in recording_hook.events if e[0] == "on_complete"]
        assert len(on_complete_events) == 1
        assert on_complete_events[0][2] == task.task_id
        assert on_complete_events[0][3] is True  # success=True

    def test_hooks_fire_on_fail_during_poll(self, tmp_path):
        """Test that on_fail hook fires when polling detects failure."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        # First, submit the task
        mock_op = MockOperator(initial_status=ExternalRunStatus.SUBMITTED)
        operators = {"mock.default": mock_op}

        with store.lock():
            submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
            )

        # Now set the operator to return FAILED on poll
        mock_op.poll_status = ExternalRunStatus.FAILED

        recording_hook = RecordingHook()

        with store.lock():
            poll_active_attempts(
                run_handle.run_id, store, operators,
                lifecycle_hooks=recording_hook,
            )

        # Verify on_fail fired
        on_fail_events = [e for e in recording_hook.events if e[0] == "on_fail"]
        assert len(on_fail_events) == 1
        assert on_fail_events[0][2] == task.task_id

    def test_hooks_none_does_not_break_polling(self, tmp_path):
        """Test that None lifecycle_hooks parameter doesn't break polling."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        mock_op = MockOperator(initial_status=ExternalRunStatus.SUBMITTED)
        operators = {"mock.default": mock_op}

        with store.lock():
            submit_task_to_operator(
                task, "mock.default", run_handle, store, operators,
            )

        mock_op.poll_status = ExternalRunStatus.COMPLETED

        with store.lock():
            # Pass None for lifecycle_hooks (default behavior)
            poll_active_attempts(
                run_handle.run_id, store, operators,
                lifecycle_hooks=None,
            )

        # Should complete without errors
        assert store.get_task_status(task.task_id) == "COMPLETED"


class TestLifecycleHooksInStepRun:
    """Tests verifying hooks are threaded through step_run."""

    def test_step_run_accepts_lifecycle_hooks(self, tmp_path):
        """Test that step_run accepts lifecycle_hooks parameter."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        recording_hook = RecordingHook()

        # step_run in simulation mode doesn't create attempts (local execution)
        # but it should accept the parameter without error
        status = step_run(run_handle, campaign, lifecycle_hooks=recording_hook)

        # In simulation mode, tasks complete immediately without attempts
        assert status in ["RUNNING", "COMPLETED"]

    def test_composite_hook_in_step_run(self, tmp_path):
        """Test that CompositeLifecycleHook works with step_run."""
        campaign = SimpleCampaign()
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        hook1 = RecordingHook()
        hook2 = RecordingHook()
        composite = CompositeLifecycleHook([hook1, hook2])

        status = step_run(run_handle, campaign, lifecycle_hooks=composite)

        # Should complete without errors
        assert status in ["RUNNING", "COMPLETED"]


class TestLifecycleHooksWithRealOperators:
    """Tests using real operator flow with custom registry."""

    def test_full_lifecycle_with_custom_operator(self, tmp_path):
        """Test complete lifecycle: create -> submit -> poll complete."""
        campaign = SimpleCampaign(operator_key="custom.test")
        run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)

        store = SQLiteStateStore(run_handle.db_path)
        tasks = store.get_tasks(run_handle.run_id)
        task = tasks[0]

        # Create a mock operator that starts as SUBMITTED, then COMPLETED
        mock_op = MockOperator(initial_status=ExternalRunStatus.SUBMITTED)
        operators = {
            "custom.test": mock_op,
        }

        recording_hook = RecordingHook()

        # Phase 1: Dispatch
        with store.lock():
            success = submit_task_to_operator(
                task, "custom.test", run_handle, store, operators,
                lifecycle_hooks=recording_hook,
            )
        assert success is True

        # Verify dispatch hooks fired
        assert any(e[0] == "on_create" for e in recording_hook.events)
        assert any(e[0] == "on_submit" for e in recording_hook.events)

        # Phase 2: Poll with status change to COMPLETED
        mock_op.poll_status = ExternalRunStatus.COMPLETED

        with store.lock():
            poll_active_attempts(
                run_handle.run_id, store, operators,
                lifecycle_hooks=recording_hook,
            )

        # Verify completion hook fired
        on_complete_events = [e for e in recording_hook.events if e[0] == "on_complete"]
        assert len(on_complete_events) == 1

        # Verify full lifecycle order
        event_types = [e[0] for e in recording_hook.events]
        assert event_types == ["on_create", "on_submit", "on_complete"]
