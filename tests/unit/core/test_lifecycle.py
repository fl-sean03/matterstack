"""
Unit tests for the lifecycle hooks module.

Tests cover:
- AttemptContext dataclass creation
- LoggingHook behavior
- CompositeLifecycleHook chaining and error isolation
- fire_hook_safely helper function
"""

import logging
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from matterstack.core.lifecycle import (
    AttemptContext,
    AttemptLifecycleHook,
    CompositeLifecycleHook,
    LoggingHook,
    fire_hook_safely,
)


class TestAttemptContext:
    """Tests for AttemptContext dataclass."""

    def test_attempt_context_creation(self):
        """Verify AttemptContext can be created with all fields."""
        context = AttemptContext(
            run_id="run_123",
            task_id="task_456",
            attempt_id="attempt_789",
            operator_key="hpc.default",
            attempt_index=1,
        )

        assert context.run_id == "run_123"
        assert context.task_id == "task_456"
        assert context.attempt_id == "attempt_789"
        assert context.operator_key == "hpc.default"
        assert context.attempt_index == 1

    def test_attempt_context_with_none_operator_key(self):
        """Verify AttemptContext accepts None operator_key."""
        context = AttemptContext(
            run_id="run_123",
            task_id="task_456",
            attempt_id="attempt_789",
            operator_key=None,
            attempt_index=2,
        )

        assert context.operator_key is None
        assert context.attempt_index == 2


class MockHook(AttemptLifecycleHook):
    """Mock hook for testing that records all calls."""

    def __init__(self):
        self.calls: List[tuple] = []
        self.should_raise = False

    def on_create(self, context: AttemptContext) -> None:
        self.calls.append(("on_create", context))
        if self.should_raise:
            raise RuntimeError("Test error in on_create")

    def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
        self.calls.append(("on_submit", context, external_id))
        if self.should_raise:
            raise RuntimeError("Test error in on_submit")

    def on_complete(self, context: AttemptContext, success: bool) -> None:
        self.calls.append(("on_complete", context, success))
        if self.should_raise:
            raise RuntimeError("Test error in on_complete")

    def on_fail(self, context: AttemptContext, error: str) -> None:
        self.calls.append(("on_fail", context, error))
        if self.should_raise:
            raise RuntimeError("Test error in on_fail")


class TestLoggingHook:
    """Tests for LoggingHook."""

    @pytest.fixture
    def context(self):
        return AttemptContext(
            run_id="run_123",
            task_id="task_456",
            attempt_id="attempt_789",
            operator_key="hpc.default",
            attempt_index=1,
        )

    def test_logging_hook_on_create(self, context, caplog):
        """LoggingHook logs on_create events."""
        hook = LoggingHook()

        with caplog.at_level(logging.INFO, logger="matterstack.lifecycle"):
            hook.on_create(context)

        assert "Attempt created" in caplog.text
        assert "attempt_789" in caplog.text
        assert "task_456" in caplog.text

    def test_logging_hook_on_submit(self, context, caplog):
        """LoggingHook logs on_submit events."""
        hook = LoggingHook()

        with caplog.at_level(logging.INFO, logger="matterstack.lifecycle"):
            hook.on_submit(context, "slurm-12345")

        assert "Attempt submitted" in caplog.text
        assert "attempt_789" in caplog.text
        assert "slurm-12345" in caplog.text

    def test_logging_hook_on_complete(self, context, caplog):
        """LoggingHook logs on_complete events."""
        hook = LoggingHook()

        with caplog.at_level(logging.INFO, logger="matterstack.lifecycle"):
            hook.on_complete(context, success=True)

        assert "Attempt completed" in caplog.text
        assert "attempt_789" in caplog.text
        assert "success=True" in caplog.text

    def test_logging_hook_on_fail(self, context, caplog):
        """LoggingHook logs on_fail events."""
        hook = LoggingHook()

        with caplog.at_level(logging.ERROR, logger="matterstack.lifecycle"):
            hook.on_fail(context, "Slurm job timeout")

        assert "Attempt failed" in caplog.text
        assert "attempt_789" in caplog.text
        assert "Slurm job timeout" in caplog.text

    def test_logging_hook_custom_logger(self, context, caplog):
        """LoggingHook can use a custom logger name."""
        hook = LoggingHook(logger_name="custom.logger")

        with caplog.at_level(logging.INFO, logger="custom.logger"):
            hook.on_create(context)

        assert "Attempt created" in caplog.text


class TestCompositeLifecycleHook:
    """Tests for CompositeLifecycleHook."""

    @pytest.fixture
    def context(self):
        return AttemptContext(
            run_id="run_123",
            task_id="task_456",
            attempt_id="attempt_789",
            operator_key="hpc.default",
            attempt_index=1,
        )

    def test_composite_hook_chains_on_create(self, context):
        """CompositeLifecycleHook calls on_create on all hooks."""
        hook1 = MockHook()
        hook2 = MockHook()
        composite = CompositeLifecycleHook([hook1, hook2])

        composite.on_create(context)

        assert len(hook1.calls) == 1
        assert len(hook2.calls) == 1
        assert hook1.calls[0][0] == "on_create"
        assert hook2.calls[0][0] == "on_create"

    def test_composite_hook_chains_on_submit(self, context):
        """CompositeLifecycleHook calls on_submit on all hooks."""
        hook1 = MockHook()
        hook2 = MockHook()
        composite = CompositeLifecycleHook([hook1, hook2])

        composite.on_submit(context, "external-123")

        assert len(hook1.calls) == 1
        assert len(hook2.calls) == 1
        assert hook1.calls[0] == ("on_submit", context, "external-123")
        assert hook2.calls[0] == ("on_submit", context, "external-123")

    def test_composite_hook_chains_on_complete(self, context):
        """CompositeLifecycleHook calls on_complete on all hooks."""
        hook1 = MockHook()
        hook2 = MockHook()
        composite = CompositeLifecycleHook([hook1, hook2])

        composite.on_complete(context, True)

        assert len(hook1.calls) == 1
        assert len(hook2.calls) == 1
        assert hook1.calls[0] == ("on_complete", context, True)
        assert hook2.calls[0] == ("on_complete", context, True)

    def test_composite_hook_chains_on_fail(self, context):
        """CompositeLifecycleHook calls on_fail on all hooks."""
        hook1 = MockHook()
        hook2 = MockHook()
        composite = CompositeLifecycleHook([hook1, hook2])

        composite.on_fail(context, "Test error")

        assert len(hook1.calls) == 1
        assert len(hook2.calls) == 1
        assert hook1.calls[0] == ("on_fail", context, "Test error")
        assert hook2.calls[0] == ("on_fail", context, "Test error")

    def test_composite_hook_error_isolation(self, context, caplog):
        """CompositeLifecycleHook continues calling hooks even if one fails."""
        hook1 = MockHook()
        hook1.should_raise = True
        hook2 = MockHook()
        hook3 = MockHook()
        composite = CompositeLifecycleHook([hook1, hook2, hook3])

        with caplog.at_level(logging.WARNING):
            composite.on_create(context)

        # Hook1 raised but hook2 and hook3 were still called
        assert len(hook1.calls) == 1
        assert len(hook2.calls) == 1
        assert len(hook3.calls) == 1

        # Error was logged
        assert "MockHook.on_create failed" in caplog.text

    def test_composite_hook_empty_list(self, context):
        """CompositeLifecycleHook works with empty hook list."""
        composite = CompositeLifecycleHook([])

        # Should not raise
        composite.on_create(context)
        composite.on_submit(context, "ext-123")
        composite.on_complete(context, True)
        composite.on_fail(context, "error")


class TestFireHookSafely:
    """Tests for fire_hook_safely helper function."""

    @pytest.fixture
    def context(self):
        return AttemptContext(
            run_id="run_123",
            task_id="task_456",
            attempt_id="attempt_789",
            operator_key="hpc.default",
            attempt_index=1,
        )

    def test_fire_hook_safely_with_none_hook(self, context):
        """fire_hook_safely does nothing when hook is None."""
        # Should not raise
        fire_hook_safely(None, "on_create", context)

    def test_fire_hook_safely_calls_method(self, context):
        """fire_hook_safely calls the specified method."""
        hook = MockHook()

        fire_hook_safely(hook, "on_create", context)

        assert len(hook.calls) == 1
        assert hook.calls[0][0] == "on_create"

    def test_fire_hook_safely_passes_args(self, context):
        """fire_hook_safely passes arguments to the method."""
        hook = MockHook()

        fire_hook_safely(hook, "on_submit", context, "external-123")

        assert hook.calls[0] == ("on_submit", context, "external-123")

    def test_fire_hook_safely_catches_exceptions(self, context, caplog):
        """fire_hook_safely catches and logs exceptions."""
        hook = MockHook()
        hook.should_raise = True

        with caplog.at_level(logging.WARNING):
            # Should not raise
            fire_hook_safely(hook, "on_create", context)

        assert "failed" in caplog.text.lower()

    def test_fire_hook_safely_invalid_method(self, context, caplog):
        """fire_hook_safely handles non-existent method names."""
        hook = MockHook()

        with caplog.at_level(logging.WARNING):
            fire_hook_safely(hook, "nonexistent_method", context)

        assert "no method" in caplog.text.lower()

    def test_fire_hook_safely_with_complete_success(self, context):
        """fire_hook_safely works with on_complete and success flag."""
        hook = MockHook()

        fire_hook_safely(hook, "on_complete", context, True)

        assert hook.calls[0] == ("on_complete", context, True)

    def test_fire_hook_safely_with_fail_error(self, context):
        """fire_hook_safely works with on_fail and error message."""
        hook = MockHook()

        fire_hook_safely(hook, "on_fail", context, "Something went wrong")

        assert hook.calls[0] == ("on_fail", context, "Something went wrong")


class TestLifecycleHookAbstraction:
    """Tests verifying AttemptLifecycleHook is abstract."""

    def test_cannot_instantiate_base_class(self):
        """AttemptLifecycleHook cannot be instantiated directly."""
        with pytest.raises(TypeError):
            AttemptLifecycleHook()  # type: ignore

    def test_subclass_must_implement_all_methods(self):
        """Subclass must implement all abstract methods."""

        class IncompleteHook(AttemptLifecycleHook):
            def on_create(self, context: AttemptContext) -> None:
                pass

        with pytest.raises(TypeError):
            IncompleteHook()  # type: ignore
