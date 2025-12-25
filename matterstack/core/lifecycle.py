"""
Attempt lifecycle hooks for extensibility.

This module provides a hook system for plugins to react to attempt lifecycle events:
- on_create: After attempt record is created
- on_submit: After attempt is submitted to operator
- on_complete: When attempt reaches terminal success state
- on_fail: When attempt fails

Example usage:
    from matterstack.core.lifecycle import (
        AttemptLifecycleHook,
        AttemptContext,
        CompositeLifecycleHook,
        LoggingHook,
    )

    class SlackNotificationHook(AttemptLifecycleHook):
        def on_create(self, context: AttemptContext) -> None:
            pass

        def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
            pass

        def on_complete(self, context: AttemptContext, success: bool) -> None:
            send_slack(f"Task {context.task_id} completed: {'✓' if success else '✗'}")

        def on_fail(self, context: AttemptContext, error: str) -> None:
            send_slack(f"Task {context.task_id} failed: {error}")

    # Compose multiple hooks
    hooks = CompositeLifecycleHook([LoggingHook(), SlackNotificationHook()])
    step_run(run_handle, campaign, lifecycle_hooks=hooks)
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class AttemptContext:
    """
    Context passed to lifecycle hooks.
    
    Contains identifiers and metadata for the current attempt.
    
    Attributes:
        run_id: The run identifier.
        task_id: The task identifier.
        attempt_id: Unique attempt identifier.
        operator_key: Canonical operator key (e.g., "hpc.default"), or None.
        attempt_index: 1-based index for this task's attempts.
    """
    run_id: str
    task_id: str
    attempt_id: str
    operator_key: Optional[str]
    attempt_index: int


class AttemptLifecycleHook(ABC):
    """
    Abstract base class for attempt lifecycle hooks.
    
    Implement this class to react to attempt lifecycle events. All methods
    should be implemented, but can be no-ops if the event is not relevant.
    
    Important: Hook implementations should be lightweight and should not
    raise exceptions that could interfere with the dispatch/polling flow.
    If exceptions are raised, they will be caught and logged.
    """
    
    @abstractmethod
    def on_create(self, context: AttemptContext) -> None:
        """
        Called after attempt record is created in database.
        
        This is called immediately after store.create_attempt() succeeds,
        before operator.prepare_run() is called.
        
        Args:
            context: The attempt context with identifiers.
        """
        pass
    
    @abstractmethod
    def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
        """
        Called after attempt is submitted to operator.
        
        This is called after operator.submit() succeeds.
        
        Args:
            context: The attempt context with identifiers.
            external_id: The external ID assigned by the operator (e.g., Slurm job ID),
                        or None if the operator doesn't assign external IDs.
        """
        pass
    
    @abstractmethod
    def on_complete(self, context: AttemptContext, success: bool) -> None:
        """
        Called when attempt reaches terminal success state.
        
        This is called during polling when the attempt status transitions
        to COMPLETED.
        
        Args:
            context: The attempt context with identifiers.
            success: True if the attempt completed successfully (always True for COMPLETED).
        """
        pass
    
    @abstractmethod
    def on_fail(self, context: AttemptContext, error: str) -> None:
        """
        Called when attempt fails.
        
        This is called when:
        - Dispatch fails during prepare_run() or submit()
        - Polling detects a FAILED status
        
        Args:
            context: The attempt context with identifiers.
            error: Error message describing the failure.
        """
        pass


class CompositeLifecycleHook(AttemptLifecycleHook):
    """
    Chains multiple lifecycle hooks together.
    
    Each hook is called in order. If any hook raises an exception, it is
    caught and logged, and the remaining hooks are still called.
    
    Example:
        hooks = CompositeLifecycleHook([
            LoggingHook(),
            MetricsHook(),
            NotificationHook(),
        ])
    """
    
    def __init__(self, hooks: List[AttemptLifecycleHook]):
        """
        Initialize the composite hook.
        
        Args:
            hooks: List of lifecycle hooks to chain together.
        """
        self.hooks = hooks
    
    def on_create(self, context: AttemptContext) -> None:
        """Call on_create on all hooks."""
        for hook in self.hooks:
            try:
                hook.on_create(context)
            except Exception as e:
                logger.warning(
                    f"Hook {type(hook).__name__}.on_create failed: {e}",
                    exc_info=True
                )
    
    def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
        """Call on_submit on all hooks."""
        for hook in self.hooks:
            try:
                hook.on_submit(context, external_id)
            except Exception as e:
                logger.warning(
                    f"Hook {type(hook).__name__}.on_submit failed: {e}",
                    exc_info=True
                )
    
    def on_complete(self, context: AttemptContext, success: bool) -> None:
        """Call on_complete on all hooks."""
        for hook in self.hooks:
            try:
                hook.on_complete(context, success)
            except Exception as e:
                logger.warning(
                    f"Hook {type(hook).__name__}.on_complete failed: {e}",
                    exc_info=True
                )
    
    def on_fail(self, context: AttemptContext, error: str) -> None:
        """Call on_fail on all hooks."""
        for hook in self.hooks:
            try:
                hook.on_fail(context, error)
            except Exception as e:
                logger.warning(
                    f"Hook {type(hook).__name__}.on_fail failed: {e}",
                    exc_info=True
                )


class LoggingHook(AttemptLifecycleHook):
    """
    Built-in hook that logs lifecycle events.
    
    This is useful for debugging and audit trails.
    
    Example:
        hooks = LoggingHook()
        # or with a custom logger name:
        hooks = LoggingHook(logger_name="myapp.lifecycle")
    """
    
    def __init__(self, logger_name: str = "matterstack.lifecycle"):
        """
        Initialize the logging hook.
        
        Args:
            logger_name: Name of the logger to use for lifecycle events.
        """
        self._logger = logging.getLogger(logger_name)
    
    def on_create(self, context: AttemptContext) -> None:
        """Log attempt creation."""
        self._logger.info(
            f"Attempt created: attempt_id={context.attempt_id}, "
            f"task_id={context.task_id}, run_id={context.run_id}, "
            f"operator_key={context.operator_key}, attempt_index={context.attempt_index}"
        )
    
    def on_submit(self, context: AttemptContext, external_id: Optional[str]) -> None:
        """Log attempt submission."""
        self._logger.info(
            f"Attempt submitted: attempt_id={context.attempt_id}, "
            f"task_id={context.task_id}, external_id={external_id}"
        )
    
    def on_complete(self, context: AttemptContext, success: bool) -> None:
        """Log attempt completion."""
        self._logger.info(
            f"Attempt completed: attempt_id={context.attempt_id}, "
            f"task_id={context.task_id}, success={success}"
        )
    
    def on_fail(self, context: AttemptContext, error: str) -> None:
        """Log attempt failure."""
        self._logger.error(
            f"Attempt failed: attempt_id={context.attempt_id}, "
            f"task_id={context.task_id}, error={error}"
        )


def fire_hook_safely(
    hook: Optional[AttemptLifecycleHook],
    method_name: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    Fire a lifecycle hook method safely, catching and logging any errors.
    
    This is a convenience function for integration points that need to
    fire hooks without risking breaking the main dispatch/polling flow.
    
    Args:
        hook: The lifecycle hook, or None if no hooks are configured.
        method_name: Name of the hook method to call (e.g., "on_create").
        *args: Positional arguments to pass to the hook method.
        **kwargs: Keyword arguments to pass to the hook method.
    """
    if hook is None:
        return
    
    method = getattr(hook, method_name, None)
    if method is None:
        logger.warning(f"Lifecycle hook has no method: {method_name}")
        return
    
    try:
        method(*args, **kwargs)
    except Exception as e:
        logger.warning(
            f"Lifecycle hook {type(hook).__name__}.{method_name} failed: {e}",
            exc_info=True
        )


__all__ = [
    "AttemptContext",
    "AttemptLifecycleHook",
    "CompositeLifecycleHook",
    "LoggingHook",
    "fire_hook_safely",
]
