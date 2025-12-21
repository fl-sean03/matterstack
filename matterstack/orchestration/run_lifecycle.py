"""
Backward-compatible shim for run_lifecycle.py

All implementations have been moved to submodules:
- initialization.py: Run init and resume logic
- polling.py: POLL phase logic
- dispatch.py: EXECUTE phase logic
- analyze.py: ANALYZE phase logic
- step_execution.py: Main step_run coordinator
- utilities.py: Loop and discovery utilities

This module re-exports all public APIs to maintain backward compatibility.
All existing imports continue to work:
- from matterstack.orchestration.run_lifecycle import initialize_run
- from matterstack.orchestration.run_lifecycle import step_run
- from matterstack.orchestration.run_lifecycle import run_until_completion
- etc.
"""
from __future__ import annotations

# Re-export from initialization
from matterstack.orchestration.initialization import (
    RunLifecycleError,
    initialize_run,
    initialize_or_resume_run,
)

# Re-export from step_execution
from matterstack.orchestration.step_execution import step_run

# Re-export from utilities
from matterstack.orchestration.utilities import (
    run_until_completion,
    list_active_runs,
)

# Re-export RunHandle for backward compatibility (used in some tests)
from matterstack.core.run import RunHandle

# Re-export SQLiteStateStore for backward compatibility (used in test patching)
from matterstack.storage.state_store import SQLiteStateStore

__all__ = [
    "RunLifecycleError",
    "initialize_run",
    "initialize_or_resume_run",
    "step_run",
    "run_until_completion",
    "list_active_runs",
    "RunHandle",
    "SQLiteStateStore",
]
