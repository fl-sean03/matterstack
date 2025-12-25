"""
CLI commands subpackage.

Re-exports all command handlers for use by main.py.
"""

from matterstack.cli.commands.inspection import (
    cmd_explain,
    cmd_export_evidence,
    cmd_monitor,
    cmd_status,
)
from matterstack.cli.commands.run_management import (
    cmd_cancel,
    cmd_init,
    cmd_loop,
    cmd_pause,
    cmd_resume,
    cmd_revive,
    cmd_step,
)
from matterstack.cli.commands.self_test import cmd_self_test
from matterstack.cli.commands.task_management import (
    cmd_attempts,
    cmd_cancel_attempt,
    cmd_cleanup_orphans,
    cmd_rerun,
)

__all__ = [
    # Run management
    "cmd_init",
    "cmd_step",
    "cmd_loop",
    "cmd_cancel",
    "cmd_pause",
    "cmd_resume",
    "cmd_revive",
    # Task management
    "cmd_rerun",
    "cmd_attempts",
    "cmd_cancel_attempt",
    "cmd_cleanup_orphans",
    # Inspection
    "cmd_status",
    "cmd_explain",
    "cmd_monitor",
    "cmd_export_evidence",
    # Self-test
    "cmd_self_test",
]
