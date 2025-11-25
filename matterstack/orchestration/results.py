from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ..core.workflow import Task, Workflow
from ..core.backend import JobStatus, JobState


@dataclass
class TaskLogs:
    """Captured stdout/stderr for a completed or running task."""

    stdout: str
    stderr: str


@dataclass
class TaskResult:
    """Result object for an executed Task."""

    task: Task
    job_id: str
    status: JobStatus
    logs: TaskLogs
    workspace_path: Path
    profile_name: Optional[str] = None


@dataclass
class WorkflowResult:
    """Aggregate result for a Workflow execution."""

    workflow: Workflow
    tasks: Dict[str, TaskResult]

    @property
    def failed_tasks(self) -> Dict[str, TaskResult]:
        """Subset of tasks that ended in JobState.FAILED."""

        return {
            task_id: result
            for task_id, result in self.tasks.items()
            if result.status.state == JobState.FAILED
        }

    @property
    def succeeded_tasks(self) -> Dict[str, TaskResult]:
        """Subset of tasks that ended in JobState.COMPLETED."""

        return {
            task_id: result
            for task_id, result in self.tasks.items()
            if result.status.state == JobState.COMPLETED
        }

    @property
    def status(self) -> JobState:
        """Synthetic JobState for the overall workflow."""

        if not self.tasks:
            return JobState.UNKNOWN

        states = {result.status.state for result in self.tasks.values()}

        if JobState.FAILED in states:
            # If some tasks succeeded but others failed, it's a partial success
            # (assuming the workflow wasn't aborted immediately, which this check implies)
            if JobState.COMPLETED in states:
                return JobState.PARTIAL_SUCCESS
            return JobState.FAILED
            
        if JobState.CANCELLED in states:
            return JobState.CANCELLED
        if states == {JobState.COMPLETED}:
            return JobState.COMPLETED
        if JobState.RUNNING in states:
            return JobState.RUNNING
        if JobState.PENDING in states:
            return JobState.PENDING

        return JobState.UNKNOWN