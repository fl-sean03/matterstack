from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ..core.backend import JobState, JobStatus
from ..core.workflow import Task, Workflow


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
        """Subset of tasks that ended in JobState.COMPLETED_ERROR."""

        return {
            task_id: result for task_id, result in self.tasks.items() if result.status.state == JobState.COMPLETED_ERROR
        }

    @property
    def succeeded_tasks(self) -> Dict[str, TaskResult]:
        """Subset of tasks that ended in JobState.COMPLETED_OK."""

        return {
            task_id: result for task_id, result in self.tasks.items() if result.status.state == JobState.COMPLETED_OK
        }

    @property
    def status(self) -> JobState:
        """Synthetic JobState for the overall workflow."""

        if not self.tasks:
            return JobState.UNKNOWN

        states = {result.status.state for result in self.tasks.values()}

        if JobState.COMPLETED_ERROR in states:
            # If some tasks succeeded but others failed, it's still an error state for the whole workflow
            return JobState.COMPLETED_ERROR

        if JobState.CANCELLED in states:
            return JobState.CANCELLED
        if states == {JobState.COMPLETED_OK}:
            return JobState.COMPLETED_OK
        if JobState.RUNNING in states:
            return JobState.RUNNING
        if JobState.QUEUED in states:
            return JobState.QUEUED

        return JobState.UNKNOWN
