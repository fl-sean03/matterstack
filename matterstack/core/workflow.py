from __future__ import annotations
from typing import Dict, List, Set, Union
from pathlib import Path
import uuid

from pydantic import BaseModel, Field

class Task(BaseModel):
    """
    A declarative unit of work to be executed by a Backend.

    Attributes:
        image: Container image to run (e.g., 'ubuntu:22.04', 'docker://...').
        command: Shell command to execute inside the container.
        files: Dictionary mapping destination paths (relative to workdir) to content.
               - If value is `str`: content is written literally to the file.
               - If value is `Path`: content is copied/uploaded from the local source path.
        env: Environment variables to set in the container.
        dependencies: Set of task_ids that must complete successfully before this task starts.
        task_id: Unique identifier for the task.
        cores: Number of CPU cores required (default: 1).
        memory_gb: Amount of RAM required in GB (default: 1).
        gpus: Number of GPUs required (default: 0).
        time_limit_minutes: Maximum execution time in minutes (default: 60).
    """
    image: str
    command: str
    files: Dict[str, Union[str, Path]] = Field(default_factory=dict)
    env: Dict[str, str] = Field(default_factory=dict)
    dependencies: Set[str] = Field(default_factory=set)
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Resource requirements
    cores: int = 1
    memory_gb: int = 1
    gpus: int = 0
    time_limit_minutes: int = 60
    
    # Execution behavior
    allow_dependency_failure: bool = False
    allow_failure: bool = False

class Workflow(BaseModel):
    """
    A Directed Acyclic Graph (DAG) of Tasks.
    
    Manages dependencies and execution order for a collection of Tasks.
    Ensures no circular dependencies exist.
    """
    tasks: Dict[str, Task] = Field(default_factory=dict)
    
    def add_task(self, task: Task):
        """Add a task to the workflow."""
        if task.task_id in self.tasks:
            raise ValueError(f"Task with ID {task.task_id} already exists.")
        
        # Verify dependencies exist
        for dep_id in task.dependencies:
            if dep_id not in self.tasks:
                raise ValueError(f"Dependency {dep_id} not found in workflow.")
                
        self.tasks[task.task_id] = task

    def get_topo_sorted_tasks(self) -> List[Task]:
        """Return tasks in topological order."""
        visited = set()
        temp_mark = set()
        sorted_list = []

        def visit(n: str):
            if n in temp_mark:
                raise ValueError("Graph has cycles")
            if n not in visited:
                temp_mark.add(n)
                for m in self.tasks[n].dependencies:
                    visit(m)
                temp_mark.remove(n)
                visited.add(n)
                sorted_list.append(self.tasks[n])

        for task_id in self.tasks:
            if task_id not in visited:
                visit(task_id)
                
        return sorted_list
