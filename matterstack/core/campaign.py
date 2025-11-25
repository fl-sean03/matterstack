from abc import ABC, abstractmethod
from typing import Optional
from matterstack.core.domain import DesignSpace
from matterstack.core.environment import Environment
from matterstack.core.workflow import Workflow
from matterstack.core.backend import ComputeBackend

class Campaign(ABC):
    """
    Abstract Base Class for a Campaign.
    Defines the high-level logic of compiling domain objects into a runtime Workflow.
    """
    def __init__(self, design_space: DesignSpace, environment: Environment, backend: ComputeBackend):
        self.design_space = design_space
        self.environment = environment
        self.backend = backend

    @abstractmethod
    def compile(self) -> Workflow:
        """
        Transforms the Domain objects into a Runtime DAG (Workflow).
        """
        pass

    async def run(self) -> None:
        """
        Calls compile() then submits the Workflow to the Backend.
        """
        workflow = self.compile()
        
        # In Phase 2, we just iterate and submit.
        # Dependency handling is left to the specific Backend implementation
        # or the workflow definition itself if the backend supports DAGs.
        # Since ComputeBackend.submit takes a single Task, we iterate.
        sorted_tasks = workflow.get_topo_sorted_tasks()
        
        for task in sorted_tasks:
            job_id = await self.backend.submit(task)
            # Potentially log the job_id or map it back to the task for dependencies
            # But the current Interface doesn't explicitly require it here.