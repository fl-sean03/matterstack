from abc import ABC, abstractmethod
from typing import Any, Optional

from matterstack.core.workflow import Workflow


class Campaign(ABC):
    """
    Abstract Base Class for a Campaign (Stateless).

    The Campaign logic is now purely functional:
    1. plan(state) -> Workflow: Generates the next set of tasks based on current state.
    2. analyze(result) -> State: Updates the campaign state based on new results.

    The orchestrator manages persistence of the state and execution of the workflow.
    """

    @abstractmethod
    def plan(self, state: Any) -> Optional[Workflow]:
        """
        Generate the workflow for the current iteration based on the provided state.
        Return None if no work is needed (campaign finished).
        """
        pass

    @abstractmethod
    def analyze(self, state: Any, results: Any) -> Any:
        """
        Analyze the results of the execution and return the updated state.

        Args:
            state: The current campaign state.
            results: The results from the executed workflow (e.g. from EvidenceBundle).

        Returns:
            The new updated state object.
        """
        pass
