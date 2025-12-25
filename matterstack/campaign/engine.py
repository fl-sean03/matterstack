from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from matterstack.core.backend import ComputeBackend
from matterstack.core.workflow import Workflow
from matterstack.orchestration.api import run_workflow
from matterstack.orchestration.results import WorkflowResult

logger = logging.getLogger(__name__)


@dataclass
class CampaignState:
    """Holds the current state of the campaign."""

    iteration: int = 0
    data: Any = None
    stopped: bool = False


class Campaign(ABC):
    """
    Abstract base class for iterative campaigns (Active Learning, Optimization).

    The loop:
    1. plan() -> Workflow
    2. execute(workflow) -> WorkflowResult
    3. analyze(result) -> update state
    4. should_stop() -> bool
    """

    def __init__(self):
        self.state = CampaignState()

    @abstractmethod
    def plan(self) -> Optional[Workflow]:
        """
        Generate the workflow for the current iteration.
        Return None if no work is needed.
        """
        pass

    @abstractmethod
    def analyze(self, result: WorkflowResult) -> None:
        """
        Analyze the results of the execution and update internal state.
        """
        pass

    def should_stop(self) -> bool:
        """
        Determine if the campaign should terminate.
        """
        return self.state.stopped

    def run(self, max_iterations: int = 10, backend: Optional[ComputeBackend] = None) -> CampaignState:
        """
        Execute the campaign loop.
        """
        logger.info("Starting campaign execution.")

        while self.state.iteration < max_iterations:
            if self.should_stop():
                logger.info("Campaign stopping condition met.")
                break

            logger.info(f"Campaign Iteration {self.state.iteration + 1}")

            # 1. Plan
            workflow = self.plan()
            if workflow is None:
                logger.info("Plan returned no workflow. Stopping.")
                break

            # 2. Execute
            logger.info("Executing workflow...")
            result = run_workflow(workflow, backend=backend)

            # 3. Analyze
            logger.info("Analyzing results...")
            self.analyze(result)

            self.state.iteration += 1

        logger.info("Campaign finished.")
        return self.state
