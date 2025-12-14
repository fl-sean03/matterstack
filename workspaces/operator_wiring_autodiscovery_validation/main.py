from __future__ import annotations

from typing import Any, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow


class OperatorWiringAutodiscoveryValidationCampaign(Campaign):
    """
    Minimal local-only campaign used to validate workspace-default operator wiring auto-discovery.

    - Defines a single compute task routed via env MATTERSTACK_OPERATOR=hpc.default
    - Intended to run with an hpc.default operator wired to LocalBackend dry_run=true
    """

    def plan(self, state: Optional[Any]) -> Optional[Workflow]:
        if state is not None:
            return None

        wf = Workflow()

        wf.add_task(
            Task(
                task_id="compute_autodiscovery_smoke",
                image="ubuntu:latest",
                command="echo operator_wiring_autodiscovery_validation",
                env={"MATTERSTACK_OPERATOR": "hpc.default"},
            )
        )

        return wf

    def analyze(self, state: Any, results: Any) -> Any:
        return {"done": True}


def get_campaign() -> Campaign:
    return OperatorWiringAutodiscoveryValidationCampaign()