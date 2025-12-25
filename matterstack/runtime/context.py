from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class HPCClient:
    # v0: local-only, no real scheduler integration yet
    def run_local(self, script: str, workdir: str) -> Dict[str, Any]:
        # TODO: implement in v1 if needed
        return {"status": "completed"}


@dataclass
class LabClient:
    # v0 stub
    def create_work_order(self, **kwargs) -> Any:
        return {"id": "WO-0001", "status": "pending"}


@dataclass
class RuntimeContext:
    hpc: HPCClient
    lab: LabClient
    models: Dict[str, Any] = field(default_factory=dict)
    featurizer: Any | None = None
    cost_model: Any | None = None
