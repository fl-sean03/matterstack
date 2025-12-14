from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from matterstack.config.operators import load_operators_config
from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.runtime.backends.local import LocalBackend
from matterstack.runtime.operators.hpc import ComputeOperator
from matterstack.runtime.operators.registry import get_cached_operator_registry_from_operators_config
from matterstack.storage.schema import TaskAttemptModel
from matterstack.storage.state_store import SQLiteStateStore


class _TwoComputeOperatorsCampaign(Campaign):
    """
    Deterministic 2-task workflow where each compute task is routed via canonical operator_key.

    This validates multi-instance compute routing at the integration layer:
      - t_default -> hpc.default
      - t_dev     -> hpc.dev
    """

    def __init__(self) -> None:
        self._planned_once = False

    def plan(self, state: Any) -> Any:
        if self._planned_once:
            return None

        wf = Workflow()

        # NOTE: Use explicit MATTERSTACK_OPERATOR to exercise routing logic in
        # [`step_run()`](matterstack/orchestration/run_lifecycle.py:181) without modifying
        # any campaign/engine code.
        t_default = Task(
            task_id="t_default",
            image="ubuntu",
            command="echo default-ok > default.txt",
            env={"MATTERSTACK_OPERATOR": "hpc.default"},
        )

        t_dev = Task(
            task_id="t_dev",
            image="ubuntu",
            command="echo dev-ok > dev.txt",
            env={"MATTERSTACK_OPERATOR": "hpc.dev"},
            dependencies={"t_default"},
        )

        wf.add_task(t_default)
        wf.add_task(t_dev)

        self._planned_once = True
        return wf

    def analyze(self, state: Any, results: Any) -> Any:
        # No-op: this test only validates routing + attempt persistence.
        return state


def _write_operators_yaml(path: Path, *, hpc_default_root: Path, hpc_dev_root: Path) -> None:
    """
    Write a minimal operators.yaml defining two compute instances with distinct backend roots.

    We intentionally use backend.type=local + dry_run=True to keep the test CI-friendly and fast.
    """
    cfg: Dict[str, Any] = {
        "operators": {
            "hpc.default": {
                "kind": "hpc",
                "backend": {
                    "type": "local",
                    "workspace_root": str(hpc_default_root),
                    "dry_run": True,
                },
            },
            "hpc.dev": {
                "kind": "hpc",
                "backend": {
                    "type": "local",
                    "workspace_root": str(hpc_dev_root),
                    "dry_run": True,
                },
            },
        }
    }

    # YAML loader accepts JSON (YAML 1.2 is a superset of JSON).
    path.write_text(json.dumps(cfg, indent=2) + "\n")


def _attempt_row_by_task_id(store: SQLiteStateStore, *, task_id: str) -> TaskAttemptModel:
    attempt = store.get_current_attempt(task_id)
    assert attempt is not None, f"expected attempt for {task_id}"
    return attempt


def test_run_lifecycle_multi_instance_compute_routing(tmp_path: Path) -> None:
    ws = "ws_multi_instance_compute"
    campaign = _TwoComputeOperatorsCampaign()

    # 1) Initialize run in tmpdir
    run_handle = initialize_run(ws, campaign, base_path=tmp_path)

    # 2) operators.yaml defines two distinct compute operator instances
    operators_yaml = tmp_path / "operators.yaml"
    hpc_default_root = tmp_path / "hpc_default_backend_root"
    hpc_dev_root = tmp_path / "hpc_dev_backend_root"

    # Not required for dry_run=True, but makes intent explicit and avoids any path edge cases.
    hpc_default_root.mkdir(parents=True, exist_ok=True)
    hpc_dev_root.mkdir(parents=True, exist_ok=True)

    _write_operators_yaml(
        operators_yaml,
        hpc_default_root=hpc_default_root,
        hpc_dev_root=hpc_dev_root,
    )

    operators_cfg = load_operators_config(operators_yaml)
    operator_registry = get_cached_operator_registry_from_operators_config(run_handle, operators_cfg)

    # 3) Prove registry is truly multi-instance (distinct backends / roots)
    op_default = operator_registry["hpc.default"]
    op_dev = operator_registry["hpc.dev"]

    assert op_default is not op_dev

    assert isinstance(op_default, ComputeOperator)
    assert isinstance(op_dev, ComputeOperator)

    assert isinstance(op_default.backend, LocalBackend)
    assert isinstance(op_dev.backend, LocalBackend)

    assert op_default.backend.workspace_root != op_dev.backend.workspace_root
    assert str(op_default.backend.workspace_root) == str(hpc_default_root.resolve())
    assert str(op_dev.backend.workspace_root) == str(hpc_dev_root.resolve())

    # 4) Tick run until completion (bounded)
    store = SQLiteStateStore(run_handle.db_path)

    max_ticks = 50
    status: Optional[str] = None
    for _ in range(max_ticks):
        status = step_run(run_handle, campaign, operator_registry=operator_registry)
        if status == "COMPLETED":
            break

    assert status == "COMPLETED"

    # 5) Assert attempts persist operator_key as routed per-task
    attempt_default = _attempt_row_by_task_id(store, task_id="t_default")
    attempt_dev = _attempt_row_by_task_id(store, task_id="t_dev")

    assert attempt_default.operator_key == "hpc.default"
    assert attempt_dev.operator_key == "hpc.dev"

    # 6) Prove multi-instance execution metadata diverges deterministically:
    # ComputeOperator.prepare_run() computes operator_data["remote_workdir"] from backend.workspace_root.
    remote_default = (attempt_default.operator_data or {}).get("remote_workdir")
    remote_dev = (attempt_dev.operator_data or {}).get("remote_workdir")

    assert isinstance(remote_default, str) and remote_default.strip()
    assert isinstance(remote_dev, str) and remote_dev.strip()
    assert remote_default != remote_dev

    # Remote workdir must include the configured per-operator workspace root prefix.
    assert remote_default.startswith(str(hpc_default_root.resolve()))
    assert remote_dev.startswith(str(hpc_dev_root.resolve()))

    # 7) Stronger integration assertion: check persisted DB rows (operator_key column + operator_data JSON)
    engine = create_engine(f"sqlite:///{run_handle.db_path}")
    with Session(engine) as session:
        rows = session.scalars(
            select(TaskAttemptModel).where(TaskAttemptModel.run_id == run_handle.run_id)
        ).all()
        by_task: Dict[str, TaskAttemptModel] = {r.task_id: r for r in rows}

        assert by_task["t_default"].operator_key == "hpc.default"
        assert by_task["t_dev"].operator_key == "hpc.dev"

        rd = (by_task["t_default"].operator_data or {}).get("remote_workdir")
        rv = (by_task["t_dev"].operator_data or {}).get("remote_workdir")
        assert isinstance(rd, str) and rd.startswith(str(hpc_default_root.resolve()))
        assert isinstance(rv, str) and rv.startswith(str(hpc_dev_root.resolve()))
        assert rd != rv