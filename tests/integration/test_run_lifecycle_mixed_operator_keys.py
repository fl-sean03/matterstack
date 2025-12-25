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
from matterstack.runtime.fs_safety import attempt_evidence_dir
from matterstack.runtime.operators.registry import get_cached_operator_registry_from_operators_config
from matterstack.storage.schema import TaskAttemptModel
from matterstack.storage.state_store import SQLiteStateStore


class _MixedOperatorsCampaign(Campaign):
    """
    Deterministic 3-task workflow where each task is routed via canonical operator_key.

    - compute task routed via hpc.default (configured as LocalBackend in operators.yaml)
    - human task routed via human.default
    - experiment task routed via experiment.default
    """

    def __init__(self) -> None:
        self._planned_once = False

    def plan(self, state: Any) -> Any:
        if self._planned_once:
            return None

        wf = Workflow()

        # Deterministic compute task: local backend writes exit_code=0 and produces a small artifact.
        t_compute = Task(
            task_id="t_compute",
            image="ubuntu",
            command="echo compute-ok > compute.txt",
            env={"MATTERSTACK_OPERATOR": "hpc.default"},
        )

        # Human task: completes when response.json is written.
        t_human = Task(
            task_id="t_human",
            image="ubuntu",
            command="echo ignored",
            env={
                "MATTERSTACK_OPERATOR": "human.default",
                "INSTRUCTIONS": "Write response.json with status COMPLETED.",
            },
            dependencies={"t_compute"},
        )

        # Experiment task: completes when experiment_result.json is written.
        t_experiment = Task(
            task_id="t_experiment",
            image="ubuntu",
            command="echo ignored",
            env={"MATTERSTACK_OPERATOR": "experiment.default"},
            dependencies={"t_human"},
        )

        wf.add_task(t_compute)
        wf.add_task(t_human)
        wf.add_task(t_experiment)

        self._planned_once = True
        return wf

    def analyze(self, state: Any, results: Any) -> Any:
        # No-op: we only care about completing the workflow deterministically.
        return state


def _write_operators_yaml(path: Path, *, run_root: Path) -> None:
    """
    Write a minimal operators.yaml for the integration test.

    hpc.default is backed by LocalBackend. We set workspace_root to the run root so any
    backend-internal state stays inside tmp_path.
    """
    cfg = {
        "operators": {
            "hpc.default": {
                "kind": "hpc",
                "backend": {
                    "type": "local",
                    "workspace_root": str(run_root),
                    "dry_run": True,
                },
            },
            "human.default": {"kind": "human"},
            "experiment.default": {"kind": "experiment"},
        }
    }
    path.write_text(json.dumps(cfg, indent=2) + "\n")


def _attempt_row_by_task_id(store: SQLiteStateStore, *, task_id: str) -> TaskAttemptModel:
    attempt = store.get_current_attempt(task_id)
    assert attempt is not None, f"expected attempt for {task_id}"
    return attempt


def _assert_attempt_scoped_evidence_dir(
    run_root: Path, *, task_id: str, attempt_id: str
) -> Path:
    expected = attempt_evidence_dir(run_root, task_id, attempt_id)
    assert expected.exists(), f"missing attempt evidence dir: {expected}"
    assert expected.is_dir()
    return expected


def test_run_lifecycle_mixed_operators_routed_via_operator_key(tmp_path: Path) -> None:
    ws = "ws_mixed_ops"
    campaign = _MixedOperatorsCampaign()

    # 1) Initialize run in tmpdir
    run_handle = initialize_run(ws, campaign, base_path=tmp_path)
    run_root = run_handle.root_path

    # 2) Write operators.yaml into tmpdir and build operator registry from config
    operators_yaml = tmp_path / "operators.yaml"
    _write_operators_yaml(operators_yaml, run_root=run_root)

    operators_cfg = load_operators_config(operators_yaml)
    operator_registry = get_cached_operator_registry_from_operators_config(
        run_handle, operators_cfg
    )

    # 3) Tick the run until completion (bounded) and complete human/experiment externally
    store = SQLiteStateStore(run_handle.db_path)

    max_ticks = 50
    status: Optional[str] = None
    wrote_human = False
    wrote_experiment = False

    for _ in range(max_ticks):
        status = step_run(run_handle, campaign, operator_registry=operator_registry)

        # Once attempts exist, write the completion files into attempt evidence dirs.
        # Note: Human/Experiment operators are now attempt-aware and use attempt-scoped dirs.
        if not wrote_human:
            attempt = store.get_current_attempt("t_human")
            if attempt is not None:
                human_dir = _assert_attempt_scoped_evidence_dir(
                    run_root, task_id="t_human", attempt_id=attempt.attempt_id
                )
                (human_dir / "response.json").write_text(
                    json.dumps({"status": "COMPLETED", "data": {"answer": "ok"}}, indent=2)
                    + "\n"
                )
                wrote_human = True

        if not wrote_experiment:
            attempt = store.get_current_attempt("t_experiment")
            if attempt is not None:
                exp_dir = _assert_attempt_scoped_evidence_dir(
                    run_root, task_id="t_experiment", attempt_id=attempt.attempt_id
                )
                (exp_dir / "experiment_result.json").write_text(
                    json.dumps({"status": "COMPLETED", "data": {"result": 123}, "files": []}, indent=2)
                    + "\n"
                )
                wrote_experiment = True

        if status == "COMPLETED":
            break

    assert status == "COMPLETED"

    # 4) Assert attempts exist and operator_key routing persisted for each task
    compute_attempt = _attempt_row_by_task_id(store, task_id="t_compute")
    human_attempt = _attempt_row_by_task_id(store, task_id="t_human")
    exp_attempt = _attempt_row_by_task_id(store, task_id="t_experiment")

    assert compute_attempt.operator_key == "hpc.default"
    assert human_attempt.operator_key == "human.default"
    assert exp_attempt.operator_key == "experiment.default"

    # 5) Assert attempt-scoped evidence directories exist for each attempt
    _assert_attempt_scoped_evidence_dir(
        run_root, task_id="t_compute", attempt_id=compute_attempt.attempt_id
    )
    _assert_attempt_scoped_evidence_dir(
        run_root, task_id="t_human", attempt_id=human_attempt.attempt_id
    )
    _assert_attempt_scoped_evidence_dir(
        run_root, task_id="t_experiment", attempt_id=exp_attempt.attempt_id
    )

    # 6) Assert persisted attempt rows in DB (operator_key column) as a stronger integration check
    engine = create_engine(f"sqlite:///{run_handle.db_path}")
    with Session(engine) as session:
        rows = session.scalars(
            select(TaskAttemptModel).where(TaskAttemptModel.run_id == run_handle.run_id)
        ).all()
        by_task: Dict[str, TaskAttemptModel] = {r.task_id: r for r in rows}
        assert by_task["t_compute"].operator_key == "hpc.default"
        assert by_task["t_human"].operator_key == "human.default"
        assert by_task["t_experiment"].operator_key == "experiment.default"
