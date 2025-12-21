from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from matterstack.cli.operator_registry import RegistryConfig, build_operator_registry
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.local import LocalBackend
from matterstack.runtime.operators.hpc import ComputeOperator
from matterstack.storage.state_store import SQLiteStateStore


def _write_min_hpc_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "HPC_atesting_config.yaml"
    p.write_text(
        """cluster:
  name: cu_alpine
  ssh:
    host: login.rc.colorado.edu
    user: testuser
    key_path: ~/.ssh/cu_alpine
  paths:
    remote_workspace: /scratch/alpine/testuser/Agent_Runs
  slurm:
    account: ucb591_asc1
    partition: atesting
    qos: testing
    time: "00:01:00"
    ntasks: 8
"""
    )
    return p


def test_build_operator_registry_defaults_to_local_backend_for_hpc(tmp_path: Path) -> None:
    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)

    handle = RunHandle(workspace_slug="ws", run_id="r1", root_path=run_root)
    reg = build_operator_registry(handle, registry_config=RegistryConfig())

    assert set(reg.keys()) == {"Human", "Experiment", "Local", "HPC"}
    assert isinstance(reg["Local"], ComputeOperator)
    assert isinstance(reg["HPC"], ComputeOperator)

    # Back-compat: HPC defaults to local backend when not configured.
    assert isinstance(reg["HPC"].backend, LocalBackend)


def test_build_operator_registry_hpc_config_wins_and_builds_slurm_backend(tmp_path: Path) -> None:
    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)

    hpc_yaml = _write_min_hpc_yaml(tmp_path)

    handle = RunHandle(workspace_slug="ws", run_id="r1", root_path=run_root)
    reg = build_operator_registry(
        handle,
        registry_config=RegistryConfig(
            hpc_config_path=str(hpc_yaml),
            profile="ignored_profile",
            config_path=str(tmp_path / "matterstack.yaml"),
        ),
    )

    assert isinstance(reg["HPC"], ComputeOperator)
    assert isinstance(reg["HPC"].backend, SlurmBackend)


def test_cli_step_passes_operator_registry_and_accepts_hpc_config_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """
    Local-only plumbing test:
    - Ensure the CLI accepts --hpc-config on `step`
    - Ensure cmd_step builds and passes operator_registry into step_run()
    """
    import matterstack.cli.main as cli_main
    import matterstack.cli.commands.run_management as run_mgmt

    run_id = "r1"
    ws = "ws"
    run_root = tmp_path / "workspaces" / ws / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    # Minimal DB exists so operator/attempt code paths that open it won't fail unexpectedly.
    store = SQLiteStateStore(run_root / "state.sqlite")
    store.create_run(RunHandle(workspace_slug=ws, run_id=run_id, root_path=run_root))

    hpc_yaml = _write_min_hpc_yaml(tmp_path)

    handle = RunHandle(workspace_slug=ws, run_id=run_id, root_path=run_root)

    def _fake_find_run(rid: str) -> Optional[RunHandle]:
        assert rid == run_id
        return handle

    class _DummyCampaign:
        def plan(self, state: Any) -> Any:
            return None

        def analyze(self, state: Any, results: Any) -> Any:
            return None

    def _fake_load_workspace_context(workspace_slug: str) -> Any:
        assert workspace_slug == ws
        return _DummyCampaign()

    def _fake_step_run(
        run_handle: RunHandle, campaign: Any, operator_registry: Optional[Dict[str, Any]] = None
    ) -> str:
        assert operator_registry is not None
        assert "HPC" in operator_registry
        assert isinstance(operator_registry["HPC"], ComputeOperator)
        assert isinstance(operator_registry["HPC"].backend, SlurmBackend)
        return "RUNNING"

    with pytest.MonkeyPatch().context() as mp:
        # Patch in run_management module where cmd_step is defined
        mp.setattr(run_mgmt, "find_run", _fake_find_run)
        mp.setattr(run_mgmt, "load_workspace_context", _fake_load_workspace_context)
        mp.setattr(run_mgmt, "step_run", _fake_step_run)

        mp.setattr(
            sys,
            "argv",
            [
                "main.py",
                "step",
                run_id,
                "--hpc-config",
                str(hpc_yaml),
            ],
        )
        cli_main.main()

    out = capsys.readouterr().out
    assert f"Run {run_id} step complete." in out


def test_run_until_completion_threads_operator_registry(tmp_path: Path) -> None:
    import matterstack.orchestration.run_lifecycle as rl
    import matterstack.orchestration.step_execution as step_exec

    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)
    handle = RunHandle(workspace_slug="ws", run_id="r1", root_path=run_root)

    class _DummyCampaign:
        def plan(self, state: Any) -> Any:
            return None

        def analyze(self, state: Any, results: Any) -> Any:
            return None

    dummy_registry: Dict[str, Any] = {"HPC": object()}
    seen: Dict[str, Any] = {}

    def _fake_step_run(run_handle: RunHandle, campaign: Any, operator_registry: Optional[Dict[str, Any]] = None) -> str:
        seen["operator_registry"] = operator_registry
        return "COMPLETED"

    with pytest.MonkeyPatch().context() as mp:
        # Patch step_run in the step_execution module where utilities.run_until_completion imports it from
        mp.setattr(step_exec, "step_run", _fake_step_run)
        status = rl.run_until_completion(handle, _DummyCampaign(), poll_interval=0.0, operator_registry=dummy_registry)

    assert status == "COMPLETED"
    assert seen["operator_registry"] is dummy_registry


def test_compute_operator_local_backend_runs_in_attempt_evidence_dir(tmp_path: Path) -> None:
    """
    Regression test for local evidence/workdir override behavior.

    When using LocalBackend via ComputeOperator, submission should run inside the local
    attempt evidence directory (runs/<run>/tasks/<task>/attempts/<attempt>/)
    rather than a synthesized "remote-like" workdir.
    """
    ws = "ws"
    run_id = "r1"
    run_root = tmp_path / "workspaces" / ws / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    handle = RunHandle(workspace_slug=ws, run_id=run_id, root_path=run_root)

    store = SQLiteStateStore(handle.db_path)
    store.create_run(handle)

    task = Task(task_id="t1", image="local", command="echo hi", files={}, env={"MATTERSTACK_OPERATOR": "HPC"})
    wf = Workflow()
    wf.add_task(task)
    store.add_workflow(wf, run_id=run_id)

    # Create an attempt so prepare_run chooses attempt_evidence_dir.
    attempt_id = store.create_attempt(run_id=run_id, task_id=task.task_id, operator_type="HPC", operator_data={})
    assert attempt_id

    backend = LocalBackend(workspace_root=str(run_root), dry_run=True)
    op = ComputeOperator(backend=backend, slug="hpc", operator_name="HPC")

    ext = op.prepare_run(handle, task)
    abs_path_str = ext.operator_data.get("absolute_path")
    assert abs_path_str, "prepare_run should store absolute_path"

    ext = op.submit(ext)

    # LocalBackend stores the resolved workdir it used for the job in _job_paths
    used = Path(backend._job_paths[task.task_id]).resolve()  # noqa: SLF001 (test asserting internal behavior)
    expected = Path(abs_path_str).resolve()
    assert used == expected