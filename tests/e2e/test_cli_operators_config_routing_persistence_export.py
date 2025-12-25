import json
import re
import sys
import time
from pathlib import Path

import pytest

from matterstack.cli.main import main
from matterstack.storage.state_store import SQLiteStateStore

WORKSPACE_SLUG = "test_workspace_ops_cfg_e2e"

TASK_COMPUTE_1 = "compute_before_gate"
TASK_GATE = "human_gate"
TASK_COMPUTE_2 = "compute_after_gate"


def _write_workspace_campaign(workspaces_root: Path) -> None:
    """
    Create workspaces/<slug>/main.py implementing get_campaign().

    Campaign behavior:
    - compute_before_gate: routed explicitly to hpc.default (ComputeOperator on LocalBackend dry_run)
    - human_gate: GateTask routed to human.default (HumanOperator) and completed by the test via response.json
    - compute_after_gate: routed to hpc.default, depends on human_gate
    """
    ws_dir = workspaces_root / WORKSPACE_SLUG
    ws_dir.mkdir(parents=True, exist_ok=True)

    campaign_py = ws_dir / "main.py"
    campaign_py.write_text(
        """
from __future__ import annotations

from typing import Any, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.gate import GateTask
from matterstack.core.workflow import Workflow, Task


class OperatorsConfigE2ECampaign(Campaign):
    def plan(self, state: Any) -> Optional[Workflow]:
        if state is None:
            wf = Workflow()

            # Compute step routed to canonical operator_key via env.
            t1 = Task(
                task_id="compute_before_gate",
                image="ubuntu:latest",
                command="echo compute1",
                env={"MATTERSTACK_OPERATOR": "hpc.default"},
            )
            wf.add_task(t1)

            # Human gate must be completed by a HumanOperator response.json (test writes it).
            gate = GateTask(
                task_id="human_gate",
                image="ubuntu:latest",
                message="Approve the gate (test will auto-complete).",
                dependencies={"compute_before_gate"},
            )
            wf.add_task(gate)

            # Another compute step after the gate to ensure the run can complete.
            t2 = Task(
                task_id="compute_after_gate",
                image="ubuntu:latest",
                command="echo compute2",
                dependencies={"human_gate"},
                env={"MATTERSTACK_OPERATOR": "hpc.default"},
            )
            wf.add_task(t2)

            return wf

        return None

    def analyze(self, state: Any, results: Any) -> Any:
        return {"done": True}


def get_campaign():
    return OperatorsConfigE2ECampaign()
""".lstrip()
    )


def _write_operators_yaml(path: Path) -> None:
    """
    Minimal operators.yaml for v0.2.6 Operator System v2.
    - hpc.default: LocalBackend dry_run for CI-friendly deterministic execution
    - human.default: HumanOperator for gate
    """
    path.write_text(
        """
operators:
  hpc.default:
    kind: hpc
    backend:
      type: local
      dry_run: true
  human.default:
    kind: human
""".lstrip()
    )


def _run_cli(capsys: pytest.CaptureFixture[str], argv: list[str]) -> str:
    """
    Run the CLI entrypoint with patched argv and return stdout.
    """
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(sys, "argv", ["main.py", *argv])
        main()
    out = capsys.readouterr().out
    return out


def _parse_run_id(init_stdout: str) -> str:
    m = re.search(r"Run initialized:\s+([^\s]+)", init_stdout)
    assert m, f"Could not parse run_id from init output:\n{init_stdout}"
    return m.group(1)


def _run_root(tmp_path: Path, run_id: str) -> Path:
    return tmp_path / "workspaces" / WORKSPACE_SLUG / "runs" / run_id


def _db_path(tmp_path: Path, run_id: str) -> Path:
    return _run_root(tmp_path, run_id) / "state.sqlite"


def _parse_attempts_tsv(tsv: str) -> tuple[list[str], list[list[str]]]:
    lines = [ln for ln in tsv.splitlines() if ln.strip()]
    assert lines, f"Expected TSV output, got empty:\n{tsv}"
    header = lines[0].split("\t")
    rows = [ln.split("\t") for ln in lines[1:]]
    return header, rows


@pytest.fixture
def e2e_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Ensure Path("workspaces") in the CLI points at tmp_path/workspaces.
    monkeypatch.chdir(tmp_path)
    _write_workspace_campaign(tmp_path / "workspaces")
    return tmp_path


def test_cli_operators_config_routing_persistence_and_evidence_export(
    e2e_workspace: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Avoid slow sleeps in CLI loop for fast CI (<2s).
    monkeypatch.setattr(time, "sleep", lambda _secs: None)

    # Prevent LocalBackend.download from copytree recursion when src==dst (LocalBackend case).
    # For this E2E we only care about routing/persistence/export fields, not file download behavior.
    from matterstack.runtime.backends.local import LocalBackend

    async def _noop_download(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(LocalBackend, "download", _noop_download, raising=True)

    operators_yaml = e2e_workspace / "operators.yaml"
    _write_operators_yaml(operators_yaml)

    # Initialize run via CLI, persisting wiring snapshot immediately (Subtask 6).
    init_out = _run_cli(
        capsys, ["init", WORKSPACE_SLUG, "--operators-config", str(operators_yaml)]
    )
    run_id = _parse_run_id(init_out)

    run_root = _run_root(e2e_workspace, run_id)
    snap_dir = run_root / "operators_snapshot"
    snap_yaml = snap_dir / "operators.yaml"
    meta_json = snap_dir / "metadata.json"
    hist_jsonl = snap_dir / "history.jsonl"

    # Snapshot artifacts must exist immediately after init (before any step).
    assert snap_yaml.exists(), f"Expected run snapshot at {snap_yaml}"
    assert meta_json.exists(), f"Expected run snapshot metadata at {meta_json}"
    assert hist_jsonl.exists(), f"Expected run snapshot history at {hist_jsonl}"

    # Snapshot contents should match the explicitly provided operators.yaml.
    assert snap_yaml.read_bytes() == operators_yaml.read_bytes()

    # Remove the external config to prove subsequent ticks do not require re-specifying wiring.
    # With no workspace default and no env var, this forces RUN_PERSISTED snapshot usage.
    operators_yaml.unlink()

    store = SQLiteStateStore(_db_path(e2e_workspace, run_id))

    # --- v0.2.7 wiring persistence / no-flag resume regression ---
    # Step without any wiring flags (uses init-persisted RUN_PERSISTED snapshot).
    _run_cli(capsys, ["step", run_id])

    # Step until the human gate attempt exists and is waiting for external completion (no flags).
    max_steps = 25
    gate_attempt = None
    for _ in range(max_steps):
        _run_cli(capsys, ["step", run_id])
        gate_attempt = store.get_current_attempt(TASK_GATE)
        if gate_attempt is not None and (gate_attempt.status or "").upper() in {
            "WAITING_EXTERNAL",
            "RUNNING",
            "SUBMITTED",
            "CREATED",
        }:
            # Once the attempt exists, we can write response.json into its evidence directory.
            break

    assert gate_attempt is not None, "Expected a human gate attempt to be created"
    assert gate_attempt.relative_path, "Expected human attempt to have a relative_path"

    # Auto-complete the HumanOperator gate by writing response.json into the operator directory.
    # HumanOperator.check_status() looks for response.json.
    response_path = _run_root(e2e_workspace, run_id) / Path(gate_attempt.relative_path) / "response.json"
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_path.write_text(json.dumps({"status": "COMPLETED", "data": {"approved": True}}, indent=2))

    # Loop to completion WITHOUT flags (uses persisted run snapshot).
    _run_cli(capsys, ["loop", run_id])

    assert store.get_run_status(run_id) == "COMPLETED"

    # --- Persistence: operator_key stored on attempts (not just derived) ---
    a1 = store.get_current_attempt(TASK_COMPUTE_1)
    ag = store.get_current_attempt(TASK_GATE)
    a2 = store.get_current_attempt(TASK_COMPUTE_2)

    assert a1 is not None and a1.operator_key == "hpc.default"
    assert ag is not None and ag.operator_key == "human.default"
    assert a2 is not None and a2.operator_key == "hpc.default"

    # --- CLI attempts output: stable TSV format (backward compatible) ---
    #
    # operator_key provenance is asserted via the DB fields above (authoritative).
    # CLI output remains on the legacy 7-column header for compatibility with older tooling/tests.
    for task_id in [TASK_COMPUTE_1, TASK_GATE, TASK_COMPUTE_2]:
        attempts_out = _run_cli(capsys, ["attempts", run_id, task_id])
        header, rows = _parse_attempts_tsv(attempts_out)

        assert header[:7] == [
            "attempt_id",
            "attempt_index",
            "status",
            "operator_type",
            "external_id",
            "artifact_path",
            "config_hash",
        ]

        assert rows, f"Expected at least one attempt row for {task_id}, got:\n{attempts_out}"
        for row in rows:
            assert len(row) == len(header)

    # --- Evidence export: operator_key included in bundle.json ---
    _run_cli(capsys, ["export-evidence", run_id])

    evidence_dir = _run_root(e2e_workspace, run_id) / "evidence"
    bundle_path = evidence_dir / "bundle.json"
    assert bundle_path.exists(), f"Expected evidence bundle at {bundle_path}"

    # Snapshot artifacts must be copied into evidence export (v0.2.7+).
    ev_snap_dir = evidence_dir / "operators_snapshot"
    assert (ev_snap_dir / "operators.yaml").exists()
    assert (ev_snap_dir / "metadata.json").exists()
    assert (ev_snap_dir / "history.jsonl").exists()

    # Evidence snapshot should match the run snapshot we persisted.
    assert (ev_snap_dir / "operators.yaml").read_bytes() == (snap_dir / "operators.yaml").read_bytes()

    # Evidence report should mention the snapshot copy.
    report_md = evidence_dir / "report.md"
    assert report_md.exists()
    report_text = report_md.read_text(encoding="utf-8")
    assert "Copied into this evidence export under:" in report_text
    assert "operators_snapshot/operators.yaml" in report_text

    bundle = json.loads(bundle_path.read_text())
    tasks = bundle["data"]["tasks"]

    def _assert_bundle_operator(task_id: str, expected_ok: str) -> None:
        assert task_id in tasks, f"Missing {task_id} in bundle tasks keys: {list(tasks.keys())}"
        tinfo = tasks[task_id]

        # Task summary field
        assert tinfo.get("operator_key") == expected_ok

        # Attempt-level field on current attempt
        cur = tinfo.get("current_attempt") or {}
        assert cur.get("operator_key") == expected_ok

        # Attempt history should also include operator_key
        attempts = tinfo.get("attempts") or []
        assert attempts, f"Expected attempts[] for {task_id}"
        assert all(a.get("operator_key") == expected_ok for a in attempts)

    _assert_bundle_operator(TASK_COMPUTE_1, "hpc.default")
    _assert_bundle_operator(TASK_GATE, "human.default")
    _assert_bundle_operator(TASK_COMPUTE_2, "hpc.default")
