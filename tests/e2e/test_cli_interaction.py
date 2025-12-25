import re
import sys
from pathlib import Path

import pytest

from matterstack.cli.main import main
from matterstack.storage.state_store import SQLiteStateStore

WORKSPACE_SLUG = "test_workspace_attempts"
TASK_ID = "ext_rerun_1"


def _write_workspace_campaign(workspaces_root: Path) -> None:
    """
    Create workspaces/<slug>/main.py implementing get_campaign().

    Campaign behavior:
    - Creates one ExternalTask that runs via ComputeOperator on LocalBackend (MATTERSTACK_OPERATOR=HPC).
    - Task command fails on first attempt and succeeds on second attempt by using a marker file
      placed at the run root (../../../../rerun_marker) from the backend workdir layout.
    - Adds a dependent blocker task so the run does not become terminal when ext_rerun_1 fails,
      allowing rerun to be issued while the run is still RUNNING.
    """
    ws_dir = workspaces_root / WORKSPACE_SLUG
    ws_dir.mkdir(parents=True, exist_ok=True)

    campaign_py = ws_dir / "main.py"
    campaign_py.write_text(
        """
from __future__ import annotations

from typing import Any, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.core.external import ExternalTask


class AttemptE2ECampaign(Campaign):
    def plan(self, state: Any) -> Optional[Workflow]:
        if state is None:
            wf = Workflow()

            # ExternalTask: force ComputeOperator path (attempt-aware) using MATTERSTACK_OPERATOR.
            # Command:
            # - attempt 1: create rerun_marker at run root and exit 1 (FAILED)
            # - attempt 2: rerun_marker exists, exit 0 (COMPLETED)
            cmd = (
                "bash -lc "
                "\\""
                "if [ -f ../../../../rerun_marker ]; then "
                "  echo 'attempt2 ok'; exit 0; "
                "else "
                "  echo 'attempt1 fail'; touch ../../../../rerun_marker; exit 1; "
                "fi"
                "\\""
            )

            t = ExternalTask(
                task_id="ext_rerun_1",
                image="ubuntu:latest",
                command=cmd,
                env={"MATTERSTACK_OPERATOR": "HPC"},
            )
            wf.add_task(t)

            # Keep the run RUNNING even if ext_rerun_1 fails by adding a dependent task.
            blocker = Task(
                task_id="blocker_after_ext_rerun_1",
                image="ubuntu:latest",
                command="echo blocker",
                dependencies={"ext_rerun_1"},
            )
            wf.add_task(blocker)
            return wf

        return None

    def analyze(self, state: Any, results: Any) -> Any:
        return {"done": True}


def get_campaign():
    return AttemptE2ECampaign()
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


def _db_path(tmp_path: Path, run_id: str) -> Path:
    return tmp_path / "workspaces" / WORKSPACE_SLUG / "runs" / run_id / "state.sqlite"


@pytest.fixture
def e2e_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Ensure Path("workspaces") in the CLI points at tmp_path/workspaces.
    monkeypatch.chdir(tmp_path)
    _write_workspace_campaign(tmp_path / "workspaces")
    return tmp_path


def test_cli_revive_transitions_run_status(e2e_workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # Initialize run via CLI
    init_out = _run_cli(capsys, ["init", WORKSPACE_SLUG])
    run_id = _parse_run_id(init_out)

    store = SQLiteStateStore(_db_path(e2e_workspace, run_id))
    store.set_run_status(run_id, "COMPLETED", reason="forced by test")

    # Revive should set to PENDING and record reason
    revive_out = _run_cli(capsys, ["revive", run_id])
    assert f"Run {run_id} revived:" in revive_out

    assert store.get_run_status(run_id) == "PENDING"
    reason = store.get_run_status_reason(run_id)
    assert reason is not None
    assert "Revived via CLI" in reason


def test_cli_rerun_creates_second_attempt_and_attempts_lists_two(
    e2e_workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Initialize run via CLI
    init_out = _run_cli(capsys, ["init", WORKSPACE_SLUG])
    run_id = _parse_run_id(init_out)

    store = SQLiteStateStore(_db_path(e2e_workspace, run_id))

    # Step 1: submit attempt 1
    _run_cli(capsys, ["step", run_id])

    # Step 2: poll -> fail attempt 1 (command writes marker and exits 1)
    _run_cli(capsys, ["step", run_id])

    attempts_1 = store.list_attempts(TASK_ID)
    assert len(attempts_1) == 1
    assert attempts_1[0].attempt_index == 1
    assert attempts_1[0].status in {"FAILED", "COMPLETED", "WAITING_EXTERNAL", "RUNNING", "SUBMITTED"}

    # Issue rerun via CLI (force avoids interactive prompt)
    rerun_out = _run_cli(capsys, ["rerun", run_id, TASK_ID, "--force"])
    assert "Rerun queued" in rerun_out

    # Step 3: submit attempt 2
    _run_cli(capsys, ["step", run_id])

    # Step 4: poll -> complete attempt 2 (marker exists so command exits 0)
    _run_cli(capsys, ["step", run_id])

    attempts_2 = store.list_attempts(TASK_ID)
    assert len(attempts_2) == 2
    assert attempts_2[0].attempt_index == 1
    assert attempts_2[1].attempt_index == 2
    assert attempts_2[0].attempt_id != attempts_2[1].attempt_id

    # Validate attempts CLI output includes two rows (plus header)
    attempts_out = _run_cli(capsys, ["attempts", run_id, TASK_ID])
    lines = [ln for ln in attempts_out.splitlines() if ln.strip()]

    assert len(lines) == 3, f"Expected header + 2 rows, got:\n{attempts_out}"
    assert lines[0].startswith(
        "attempt_id\tattempt_index\tstatus\toperator_type\texternal_id\tartifact_path\tconfig_hash"
    )

    # Ensure both attempt_ids appear in output
    assert attempts_2[0].attempt_id in attempts_out
    assert attempts_2[1].attempt_id in attempts_out

    # Ensure config_hash is non-empty and looks like a sha256 for both rows
    row1 = lines[1].split("\t")
    row2 = lines[2].split("\t")
    assert len(row1) == 7
    assert len(row2) == 7
    assert re.fullmatch(r"[0-9a-f]{64}", row1[-1]), f"Unexpected config_hash row1: {row1[-1]}"
    assert re.fullmatch(r"[0-9a-f]{64}", row2[-1]), f"Unexpected config_hash row2: {row2[-1]}"
