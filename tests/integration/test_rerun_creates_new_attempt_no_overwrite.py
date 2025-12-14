import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

from matterstack.cli.main import cmd_rerun
from matterstack.core.campaign import Campaign
from matterstack.core.external import ExternalTask
from matterstack.core.workflow import Task, Workflow
from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore


def _tick_until(
    *,
    run_handle,
    campaign,
    store: SQLiteStateStore,
    task_id: str,
    expected_task_status: str,
    max_ticks: int = 200,
    sleep_s: float = 0.01,
) -> None:
    """
    Deterministic poll loop: step_run() until a task reaches expected status
    (or we hit a bounded tick limit).
    """
    for _ in range(max_ticks):
        step_run(run_handle, campaign)

        if store.get_task_status(task_id) == expected_task_status:
            return

        # Give the LocalBackend subprocess a chance to finish and write exit_code
        time.sleep(sleep_s)

    raise AssertionError(
        f"Task {task_id} did not reach status {expected_task_status} after {max_ticks} ticks; "
        f"current={store.get_task_status(task_id)}"
    )


def test_rerun_creates_new_attempt_without_overwriting_attempt_evidence(tmp_path: Path):
    """
    Integration coverage: rerun creates a new attempt evidence directory and does not overwrite
    artifacts from the previous attempt.

    This test exercises:
    - Orchestrator tick loop via step_run()
    - Real operator path: ComputeOperator (HPC) backed by LocalBackend
    - CLI rerun wiring via cmd_rerun() (with patched find_run)
    """

    # Source file staged into each attempt by LocalBackend._stage_files (copy per submission).
    # We'll mutate it between attempts so the sentinel output differs deterministically.
    control_src = tmp_path / "control.txt"
    control_src.write_text("ONE\n")

    compute_task_id = "compute_rerun_task"
    blocker_task_id = "blocker_after_compute_rerun_task"

    class OneComputeTaskCampaign(Campaign):
        def plan(self, state: Any) -> Optional[Workflow]:
            if state is not None:
                return None

            wf = Workflow()

            compute_task = Task(
                task_id=compute_task_id,
                image="ubuntu:latest",
                command="python3 write_sentinel.py",
                env={"MATTERSTACK_OPERATOR": "HPC"},
                files={
                    "write_sentinel.py": (
                        "from pathlib import Path\n"
                        "\n"
                        "data = Path('control.txt').read_text().strip()\n"
                        "Path('sentinel.txt').write_text(data + '\\n')\n"
                    ),
                    # Path-valued file entry -> copied into attempt workdir each attempt
                    "control.txt": control_src,
                },
            )
            wf.add_task(compute_task)

            # Keep the run non-terminal deterministically:
            # after compute_task completes, this ExternalTask will enter WAITING_EXTERNAL via the
            # orchestrator's stub attempt path and keep the run in RUNNING.
            blocker = ExternalTask(
                task_id=blocker_task_id,
                image="ubuntu:latest",
                command="echo blocker",
                dependencies={compute_task_id},
            )
            wf.add_task(blocker)

            return wf

        def analyze(self, state: Any, results: Any) -> Any:
            return {"done": True}

    campaign = OneComputeTaskCampaign()

    run_handle = initialize_run("test_ws", campaign, base_path=tmp_path)
    store = SQLiteStateStore(run_handle.db_path)

    # ---- Attempt 1 ----
    _tick_until(
        run_handle=run_handle,
        campaign=campaign,
        store=store,
        task_id=compute_task_id,
        expected_task_status="COMPLETED",
    )

    attempts_1 = store.list_attempts(compute_task_id)
    assert len(attempts_1) == 1
    assert attempts_1[0].attempt_index == 1

    attempt_1_dir = run_handle.root_path / Path(attempts_1[0].relative_path)
    sentinel_1 = attempt_1_dir / "sentinel.txt"
    assert attempt_1_dir.exists()
    assert sentinel_1.exists()
    assert sentinel_1.read_text() == "ONE\n"

    # Prepare attempt 2 to produce different evidence
    control_src.write_text("TWO\n")

    # ---- Trigger rerun via CLI path ----
    @dataclass
    class _Args:
        run_id: str
        task_id: str
        recursive: bool = False
        force: bool = True

    args = _Args(run_id=run_handle.run_id, task_id=compute_task_id)

    # cmd_rerun discovers the run via find_run(); patch it to return this test run.
    with patch("matterstack.cli.main.find_run", return_value=run_handle):
        cmd_rerun(args)

    # ---- Attempt 2 ----
    _tick_until(
        run_handle=run_handle,
        campaign=campaign,
        store=store,
        task_id=compute_task_id,
        expected_task_status="COMPLETED",
    )

    attempts_2 = store.list_attempts(compute_task_id)
    assert len(attempts_2) == 2
    assert attempts_2[0].attempt_index == 1
    assert attempts_2[1].attempt_index == 2

    attempt_2_dir = run_handle.root_path / Path(attempts_2[1].relative_path)
    sentinel_2 = attempt_2_dir / "sentinel.txt"
    assert attempt_2_dir.exists()
    assert sentinel_2.exists()
    assert sentinel_2.read_text() == "TWO\n"

    # Strong "no overwrite" assertion: attempt 1 sentinel still present and unchanged
    assert attempt_1_dir != attempt_2_dir
    assert sentinel_1.exists()
    assert sentinel_1.read_text() == "ONE\n"