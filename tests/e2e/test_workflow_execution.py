import json
import threading
import time
from pathlib import Path

from matterstack.orchestration.run_lifecycle import initialize_run, step_run
from matterstack.storage.state_store import SQLiteStateStore


# Helper to load campaign class dynamically
def load_campaign(slug):
    ws_path = Path("workspaces") / slug / "main.py"
    if not ws_path.exists():
        raise FileNotFoundError(f"Workspace not found: {ws_path}")

    import importlib.util
    spec = importlib.util.spec_from_file_location(f"workspace.{slug}", ws_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_campaign()

def run_loop_until_completion(handle, campaign, max_ticks=40):
    store = SQLiteStateStore(handle.db_path)
    for _ in range(max_ticks):
        status = step_run(handle, campaign)
        if status in ["COMPLETED", "FAILED"]:
            return status
        time.sleep(0.5)
    return "TIMEOUT"

def test_battery_screening_e2e(tmp_path):
    # Use tmp_path to avoid cluttering real workspace runs
    base_path = tmp_path / "workspaces"
    (base_path / "battery_screening").mkdir(parents=True)

    campaign = load_campaign("demos/battery_screening")

    handle = initialize_run("demos/battery_screening", campaign, base_path=base_path)

    # Configure for Local execution
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "Simulation"}, f)

    status = run_loop_until_completion(handle, campaign)
    assert status == "COMPLETED"

    store = SQLiteStateStore(handle.db_path)
    # Verify iteration 1 (Aggregator) happened
    tasks = store.get_tasks(handle.run_id)
    assert any(t.task_id == "aggregator" for t in tasks)

def test_catalyst_human_e2e(tmp_path):
    base_path = tmp_path / "workspaces"
    (base_path / "catalyst_human_in_loop").mkdir(parents=True)

    campaign = load_campaign("demos/catalyst_human_in_loop")
    handle = initialize_run("demos/catalyst_human_in_loop", campaign, base_path=base_path)

    # Start thread to simulate human approval
    stop_event = threading.Event()

    def auto_approver():
        legacy_dir = handle.root_path / "operators" / "human"
        tasks_dir = handle.root_path / "tasks"
        print(f"DEBUG: Watching {legacy_dir} and {tasks_dir}")

        while not stop_event.is_set():
            # Preferred (v0.2.5+): attempt-scoped evidence dirs for HumanOperator
            if tasks_dir.exists():
                for d in tasks_dir.glob("*/attempts/*"):
                    if d.is_dir() and (d / "instructions.md").exists():
                        resp = d / "response.json"
                        if not resp.exists():
                            print(f"DEBUG: Creating response in {d}")
                            resp.write_text(
                                '{"status": "COMPLETED", "data": {"approved": true}}'
                            )

            # Legacy: operators/human/<uuid>/
            if legacy_dir.exists():
                for d in legacy_dir.iterdir():
                    resp = d / "response.json"
                    if d.is_dir() and not resp.exists():
                        print(f"DEBUG: Creating response in {d}")
                        resp.write_text('{"status": "COMPLETED", "data": {"approved": true}}')

            time.sleep(0.5)

    t = threading.Thread(target=auto_approver)
    t.start()

    try:
        status = run_loop_until_completion(handle, campaign, max_ticks=40)
    finally:
        stop_event.set()
        t.join()

    assert status == "COMPLETED"

def test_thin_film_e2e(tmp_path):
    base_path = tmp_path / "workspaces"
    (base_path / "thin_film_lab").mkdir(parents=True)

    campaign = load_campaign("demos/thin_film_lab")
    handle = initialize_run("demos/thin_film_lab", campaign, base_path=base_path)

    # Start thread to simulate robot
    stop_event = threading.Event()

    def auto_robot():
        legacy_dir = handle.root_path / "operators" / "experiment"
        tasks_dir = handle.root_path / "tasks"
        print(f"DEBUG: Watching {legacy_dir} and {tasks_dir}")

        while not stop_event.is_set():
            # Preferred (v0.2.5+): attempt-scoped evidence dirs for ExperimentOperator
            if tasks_dir.exists():
                for d in tasks_dir.glob("*/attempts/*"):
                    # Check for experiment_request.json (ExperimentOperator marker)
                    if d.is_dir() and (d / "experiment_request.json").exists():
                        res = d / "experiment_result.json"
                        if not res.exists():
                            print(f"DEBUG: Creating result in {d}")
                            with open(res, "w") as f:
                                f.write('{"status": "COMPLETED", "data": {"yield": 0.99}, "files": []}')

            # Legacy: operators/experiment/<uuid>/
            if legacy_dir.exists():
                for d in legacy_dir.iterdir():
                    res = d / "experiment_result.json"
                    if d.is_dir() and not res.exists():
                        print(f"DEBUG: Creating result in {d}")
                        with open(res, "w") as f:
                            f.write('{"status": "COMPLETED", "data": {"yield": 0.99}, "files": []}')

            time.sleep(0.5)

    t = threading.Thread(target=auto_robot)
    t.start()

    try:
        status = run_loop_until_completion(handle, campaign, max_ticks=40)
    finally:
        stop_event.set()
        t.join()

    assert status == "COMPLETED"
