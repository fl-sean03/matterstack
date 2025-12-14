import pytest
import time
import shutil
import threading
import json
import sys
import os
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
    
    campaign = load_campaign("battery_screening")
    
    handle = initialize_run("battery_screening", campaign, base_path=base_path)
    
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
    
    campaign = load_campaign("catalyst_human_in_loop")
    handle = initialize_run("catalyst_human_in_loop", campaign, base_path=base_path)
    
    # Start thread to simulate human approval
    stop_event = threading.Event()
    
    def auto_approver():
        op_dir = handle.root_path / "operators" / "human"
        print(f"DEBUG: Watching {op_dir}")
        while not stop_event.is_set():
            if op_dir.exists():
                for d in op_dir.iterdir():
                    resp = d / "response.json"
                    if d.is_dir() and not resp.exists():
                         print(f"DEBUG: Creating response in {d}")
                         # Create response
                         with open(resp, "w") as f:
                             f.write('{"status": "COMPLETED", "data": {"approved": true}}')
            else:
                # print(f"DEBUG: {op_dir} does not exist yet")
                pass
            time.sleep(0.5)

    t = threading.Thread(target=auto_approver)
    t.start()
    
    try:
        status = run_loop_until_completion(handle, campaign, max_ticks=40)
    finally:
        stop_event.set()
        t.join()
        
    assert status == "COMPLETED"

@pytest.mark.skip(reason="Workspace 'thin_film_lab' is missing from repo")
def test_thin_film_e2e(tmp_path):
    base_path = tmp_path / "workspaces"
    (base_path / "thin_film_lab").mkdir(parents=True)

    campaign = load_campaign("thin_film_lab")
    handle = initialize_run("thin_film_lab", campaign, base_path=base_path)
    
    # Start thread to simulate robot
    stop_event = threading.Event()
    
    def auto_robot():
        op_dir = handle.root_path / "operators" / "experiment"
        print(f"DEBUG: Watching {op_dir}")
        while not stop_event.is_set():
            if op_dir.exists():
                for d in op_dir.iterdir():
                    res = d / "experiment_result.json"
                    if d.is_dir() and not res.exists():
                        print(f"DEBUG: Creating result in {d}")
                        # Create result
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