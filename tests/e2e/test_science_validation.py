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

# --- Helpers ---

def load_campaign(slug):
    ws_path = Path("workspaces") / slug / "main.py"
    if not ws_path.exists():
        raise FileNotFoundError(f"Workspace not found: {ws_path}")
        
    import importlib.util
    spec = importlib.util.spec_from_file_location(f"workspace.{slug}", ws_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_campaign()

def run_loop_until_completion(handle, campaign, max_ticks=60):
    store = SQLiteStateStore(handle.db_path)
    for _ in range(max_ticks):
        status = step_run(handle, campaign)
        if status in ["COMPLETED", "FAILED"]:
            return status
        time.sleep(0.5)
    return "TIMEOUT"

# --- Tests ---

def test_battery_science(tmp_path):
    """
    Validates that the Battery Screening workspace produces a model card with statistics.
    """
    base_path = tmp_path / "workspaces"
    (base_path / "battery_screening").mkdir(parents=True)
    
    campaign = load_campaign("demos/battery_screening")
    handle = initialize_run("demos/battery_screening", campaign, base_path=base_path)
    
    # v0.2.5: "Local" execution_mode is simulated for non-operator tasks.
    # Use "HPC" to run compute tasks locally via ComputeOperator + LocalBackend.
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC", "max_hpc_jobs_per_run": 2}, f)
        
    status = run_loop_until_completion(handle, campaign)
    assert status == "COMPLETED"
    
    # Validation: Check Model Card
    # The script outputs model_card.md in the run directory (cwd of the script)
    # The script runs in {run_dir}/aggregator/ usually, or checks where it puts it.
    # Looking at train_model.py, it writes to "model_card.md" in CWD.
    # The aggregator task runs in a specific directory?
    # No, usually MatterStack runs tasks in their own directory OR the run root?
    # Let's check where the file ends up.
    # If it's a Local execution, the CWD is set to the task directory usually?
    # We might need to hunt for the file.
    
    # Search for model_card.md in the run directory
    model_cards = list(handle.root_path.rglob("model_card.md"))
    assert len(model_cards) > 0, "model_card.md not found"
    
    card_path = model_cards[0]
    content = card_path.read_text()
    
    print(f"Model Card Content:\n{content}")
    
    assert "Failure Rate" in content
    assert "Model Statistics" in content
    assert "Average Formation Energy" in content


def test_thin_film_science(tmp_path):
    """
    Validates that Thin Film Lab produces a final report with drift metrics.
    """
    base_path = tmp_path / "workspaces"
    (base_path / "thin_film_lab").mkdir(parents=True)
    
    campaign = load_campaign("demos/thin_film_lab")
    handle = initialize_run("demos/thin_film_lab", campaign, base_path=base_path)

    # Configure for Local execution
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "Local"}, f)
    
    # Start robot thread
    stop_event = threading.Event()
    
    def auto_robot():
        legacy_dir = handle.root_path / "operators" / "experiment"
        tasks_dir = handle.root_path / "tasks"
        
        while not stop_event.is_set():
            # Preferred (v0.2.5+): attempt-scoped evidence dirs for ExperimentOperator
            if tasks_dir.exists():
                for d in tasks_dir.glob("*/attempts/*"):
                    # Check for experiment_request.json (ExperimentOperator marker)
                    if d.is_dir() and (d / "experiment_request.json").exists():
                        res = d / "experiment_result.json"
                        if not res.exists():
                            # Write robot data to run root
                            robot_data_path = handle.root_path / "robot_data.json"
                            with open(robot_data_path, "w") as f:
                                json.dump({"conductivity_exp": 0.05, "stability_exp": 0.8}, f)
                            # Create result
                            with open(res, "w") as f:
                                f.write('{"status": "COMPLETED", "data": {"yield": 0.99}, "files": []}')
            
            # Legacy: operators/experiment/<uuid>/
            if legacy_dir.exists():
                for d in legacy_dir.iterdir():
                    res = d / "experiment_result.json"
                    if d.is_dir() and not res.exists():
                        robot_data_path = handle.root_path / "robot_data.json"
                        with open(robot_data_path, "w") as f:
                            json.dump({"conductivity_exp": 0.05, "stability_exp": 0.8}, f)
                        with open(res, "w") as f:
                            f.write('{"status": "COMPLETED", "data": {"yield": 0.99}, "files": []}')
            
            time.sleep(0.5)
            
    t = threading.Thread(target=auto_robot)
    t.start()
    
    try:
        status = run_loop_until_completion(handle, campaign)
    finally:
        stop_event.set()
        t.join()
        
    assert status == "COMPLETED"
    
    # Validation: Check Final Report
    reports = list(handle.root_path.rglob("final_report.json"))
    assert len(reports) > 0, "final_report.json not found"
    
    report_path = reports[0]
    with open(report_path, "r") as f:
        data = json.load(f)
        
    assert "metrics" in data
    assert "overall_drift" in data["metrics"]
    assert isinstance(data["metrics"]["overall_drift"], float)


def test_catalyst_science(tmp_path):
    """
    Validates that Catalyst workspace produces a ranking of candidates.
    """
    base_path = tmp_path / "workspaces"
    (base_path / "catalyst_human_in_loop").mkdir(parents=True)
    
    campaign = load_campaign("demos/catalyst_human_in_loop")
    handle = initialize_run("demos/catalyst_human_in_loop", campaign, base_path=base_path)

    # v0.2.5: "Local" execution_mode is simulated for non-operator tasks.
    # Use "HPC" to run compute tasks locally via ComputeOperator + LocalBackend.
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)
    
    # Start human thread
    stop_event = threading.Event()
    
    def auto_approver():
        legacy_dir = handle.root_path / "operators" / "human"
        tasks_dir = handle.root_path / "tasks"

        while not stop_event.is_set():
            # Preferred (v0.2.5+): attempt-scoped evidence dirs for HumanOperator
            if tasks_dir.exists():
                for d in tasks_dir.glob("*/attempts/*"):
                    # Heuristic: HumanOperator writes instructions.md in the work dir
                    if d.is_dir() and (d / "instructions.md").exists():
                        resp = d / "response.json"
                        if not resp.exists():
                            resp.write_text(
                                '{"status": "COMPLETED", "data": {"approved": true}}'
                            )

            # Legacy: operators/human/<uuid>/
            if legacy_dir.exists():
                for d in legacy_dir.iterdir():
                    resp = d / "response.json"
                    if d.is_dir() and not resp.exists():
                        resp.write_text('{"status": "COMPLETED", "data": {"approved": true}}')

            time.sleep(0.5)

    t = threading.Thread(target=auto_approver)
    t.start()
    
    try:
        status = run_loop_until_completion(handle, campaign)
    finally:
        stop_event.set()
        t.join()
        
    assert status == "COMPLETED"
    
    # Validation: Check Ranking
    rankings = list(handle.root_path.rglob("ranking.json"))
    assert len(rankings) > 0, "ranking.json not found"
    
    ranking_path = rankings[0]
    with open(ranking_path, "r") as f:
        data = json.load(f)
        
    assert isinstance(data, list)
    assert len(data) > 0
    assert "energy" in data[0]


def test_coatings_science(tmp_path):
    """
    Validates that Coatings workspace performs active learning cycles.
    Expects > 10 completed candidates (10 initial + 5 from AL step).
    """
    base_path = tmp_path / "workspaces"
    (base_path / "coatings_active_learning").mkdir(parents=True)
    
    campaign = load_campaign("demos/coatings_active_learning")
    handle = initialize_run("demos/coatings_active_learning", campaign, base_path=base_path)
    
    # v0.2.5: "Local" execution_mode is simulated for non-operator tasks.
    # Use "HPC" to run compute tasks locally via ComputeOperator + LocalBackend.
    config_path = handle.root_path / "config.json"
    with open(config_path, "w") as f:
        json.dump({"execution_mode": "HPC"}, f)
        
    status = run_loop_until_completion(handle, campaign, max_ticks=80) # Needs more ticks for 2 cycles
    assert status == "COMPLETED"
    
    # Validation: Check State Store for completed candidates
    store = SQLiteStateStore(handle.db_path)
    # The state is serialized in the 'campaign_state' table or we can just load the file
    # But `SQLiteStateStore` doesn't strictly expose "get_state" easily without knowing the model?
    # Actually, `test_workspace_e2e` didn't check internal state logic.
    # Let's load the SQLite DB directly or parse the `campaign_state.json` if it exists (RunLifecycle saves it).
    
    state_path = handle.root_path / "campaign_state.json"
    assert state_path.exists()
    
    with open(state_path, "r") as f:
        state_data = json.load(f)
        
    # CoatingsState has 'completed_candidates' list
    completed = state_data.get("completed_candidates", [])
    
    # Should be 15 (10 initial + 5 active learning)
    assert len(completed) >= 15
    print(f"Completed Candidates: {len(completed)}")