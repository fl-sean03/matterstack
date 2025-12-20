import json
import sys
import argparse
from pathlib import Path


def _find_run_root(start: Path) -> Path:
    """
    Best-effort: find the run root by walking up from the current working directory.

    In v0.2.5 attempt-aware layout, compute tasks run under:
        <run_root>/tasks/<task_id>/attempts/<attempt_id>/

    We detect <run_root> by looking for state.sqlite.
    """
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        if (p / "state.sqlite").exists():
            return p
    return start.resolve()


def _resolve_attempt_scoped_path(cwd: Path, requested: Path) -> Path | None:
    """
    If a legacy path like '../sim_predict/sim_results.json' is requested but does not exist under v0.2.5,
    try to resolve it into the attempt-scoped evidence layout:
        <run_root>/tasks/<task_id>/attempts/*/<filename>
    
    Also handles run-root-relative paths like 'robot_data.json'.
    """
    if requested.exists():
        return requested

    # Heuristic: infer task_id from the requested path (…/<task_id>/<filename>)
    parts = requested.parts
    
    run_root = _find_run_root(cwd)
    
    # Case 1: Path like "../sim_predict/sim_results.json" -> task_id = "sim_predict"
    if len(parts) >= 2:
        task_id = parts[-2]
        filename = parts[-1]
        
        attempts_dir = run_root / "tasks" / task_id / "attempts"
        if attempts_dir.exists():
            matches = sorted(attempts_dir.glob(f"*/{filename}"))
            if matches:
                return matches[-1]
    
    # Case 2: Path like "../robot_data.json" (run-root-relative file)
    if len(parts) >= 1:
        filename = parts[-1]
        # Check run root directly
        root_path = run_root / filename
        if root_path.exists():
            return root_path
        
        # Check if it was written by experiment operator to a task's attempt dir
        # The robot_data.json might be in the robot_execution task's attempt directory
        for task_dir in (run_root / "tasks").glob("*/attempts/*/"):
            candidate = task_dir / filename
            if candidate.exists():
                return candidate

    return None


def main():
    """
    Reads sim_results.json and experiment_results.json, reconciles them.
    Output: final_report.json
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim", required=True, help="Path to simulation results")
    parser.add_argument("--exp", required=True, help="Path to experiment results")
    args = parser.parse_args()

    cwd = Path.cwd()
    
    # Resolve paths using v0.2.5+ layout if needed
    sim_requested = Path(args.sim)
    exp_requested = Path(args.exp)
    
    sim_path = _resolve_attempt_scoped_path(cwd, sim_requested) or sim_requested
    exp_path = _resolve_attempt_scoped_path(cwd, exp_requested) or exp_requested
    
    print(f"Resolved sim path: {sim_path}")
    print(f"Resolved exp path: {exp_path}")

    try:
        with open(sim_path, 'r') as f:
            sim_data = json.load(f)
            
        with open(exp_path, 'r') as f:
            exp_data = json.load(f)
            
        # Calculate Error
        sim_cond = sim_data.get("conductivity_sim")
        exp_cond = exp_data.get("conductivity_exp")
        
        sim_stab = sim_data.get("stability_sim")
        exp_stab = exp_data.get("stability_exp")
        
        cond_error = abs(sim_cond - exp_cond) / exp_cond if exp_cond else 0
        stab_error = abs(sim_stab - exp_stab) / exp_stab if exp_stab else 0
        
        report = {
            "candidate_id": sim_data.get("candidate_id"),
            "sim_data": sim_data,
            "exp_data": exp_data,
            "metrics": {
                "conductivity_error_rel": cond_error,
                "stability_error_rel": stab_error,
                "overall_drift": (cond_error + stab_error) / 2
            }
        }
        
        print(f"Reconciliation Complete. Drift: {report['metrics']['overall_drift']:.2%}")
        
        with open("final_report.json", "w") as f:
            json.dump(report, f, indent=2)
            
    except Exception as e:
        print(f"Error reconciling data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
