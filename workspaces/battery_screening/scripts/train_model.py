import argparse
import json
import os
from pathlib import Path
import statistics

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
    If a legacy path like '../cand_000/results.json' is requested but does not exist under v0.2.5,
    try to resolve it into the attempt-scoped evidence layout:
        <run_root>/tasks/<task_id>/attempts/*/<filename>
    """
    if requested.exists():
        return requested

    # Heuristic: infer task_id from the requested path (…/<task_id>/<filename>)
    parts = requested.parts
    if len(parts) < 2:
        return None

    task_id = parts[-2]
    filename = parts[-1]

    run_root = _find_run_root(cwd)
    attempts_dir = run_root / "tasks" / task_id / "attempts"
    if not attempts_dir.exists():
        return None

    matches = sorted(attempts_dir.glob(f"*/{filename}"))
    return matches[-1] if matches else None


def train_model(input_files):
    results = []
    failed_count = 0

    print(f"Checking {len(input_files)} potential input files...")

    cwd = Path.cwd()

    for fpath in input_files:
        requested = Path(fpath)
        path = _resolve_attempt_scoped_path(cwd, requested) or requested

        if path.exists():
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    results.append(data)
            except Exception as e:
                print(f"Error reading {path}: {e}")
                failed_count += 1
        else:
            # This is expected for failed upstream tasks
            print(f"Missing {path}")
            failed_count += 1

    success_count = len(results)
    print(f"Found {success_count} successful results.")
    print(f"Encountered {failed_count} missing/failed inputs.")
    
    # In the demo + E2E validation we always want a model card, even if all candidates failed.
    avg_e_form_str = "N/A"
    avg_voltage_str = "N/A"
    
    if success_count == 0:
        print("No valid data found to train model! Generating placeholder model card.")
    else:
        # Mock Training
        print("Training model on aggregated data...")
        e_forms = [r["E_form"] for r in results]
        voltages = [r["voltage"] for r in results]
        
        avg_e_form = statistics.mean(e_forms)
        avg_voltage = statistics.mean(voltages)
        
        avg_e_form_str = f"{avg_e_form:.4f}"
        avg_voltage_str = f"{avg_voltage:.4f}"
    
    # Generate Model Card (always)
    model_card = f"""
# Model Card: Battery Material Predictor

## Training Data
- Total Candidates: {len(input_files)}
- Successful Samples: {success_count}
- Failed/Missing Samples: {failed_count}
- Failure Rate: {failed_count / len(input_files) * 100:.1f}%

## Model Statistics
- Average Formation Energy: {avg_e_form_str} eV
- Average Voltage: {avg_voltage_str} V

## Conclusion
The model has been trained on the available data. The workflow demonstrated robustness by continuing despite {failed_count} upstream failures.
"""
    
    with open("model_card.md", "w") as f:
        f.write(model_card)
        
    print("Model card generated: model_card.md")

def main():
    parser = argparse.ArgumentParser(description="Aggregate results and train model")
    # We expect a list of files passed as arguments
    parser.add_argument("input_files", nargs="+", help="List of input files to check")
    
    args = parser.parse_args()
    
    train_model(args.input_files)

if __name__ == "__main__":
    main()