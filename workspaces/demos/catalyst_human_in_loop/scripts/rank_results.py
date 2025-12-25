import argparse
import json
from pathlib import Path


def _find_run_root(start: Path) -> Path:
    """
    Best-effort: find the run root by walking up from the current working directory.
    We detect <run_root> by looking for state.sqlite.
    """
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        if (p / "state.sqlite").exists():
            return p
    return start.resolve()


def _resolve_attempt_scoped_path(cwd: Path, requested: Path) -> Path | None:
    """
    If a legacy path like '../calc_ads_0/energy.json' is requested but does not exist under v0.2.5,
    try to resolve it into the attempt-scoped evidence layout:
        <run_root>/tasks/<task_id>/attempts/*/<filename>
    """
    if requested.exists():
        return requested

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Input energy files")
    args = parser.parse_args()

    results = []

    print(f"Aggregating {len(args.inputs)} results...")

    cwd = Path.cwd()

    for input_path in args.inputs:
        requested = Path(input_path)
        resolved = _resolve_attempt_scoped_path(cwd, requested) or requested

        if resolved.exists():
            try:
                with open(resolved, "r") as f:
                    data = json.load(f)
                results.append(data)
            except Exception as e:
                print(f"Error reading {resolved}: {e}")
                results.append({"adsorption_energy_ev": 1.0e9})
        else:
            # Keep demo behavior robust: still emit a ranking entry per expected input.
            print(f"Missing {resolved}")
            results.append({"adsorption_energy_ev": 1.0e9})

    ranked_candidates = []
    for i, res in enumerate(results):
        ranked_candidates.append(
            {
                "id": f"candidate_{i}",
                "energy": res.get("adsorption_energy_ev", 0.0),
            }
        )

    # Sort by energy (lower is better usually)
    ranked_candidates.sort(key=lambda x: x["energy"])

    with open("ranking.json", "w") as f:
        json.dump(ranked_candidates, f, indent=2)

    print(f"Ranking complete. Processed {len(ranked_candidates)} files.")


if __name__ == "__main__":
    main()
