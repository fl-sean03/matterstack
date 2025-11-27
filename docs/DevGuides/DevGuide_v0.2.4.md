# DevGuide_v0.2.4 – Demo Workspace Polish & Validation

## [AGENT::SUMMARY]

*   **PROJECT:** MatterStack v0.2.4 (Demo Polish)
*   **GOAL:** Ensure all demo workspaces (`battery`, `thin_film`, `catalyst`) are not just syntactically correct but functionally robust, properly handling data flow between tasks in the v0.2.x run-centric architecture.
*   **INPUT:** Analysis of existing workspace scripts revealing fragile data passing assumptions (e.g., relying on CWD).
*   **FOCUS:** Data Passing, Robustness, and Scientific Verification.

---

## 1. Implementation Thrusts

### THRUST 1: Robust Data Flow (The "Artifact Handover" Pattern)
**Goal:** Tasks should not assume files exist in CWD. They must be passed explicit paths.

*   **1.1 Battery Screening**:
    *   **Refactor**: `train_model.py` to accept a list of result JSON files (not just dirs).
    *   **Update Campaign**: `analyze` (Iter 0) collects paths of successful `results.json` files into `state.artifact_paths`. `plan` (Iter 1) constructs the aggregator command using these paths.

*   **1.2 Thin Film Lab**:
    *   **Refactor**: `reconcile_data.py` to accept `--sim-result <path>` and `--exp-result <path>`.
    *   **Update Campaign**: `analyze` captures paths of simulation and robot results. `plan` passes them to the reconciler.

*   **1.3 Catalyst Human**:
    *   **Refactor**: `propose_candidates` to output to a specific `--output` path (e.g., shared `run_data/`).
    *   **Update Campaign**: Ensure `human_approval` and `calc_adsorption` point to this shared file.

### THRUST 2: Scientific Validation
**Goal:** Verify the demos produce meaningful output.

*   **2.1 Validation Script**:
    *   Create `tests/test_demos_science.py`.
    *   For each workspace, run it end-to-end.
    *   **Assert**:
        *   Battery: `model_card.md` exists and contains "Failure Rate" (validating soft failure logic).
        *   Thin Film: `final_report.json` exists and contains "drift" metrics.
        *   Catalyst: `ranking.txt` or similar output exists.

### THRUST 3: Workspace Cleanup
**Goal:** Use the v0.2.3 public API.

*   **3.1 Imports**:
    *   Update all `main.py` files to use `from matterstack import Campaign, Task, initialize_run`.

---

## 2. Execution Plan

1.  **Refactor Scripts**: Update the python scripts in `workspaces/*/scripts/` to use `argparse` for input paths.
2.  **Update Campaigns**: Modify `main.py` `plan/analyze` logic to track and pass artifact paths.
3.  **Verify**: Run the new `test_demos_science.py`.