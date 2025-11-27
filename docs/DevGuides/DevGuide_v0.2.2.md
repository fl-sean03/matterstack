# DevGuide_v0.2.2 – Production Lifecycle, HPC Determinism & Safety

## [AGENT::SUMMARY]

*   **PROJECT:** MatterStack v0.2.2 (Production Lifecycle)
*   **GOAL:** Elevate the v0.2.1 hardened core to a full production-grade system. This involves enforcing deterministic HPC job states, explicit run lifecycle controls (cancel/pause), comprehensive evidence reconstruction, strict safety boundaries, and full workspace alignment.
*   **INPUT:** Detailed architectural requirements for Thrusts 1-7.
*   **FOCUS:** Determinism, User Control, Safety, and Usability.

---

## [AGENT::WORKFLOW_MODEL]

1.  **Strict State Definitions**: Define canonical states first (Job, Run, Task) to avoid ambiguity.
2.  **StateStore as Truth**: All control logic (cancel/pause) operates solely by updating the DB; the Orchestrator reacts to DB state.
3.  **Safety First**: Enforce path safety and manifest validation before any filesystem operations.
4.  **Verification**: Each thrust includes specific "stress" or "scenario" tests.

---

## 1. Implementation Thrusts

### THRUST 1: HPC Job Lifecycle Semantics & Concurrency Caps
**Context**: Currently, HPC job states are backend-specific (e.g., raw Slurm strings), and there is no limit on how many jobs a run can submit, leading to potential "job flooding" and ambiguous state tracking.

*   **1.1 Canonical Job States (`matterstack/core/backend.py`)**:
    *   **Action**: Define `JobState` enum with:
        *   `QUEUED` (Submitted and waiting)
        *   `RUNNING` (Active execution)
        *   `COMPLETED_OK` (Process finished with exit code 0)
        *   `COMPLETED_ERROR` (Process finished with non-zero exit code)
        *   `CANCELLED` (Stopped by user/admin)
        *   `LOST` (Backend cannot find the job, likely purged)
    *   **Validation**: Ensure all backends import this enum.

*   **1.2 Slurm Mapping (`matterstack/runtime/backends/hpc/slurm.py`)**:
    *   **Action**: Implement `_map_slurm_state(raw_state: str) -> JobState`.
        *   `PENDING`, `REQUEUED` -> `QUEUED`
        *   `RUNNING`, `COMPLETING` -> `RUNNING`
        *   `COMPLETED` -> `COMPLETED_OK`
        *   `FAILED`, `TIMEOUT`, `NODE_FAIL`, `PREEMPTED`, `OUT_OF_MEMORY` -> `COMPLETED_ERROR` (with reason)
        *   `CANCELLED*` -> `CANCELLED`
        *   (Missing/Unknown) -> `LOST`
    *   **Note**: Log specific Slurm states (like `TIMEOUT`) as the "reason".

*   **1.3 Operator State Mapping (`matterstack/runtime/operators/hpc.py`)**:
    *   **Action**: Update `DirectHPCOperator.check_status` to map `JobState` to `ExternalRunStatus`:
        *   `QUEUED` -> `WAITING_EXTERNAL` (or new `QUEUED` status if added, otherwise `WAITING_EXTERNAL` is fine for v0.2.2) -> *Decision*: Keep `WAITING_EXTERNAL` but add metadata "backend_status": "QUEUED".
        *   `RUNNING` -> `RUNNING`
        *   `COMPLETED_OK` -> `DONE_PENDING_COLLECT` (New internal status to trigger result collection)
        *   `COMPLETED_ERROR` -> `FAILED`
        *   `CANCELLED` -> `CANCELLED`
        *   `LOST` -> `FAILED` (Reason: "Job Lost")

*   **1.4 Concurrency Caps (`matterstack/orchestration/run_lifecycle.py`)**:
    *   **Action**: In `step_run`, before `PLAN` phase:
        1.  Load `config.json` from run root. Read `max_hpc_jobs_per_run` (default: 10?).
        2.  Count active ExternalRuns (`RUNNING` or `WAITING_EXTERNAL` where operator is HPC).
        3.  Calculate `slots_available`.
        4.  In `EXECUTE` phase, only submit up to `slots_available` new HPC tasks.
    *   **Logic**: Maintain a counter during the loop. Stop submitting if `slots_available <= 0`.

*   **1.5 Completion Logic (`step_run`)**:
    *   **Action**: Handle `DONE_PENDING_COLLECT`:
        *   Call `operator.collect_results()`.
        *   If success: `ExternalRun` -> `COMPLETED`, `Task` -> `COMPLETED`.
        *   If error (missing files): `ExternalRun` -> `FAILED` (or `WAITING_EXTERNAL` w/ error).

*   **Verification Plan**:
    *   `tests/test_backend_slurm_mapping.py`: Unit test mapping logic with various Slurm strings.
    *   `tests/test_concurrency_caps.py`: Set cap=2, queue 5 tasks. Assert only 2 submitted, others wait. Verify next tick submits more as jobs finish.

---

### THRUST 2: Run Lifecycle & User Controls
**Context**: Users currently cannot pause or cancel a run cleanly. The system lacks explicit run-level states.

*   **2.1 StateStore Updates (`matterstack/storage/schema.py`)**:
    *   **Action**: Add `run_status` column (String) to `RunModel`.
    *   **Action**: Add `status_reason` column (Text, Nullable).
    *   **Action**: Update `SQLiteStateStore` with `set_run_status(run_id, status, reason)` and `get_run_status`.

*   **2.2 Orchestrator Behavior (`matterstack/orchestration/run_lifecycle.py`)**:
    *   **Action**: Update `initialize_run` to set `PENDING`.
    *   **Action**: Update `step_run`:
        *   Start of tick: Read status.
        *   If `PAUSED`: Skip `EXECUTE` phase (don't submit new), but allow `POLL` (update existing). Log "Run PAUSED".
        *   If `CANCELLED` / `FAILED` / `COMPLETED`: Return immediately.
        *   End of tick: If workflow done, set `COMPLETED`.

*   **2.3 CLI Commands (`matterstack/cli/main.py`)**:
    *   **Action**: `run cancel <id> [--reason]`: Calls `store.set_run_status(..., "CANCELLED", reason)`.
    *   **Action**: `run pause <id>`: Calls `store.set_run_status(..., "PAUSED")`.
    *   **Action**: `run resume <id>`: Calls `store.set_run_status(..., "RUNNING")`.

*   **Verification Plan**:
    *   `tests/test_run_cancellation.py`: Start run, cancel it, call `step_run`, assert no new tasks submitted.
    *   `tests/test_run_pause.py`: Start run, pause it, assert running tasks finish but pending tasks don't start. Resume, assert pending tasks start.

---

### THRUST 3: Evidence Rebuild & Partial Run Handling
**Context**: Evidence export relies on `bundle.json` existing and doesn't handle failures well. It needs to be robust and stateless.

*   **3.1 Evidence Model (`matterstack/core/evidence.py`)**:
    *   **Action**: Add `run_status`, `is_complete` (bool), `task_counts` (dict) to `EvidenceBundle`.

*   **3.2 Export Logic (`matterstack/storage/export.py`)**:
    *   **Action**: Rewrite `build_evidence_bundle`:
        *   Query `StateStore` for ALL tasks/external_runs.
        *   Determine `run_status` from `RunModel`.
        *   Calculate counts (total, succeeded, failed).
        *   `is_complete` = (`run_status` == `COMPLETED`).
        *   **Crucial**: Do NOT read existing `bundle.json`. Always rebuild from DB.
    *   **Action**: Update `export_evidence_bundle` to overwrite files idempotently.

*   **3.3 Reporting**:
    *   **Action**: Update Markdown generator to include Status header (Red for FAILED/CANCELLED, Green for COMPLETED). List failed tasks specifically.

*   **3.4 CLI**:
    *   **Action**: `run export-evidence <id>`: Wraps the above functions.

*   **Verification Plan**:
    *   `tests/test_evidence_rebuild.py`: Run a campaign, delete `evidence/`, run `export-evidence`, assert files recreated and accurate.
    *   `tests/test_evidence_failure.py`: Create a FAILED run, export, check report indicates failure and reason.

---

### THRUST 4: Multi-Run Scheduler Loop & Fairness
**Context**: `run loop` currently handles one run ID. We need a daemon-like loop that services multiple runs in a workspace.

*   **4.1 Discovery (`matterstack/orchestration/run_lifecycle.py`)**:
    *   **Action**: `list_active_runs(workspace_path)`:
        *   Iterate `runs/` directories.
        *   Open DB, check `run_status`.
        *   Return list of `RunHandle` for `PENDING` / `RUNNING` / `PAUSED` runs.

*   **4.2 Loop Logic (`matterstack/cli/main.py`)**:
    *   **Action**: Update `cmd_loop` to accept optional `run_id`. If missing, enter "Multi-Run Mode".
    *   **Action**: Multi-Run Loop:
        1.  `active_runs = list_active_runs()`
        2.  Shuffle `active_runs` (Randomized Round Robin).
        3.  For each `run` in `active_runs`:
            *   Try `store.lock()`.
            *   If locked: Log "Skipping locked run {id}" and continue.
            *   If acquired: `step_run(run)`.
        4.  Sleep `tick_interval`.

*   **Verification Plan**:
    *   `tests/test_scheduler_multi.py`: Create 3 runs. Start a loop process (or thread). Assert all 3 runs progress to completion. Ensure locking prevents race conditions if two loops ran (though test might just verify one loop services all).

---

### THRUST 5: Introspection & Diagnostics (`run explain`)
**Context**: When a run stalls, users don't know why.

*   **5.1 Frontier Analysis (`matterstack/orchestration/diagnostics.py`)**:
    *   **Action**: `get_run_frontier(store)`:
        *   Find non-terminal tasks.
        *   Filter for "Blocking":
            *   waiting on dependencies (list deps).
            *   waiting on external operator (list operator status).
    *   **Action**: Helpers for Operator hints (e.g., "Human: Check file X").

*   **5.2 CLI**:
    *   **Action**: `run explain <id>`:
        *   Print Run Status.
        *   Print "Blocking Tasks":
            *   "Task T1 (ManualHPC): Waiting for output. Check `runs/.../T1/output`."
            *   "Task T2: Waiting for T1."

*   **Verification Plan**:
    *   `tests/test_run_explain.py`: Create run with a ManualHPC task in `WAITING_EXTERNAL`. Run `explain`. Assert output contains path to operator dir.

---

### THRUST 6: Path & Manifest Safety
**Context**: Security hardening. Prevent operators from writing outside the run root or crashing on bad JSON.

*   **6.1 FS Safety (`matterstack/runtime/fs_safety.py`)**:
    *   **Action**: `ensure_under_run_root(root, target)`: Resolve paths, check `target` starts with `root`. Raise `PathSafetyError`.
    *   **Action**: `operator_run_dir(root, op_type, uuid)`: Returns safe path.

*   **6.2 Operator Refactor**:
    *   **Action**: Update `DirectHPC`, `ManualHPC`, `Human`, `Experiment` to use `fs_safety`.
    *   **Action**: Ensure `collect_results` never blindly trusts paths from `operator_data` without re-validating against run root (or regenerating them).

*   **6.3 Manifest Schemas (`matterstack/runtime/manifests.py`)**:
    *   **Action**: Define Pydantic models for operator inputs/outputs.
    *   **Action**: Load JSON -> Validate -> Use. Catch `ValidationError`, log, and fail task (don't crash).

*   **Verification Plan**:
    *   `tests/test_path_safety.py`: Try to init operator with `../../etc/passwd`. Assert error.
    *   `tests/test_manifest_validation.py`: Feed bad JSON to HumanOperator. Assert task fails gracefully.

---

### THRUST 7: Workspace Alignment & Documentation
**Context**: Legacy workspaces use old patterns. They must be modernized to use the v0.2.2 runtime.

*   **7.1 Migrations**:
    *   **Battery Screening**:
        *   Replace `workflow.py` usage with `Campaign.plan()`.
        *   Use `DirectHPCOperator` or `LocalBackend` via config.
    *   **Thin Film Lab**:
        *   Use `ExperimentOperator` for lab interface.
    *   **Catalyst Human**:
        *   Use `HumanOperator` for gate.

*   **7.2 Documentation**:
    *   **Action**: Update `docs/architecture.md` (Run Lifecycle, Job States).
    *   **Action**: Update `docs/operators.md` (New Safety Rules).
    *   **Action**: Create `docs/DevGuides/UserGuide_v0.2.2.md` (How to use CLI, explain, evidence).

*   **Verification Plan**:
    *   Manual verification: Run `matterstack run init/step/explain` on all workspaces.
    *   `tests/test_workspace_e2e.py`: Minimal e2e test for each workspace.

---

## 2. Execution Order

1.  **Safety First (Thrust 6)**: Implement fs_safety and schemas. Refactor existing operators.
2.  **Core Lifecycle (Thrust 2)**: Add StateStore columns, orchestrator logic, CLI commands.
3.  **HPC Semantics (Thrust 1)**: Implement JobState, Slurm mapping, Concurrency caps.
4.  **Evidence (Thrust 3)**: Fix export logic to rely on the new DB state.
5.  **Scheduler (Thrust 4)**: Implement multi-run loop.
6.  **Diagnostics (Thrust 5)**: Implement `run explain`.
7.  **Alignment (Thrust 7)**: Migrate workspaces and update docs.
