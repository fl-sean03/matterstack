# DevGuide_v0.2.1 – Hardening & Resilience

## [AGENT::SUMMARY]

*   **PROJECT:** MatterStack v0.2.1 (Hardening)
*   **GOAL:** Fortify the v0.2 architecture against real-world edge cases, including crashes, concurrency, malformed inputs, and scale.
*   **INPUT:** Feedback from Senior Dev review of v0.2.
*   **FOCUS:** Correctness, Idempotency, and Observability.

---

## [AGENT::WORKFLOW_MODEL]

1.  **Analyze**: Review existing v0.2 implementations (`StateStore`, `RunLifecycle`, `Operators`) against safety requirements.
2.  **Test First**: Implement "evil" tests (crash simulation, double-submit) *before* or alongside fixes.
3.  **Lock It Down**: Enforce concurrency policies (e.g., file locks for SQLite).

---

## 1. Implementation Thrusts

### THRUST 1: Architecture Hardening & Safety
**Goal:** Ensure the system doesn't corrupt state or fail silently.

*   **1.1 Schema Versioning**:
    *   Add a `schema_info` table to `state.sqlite` with a `version` column.
    *   Update `SQLiteStateStore` to check version on connect. If mismatch/missing, handle gracefully (init or error).
    *   **Validation**: Test initializing a new DB (gets version 1) vs opening an old one.

*   **1.2 Concurrency Control**:
    *   Implement a simple **File Lock** (`run.lock`) alongside `state.sqlite`.
    *   `initialize_run` and `step_run` must acquire this lock exclusively. If locked, fail fast ("Another process is running").
    *   **Validation**: `tests/test_concurrency.py` spawns two processes trying to step the same run.

*   **1.3 Error Handling Policy**:
    *   Explicitly define behavior for Operator failures: Task state → `FAILED`.
    *   Explicitly define behavior for Orchestrator crashes: Task state stays `SUBMITTED` (or whatever was committed).
    *   **Validation**: Document in `docs/architecture.md`.

### THRUST 2: Advanced Testing (The "Evil" Suite)
**Goal:** Prove idempotency and resilience.

*   **2.1 Idempotency Tests (`tests/test_run_lifecycle_idempotent.py`)**:
    *   Call `step_run` 10x with no backend changes. Assert exactly 1 submission.
    *   Simulate a crash (exception) *after* submission but *before* DB update (if possible via mocking), then retry.

*   **2.2 Bad Input Tests (`tests/test_operator_failures.py`)**:
    *   Manual HPC: Output directory exists but empty. Assert `check_status` -> `FAILED` or retry logic.
    *   Human: `response.json` is malformed. Assert task doesn't hang forever.

*   **2.3 Scale/Stress Test (`tests/test_run_lifecycle_scale.py`)**:
    *   Generate a DAG with 100+ dummy tasks.
    *   Run it to completion. Ensure performance is acceptable (DB doesn't lock up).

### THRUST 3: Observability
**Goal:** Make debugging easier.

*   **3.1 Structured Logging**:
    *   Ensure `step_run` logs a summary: "Tick: X tasks ready, Y submitted, Z completed".
    *   Ensure `StateStore` logs DB path and version on startup.

### THRUST 4: Documentation & Cleanup
**Goal:** Leave the codebase ready for users.

*   **4.1 Documentation**:
    *   Update `docs/architecture.md` with Run Root layout and Concurrency/Locking model.
    *   Create `docs/operators.md` (Cookbook).

*   **4.2 Migration (Follow-up)**:
    *   (Optional for this sprint, but good to plan) Migrate `battery_screening` workspace.

---

## 2. Execution Plan

1.  **Locking & Schema**: Implement `Thrust 1` changes in `matterstack/storage/state_store.py` and `run_lifecycle.py`.
2.  **Tests**: Implement `Thrust 2` test files. Run them. If they fail, fix the core logic.
3.  **Docs**: Write `Thrust 4` documentation.