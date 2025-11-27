# DevGuide_v0.2 – MatterStack Run-Centric Architecture (The "Source of Truth")

## [AGENT::SUMMARY]

*   **PROJECT:** MatterStack v0.2 Refactor (Run-Centric Orchestration)
*   **PURPOSE:** Transition from a long-running, in-memory `asyncio` loop to a **stateless, tick-based architecture** backed by a run-local SQLite database. This enables long-running campaigns (days/weeks), resilience to crashes, and support for "disconnected" operators (Manual HPC, Human-in-the-Loop).
*   **CURRENT STATE (v0.1):**
    *   `matterstack/orchestration/api.py`: Contains `run_workflow` which blocks until completion.
    *   `matterstack/campaign/engine.py`: Holds in-memory `CampaignState`.
    *   `matterstack/core/`: Contains Pydantic-compatible dataclasses (`Task`, `Workflow`) but no persistence model.
*   **TARGET STATE (v0.2):**
    *   **Orchestrator:** A short-lived `step_run()` function that loads state, advances work, and exits.
    *   **State:** Persisted in `state.sqlite` within a specific run directory.
    *   **Operators:** A unified `Operator` interface for Local, HPC, and Human tasks.

---

## [AGENT::WORKFLOW_MODEL]

1.  **Analyze**: Understand the mapping between existing `matterstack.*` modules and the new requirements.
2.  **Thrust-Based Execution**: Implement changes in distinct, verifiable "Thrusts" (see Section 5).
3.  **Refactor vs. Rewrite**: Reuse existing Backend logic (`LocalBackend`, `SlurmBackend`) but wrap them in the new stateless Operator interface.
4.  **Verification**: Each thrust has concrete acceptance criteria.

---

## 1. Problem & Context

MatterStack currently runs campaigns as a single process. If this process dies, the campaign state is lost. This is unacceptable for multi-day HPC jobs or human-in-the-loop workflows.

**The Solution:**
*   **Run-Centricity:** Every campaign execution ("Run") has its own directory and `sqlite` database.
*   **Tick-Based Orchestration:** The orchestrator wakes up, checks the DB and external systems (via backends), updates the DB, submits new work, and goes back to sleep.
*   **Unified Operators:** All external actions (running a VASP job, waiting for a human to sign a file) are treated as `ExternalRun` entities managed by an `Operator`.

---

## 2. Core Concepts & Mapping

| Concept | Description | Current Codebase Mapping | v0.2 Target Location |
| :--- | :--- | :--- | :--- |
| **Workspace** | A project directory containing `main.py` and scripts. | `workspaces/*` | Unchanged |
| **Run** | A concrete execution instance. | Implicit (output dirs) | `RunHandle` in `matterstack/core/run.py` |
| **Campaign** | Logic defining the scientific loop. | `matterstack/campaign/engine.py` | `matterstack/core/campaign.py` |
| **Workflow** | DAG of Tasks. | `matterstack/core/workflow.py` | `matterstack/core/workflow.py` |
| **Task** | Unit of work. | `matterstack/core/task.py` | `matterstack/core/workflow.py` |
| **Operator** | Interface for executing tasks. | `ExternalTask` / `ComputeBackend` | `matterstack/core/operators.py` |
| **StateStore** | Transactional DB for the run. | **Missing** | `matterstack/storage/state_store.py` |

---

## 3. Environment & Setup

*   **Language:** Python 3.10+
*   **Dependencies:** `pydantic>=2.0` (for serialization), `sqlalchemy>=2.0` (for SQLite persistence).
*   **Testing:** `pytest`

---

## 4. System Architecture

### 4.1 Persistence Layer (`matterstack/storage/`)
*   **`state_store.py`**: Implements the `StateStore` interface using SQLite (via SQLAlchemy).
*   **Run Root Convention**: `workspaces/<workspace_slug>/runs/<run_id>/`
    *   `state.sqlite`: StateStore file.
    *   `config.json`: Run configuration.
    *   `operators/`: Operator-specific run data.
    *   `evidence/`: Generated evidence bundles.

### 4.2 Domain Layer (`matterstack/core/`)
*   **`run.py`**: `RunHandle`, run ID generation, path resolution.
*   **`operators.py`**: `Operator` ABC, `ExternalRunHandle`, `OperatorResult`.
*   **`evidence.py`**: `EvidenceBundle` dataclass.
*   **`workflow.py`**: Stable `Workflow` and `Task` definitions (no dependency on in-memory state).

### 4.3 Operator Layer (`matterstack/runtime/operators/`)
*   **`hpc.py`**: `DirectHPCOperator` wraps `SlurmBackend`.
*   **`manual_hpc.py`**: `ManualHPCOperator` creates kits for human submission.
*   **`human.py`**: `HumanOperator` handles human-in-the-loop gates.

### 4.4 Orchestration Layer (`matterstack/orchestration/`)
*   **`run_lifecycle.py`**:
    *   `initialize_run(workspace_slug, config) -> RunHandle`
    *   `step_run(run_handle) -> status`

---

## 5. Implementation Thrusts (Detailed)

### THRUST 1: Core Domain Models (Foundational Types)
*   **Goal**: Solidify core entities in `matterstack` package using Pydantic for serialization.
*   **Implementation Steps**:
    1.  Create `matterstack/core/run.py`: Define `RunHandle` (stores workspace slug, run_id, root_path).
    2.  Create `matterstack/core/operators.py`: Define `Operator` (ABC), `ExternalRunHandle` (stores task_id, external_id, status), `OperatorResult`.
    3.  Create `matterstack/core/evidence.py`: Define `EvidenceBundle`.
    4.  Refactor `matterstack/core/workflow.py`: Ensure `Workflow` and `Task` are Pydantic models.
    5.  Refactor `matterstack/core/campaign.py`: Define stateless `Campaign` interface (`plan(state) -> Workflow`, `analyze(result) -> State`).
*   **Validation**:
    *   Create `tests/test_core_domain.py`.
    *   Verify all models can be instantiated and serialized to/from JSON (`model_dump_json`, `model_validate_json`).

### THRUST 2: Run-Local StateStore (Persistence)
*   **Goal**: Implement a SQLite-backed store that persists the Domain Models.
*   **Implementation Steps**:
    1.  Create `matterstack/storage/schema.py`: Define SQLAlchemy ORM models (`RunModel`, `TaskModel`, `ExternalRunModel`) mapping 1:1 to Domain Models.
    2.  Create `matterstack/storage/state_store.py`: Implement `SQLiteStateStore` with methods:
        *   `create_run(run_handle)`
        *   `add_workflow(workflow)`
        *   `update_task_status(task_id, status)`
        *   `register_external_run(external_handle)`
        *   `update_external_run(external_handle)`
*   **Validation**:
    *   Create `tests/test_state_store_sqlite.py`.
    *   Open store -> Create Run -> Close Store.
    *   Reopen Store -> Verify Run exists.
    *   Add Task -> Close -> Reopen -> Verify Task state.

### THRUST 3: Plan & Tick Orchestrator (The Engine)
*   **Goal**: Implement the stateless `initialize_run` and `step_run` logic.
*   **Implementation Steps**:
    1.  Create `matterstack/orchestration/run_lifecycle.py`.
    2.  Implement `initialize_run(workspace_slug, config)`:
        *   Resolve run root path.
        *   Create directory structure (`runs/<id>/`).
        *   Initialize `state.sqlite`.
        *   Instantiate Campaign from config -> `plan()` -> store initial Workflow.
    3.  Implement `step_run(run_handle)`:
        *   Open `StateStore`.
        *   **Poll Phase**: Get active `ExternalRuns` -> call `operator.check_status()` -> update DB.
        *   **Plan Phase**: Check Workflow dependencies -> Find ready Tasks.
        *   **Execute Phase**:
            *   For Local Tasks: Submit to Backend -> update DB.
            *   For Operator Tasks: Call `operator.prepare()` + `operator.submit()` -> create `ExternalRun` record.
        *   **Analyze Phase**: If Workflow complete -> call `campaign.analyze()` -> `campaign.plan()` -> store new Workflow.
*   **Validation**:
    *   Create `tests/test_run_lifecycle_basic.py`.
    *   Simulate a run with 2 sequential local tasks. Call `step_run` repeatedly until completion. Verify DB state at each step.

### THRUST 4: Compute Backends Integration (Infrastructure)
*   **Goal**: Ensure existing backends (`Local`, `Slurm`) work with the new architecture.
*   **Implementation Steps**:
    1.  Audit `matterstack/core/backend.py`. Ensure `ComputeBackend` interface is stateless (uses only `job_id` for polling).
    2.  Create `matterstack/runtime/backends/__init__.py`: Add `create_backend(profile_name)` factory.
*   **Validation**:
    *   Create `tests/test_backends_factory.py`.
    *   Verify factory returns correct backend instance from config profile.

### THRUST 5: Operator Interface & Direct-HPC (HPC Support)
*   **Goal**: Implement the `DirectHPCOperator` using the `SlurmBackend`.
*   **Implementation Steps**:
    1.  Create `matterstack/runtime/operators/hpc.py`.
    2.  Implement `DirectHPCOperator(Operator)`:
        *   `prepare_run`: Create directory `runs/<run_id>/operators/hpc/<id>/`. Write job script.
        *   `submit`: Call `backend.submit()`. Return job ID.
        *   `check_status`: Call `backend.poll(job_id)`.
        *   `collect_results`: Read output files from operator dir.
*   **Validation**:
    *   Create `tests/test_operator_direct_hpc.py` using a Mock Backend.
    *   Verify correct directory creation and status mapping.

### THRUST 6: Manual-HPC Operator (Disconnected Support)
*   **Goal**: Implement `ManualHPCOperator` that creates "Kits" for human handling.
*   **Implementation Steps**:
    1.  Create `matterstack/runtime/operators/manual_hpc.py`.
    2.  Implement `ManualHPCOperator(Operator)`:
        *   `submit`: Do NOT call backend. Set status `WAITING_EXTERNAL`.
        *   `check_status`: Check for existence of `output/` files or `status.json` in operator dir.
*   **Validation**:
    *   Create `tests/test_operator_manual_hpc.py`.
    *   Submit task -> Check status (Waiting).
    *   Manually create output file -> Check status (Completed).

### THRUST 7: Human & Experiment Operators (Interactive)
*   **Goal**: Generic Human-in-the-Loop support.
*   **Implementation Steps**:
    1.  Create `matterstack/runtime/operators/human.py`.
    2.  Implement `HumanOperator`:
        *   `prepare_run`: Write `instructions.md` and `schema.json`.
        *   `check_status`: Check for `response.json`.
*   **Validation**:
    *   Create `tests/test_operator_human.py`. Similar to Manual HPC test.

### THRUST 8: EvidenceBundle Construction (Reporting)
*   **Goal**: Generate final artifacts.
*   **Implementation Steps**:
    1.  Implement `matterstack/storage/export.py`:
        *   `build_evidence(run_handle)`: Query StateStore for all tasks/results.
        *   `export_evidence(bundle)`: Write `evidence.json` and `report.md`.
*   **Validation**:
    *   Create `tests/test_evidence_export.py`. Generate bundle from a completed test run.

### THRUST 9: Minimal Interface Layer (CLI)
*   **Goal**: User-facing commands.
*   **Implementation Steps**:
    1.  Update `matterstack/cli/__init__.py`:
        *   `init(workspace, config)` -> Calls `initialize_run`.
        *   `step(run_id)` -> Calls `step_run`.
        *   `status(run_id)` -> Prints StateStore summary.
*   **Validation**:
    *   Create `tests/test_cli_run_commands.py`.

### THRUST 10: Workspace Adaptation (Migration)
*   **Goal**: Migrate `coatings_active_learning` workspace.
*   **Implementation Steps**:
    1.  Refactor `workspaces/coatings_active_learning/main.py`:
        *   Remove `run_workflow` call.
        *   Expose `get_campaign()` and `get_operators()` functions.
    2.  Update README with new CLI instructions.
*   **Acceptance Criteria**:
    *   User can run the full active learning loop using only `matterstack step` commands.
    *   State persists across process restarts.

---

## 6. Next Steps for You

Proceed to create the **Comprehensive Todo List** based on these detailed thrusts, then begin with **Thrust 1**.
