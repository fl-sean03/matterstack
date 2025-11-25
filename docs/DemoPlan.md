# MatterStack Real-World Materials Science Demos Implementation Plan

This document outlines the roadmap for implementing four comprehensive "Mini-Projects" that represent realistic, complex materials science campaigns. These projects require significant enhancements to the MatterStack platform to support active learning, massive batch processing, experiment-in-the-loop, and human-gated workflows.

## 1. Extreme-Environment Coatings Autonomy (`workspaces/coatings_active_learning`)
**Objective**: Closed-loop discovery of tribocorrosion-resistant coatings using Active Learning.
**Primary Mode**: 2-cycle iterative loop (Generation -> Sim -> Learn -> Generation).

### Platform Roadmap:
*   **Iterative Workflows**: Support for cyclic dependencies or "Campaign" level orchestration that can spawn new workflows based on previous results.
*   **Surrogate Interface**: A standard interface (`matterstack.ai.Surrogate`) for models (GPR/RF) that can train on `TaskResult`s.
*   **Evidence Bundle**: Standardized output format for task metadata and results.

### Implementation Steps:
1.  **Platform**: Implement `matterstack.campaign.IterativeCampaign` and `matterstack.ai.surrogate`.
2.  **Workspace**: `workspaces/coatings_active_learning/`
3.  **Mock Codes**:
    *   `scripts/sim_friction.py`: Returns noisy friction score based on inputs.
    *   `scripts/sim_dissolution.py`: Returns noisy dissolution score.
4.  **Orchestration (`main.py`)**:
    *   Seed Generation (10 candidates).
    *   Cycle 0: Run Sims -> Train GPR -> Select next 5.
    *   Cycle 1: Run Sims -> Final Rank.
5.  **Artifacts**: `surrogate.pkl`, `selection_reasoning.md`.

## 2. Critical-Materials Battery Screening (`workspaces/battery_screening`)
**Objective**: High-throughput screening of 100+ battery candidates with automated failure handling and model factory.
**Primary Mode**: Massive batch → Auto-Surrogate.

### Platform Roadmap:
*   **Batch Job Support**: Efficiently handle 100+ tasks (possibly mapping to Slurm Job Arrays in the future, but individual tasks for now).
*   **Failure Policies**: "Continue on partial failure" logic in `run_workflow`.
*   **Model Card**: Automated generation of model performance metrics.

### Implementation Steps:
1.  **Platform**: Enhance `Workflow` to support "Soft Failure" (don't abort whole workflow if one leaf fails).
2.  **Workspace**: `workspaces/battery_screening/`
3.  **Mock Codes**:
    *   `scripts/calc_properties.py`: Returns E_form, Voltage, etc. Fails randomly 10% of time.
4.  **Orchestration (`main.py`)**:
    *   Generate 100 candidates.
    *   Submit all.
    *   Aggregator: Collect successful results, train model, generate `model_card.md`.

## 3. Thin-Film Discovery with Experiment Handoff (`workspaces/thin_film_lab`)
**Objective**: Sim -> Experiment -> Learn loop.
**Primary Mode**: Asynchronous handoff to external agent (Mock Robot).

### Platform Roadmap:
*   **External Tasks**: A `Task` type that writes a request file and polls for a response file (simulating a robot or human operator).
*   **Data Reconciliation**: Tools to merge Sim and Exp dataframes.

### Implementation Steps:
1.  **Platform**: Implement `PollingTask` or `ExternalTask` in `matterstack.core.workflow`.
2.  **Workspace**: `workspaces/thin_film_lab/`
3.  **Mock Codes**:
    *   `scripts/mock_robot.py`: Watches a folder, picks up `experiment_request.json`, waits, writes `experiment_results.json`.
4.  **Orchestration (`main.py`)**:
    *   Sim Batch -> Rank -> Write Exp Request.
    *   Wait for Robot (ExternalTask).
    *   Ingest -> Update Model.

## 4. Catalyst Discovery with Human Gate (`workspaces/catalyst_human_in_loop`)
**Objective**: Constraint-driven discovery with a human approval step.
**Primary Mode**: Stop-and-wait for human intent/approval.

### Platform Roadmap:
*   **Gate Task**: A task that halts execution until a CLI command is run or a file is created.
*   **Intent Parser**: Simple mapping of `intent.md` to constraints.

### Implementation Steps:
1.  **Platform**: Implement `GateTask` (blocks thread/process until condition met).
2.  **Workspace**: `workspaces/catalyst_human_in_loop/`
3.  **Mock Codes**:
    *   `scripts/check_constraints.py`.
4.  **Orchestration (`main.py`)**:
    *   Parse Intent -> Propose Candidates -> **Gate** (Wait for user approval) -> Run Sims -> Rank.

---

## Execution Order
1.  **Platform Upgrade**: Implement `ExternalTask`, `GateTask`, and `SoftFailure` support.
2.  **Mini 1 (Coatings)**: Focus on Iterative Loop.
3.  **Mini 2 (Batteries)**: Focus on Scale and Robustness.
4.  **Mini 3 (Lab Handoff)**: Focus on External Integration.
5.  **Mini 4 (Human Gate)**: Focus on Interactivity.