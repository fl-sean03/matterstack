# MatterStack v0.2.2 User Guide

Welcome to MatterStack v0.2.2. This release introduces a robust run lifecycle, new CLI commands for control and diagnostics, and standardized workspace structures.

## 1. Getting Started

### Initialize a Run
To start a new scientific campaign, use the `init` command. This sets up the run directory, initializes the database, and generates the first workflow plan.

```bash
matterstack run init battery_screening
```
*Output: Run ID (e.g., `20251126_120000_abc123`)*

### Run the Loop
To execute the campaign, use the `loop` command. In v0.2.2, this acts as a scheduler that can manage multiple runs or a specific run.

```bash
# Run a specific campaign until completion
matterstack run loop <run_id>

# Run as a daemon (services all active runs)
matterstack run loop
```

## 2. Monitoring & Diagnostics

### Check Status
View the high-level status of a run and its tasks.

```bash
matterstack run status <run_id>
```

### Explain Stalls (`run explain`)
If a run seems stuck, use `explain` to identify blocking tasks. It analyzes the "frontier" of the workflow and reports what each blocking task is waiting for (e.g., waiting for human input, waiting for HPC job).

```bash
matterstack run explain <run_id>
```

*Example Output:*
```
Run: 20251126_...
Status: RUNNING
Found 1 blocking item(s):
[Task human_approval] - WAITING_EXTERNAL
  Reason: Waiting for response.json
  Hint: Check runs/.../operators/human/uuid/instructions.md
```

## 3. Controlling Execution

### Pause a Run
Safely suspend execution. No new tasks will be submitted, but currently running tasks will finish.

```bash
matterstack run pause <run_id>
```

### Resume a Run
Continue execution from where it left off.

```bash
matterstack run resume <run_id>
```

### Cancel a Run
Permanently stop a run. Pending tasks are cancelled.

```bash
matterstack run cancel <run_id>
```

## 4. Evidence & Results

### Export Evidence
Generate a comprehensive evidence bundle (JSON metadata + artifacts) for the run. This is rebuilt from the source of truth (SQLite DB) to ensure consistency.

```bash
matterstack run export-evidence <run_id>
```
The bundle is saved to `<run_root>/evidence/`.

## 5. Workspaces

### Battery Screening
*   **Goal**: Screen candidate materials for battery performance.
*   **Structure**: Generates candidates -> Parallel simulation -> Aggregation.
*   **Config**: execution_mode (Local/HPC).

### Catalyst Human-in-the-Loop
*   **Goal**: Propose catalyst candidates and wait for human approval before expensive calculations.
*   **Interaction**: Check `explain` output for the "Human Gate" location. Review `manifest.json` and create `response.json` to approve.

### Thin Film Lab
*   **Goal**: Simulate thin film growth -> Handoff to Robot -> Reconcile data.
*   **Interaction**: Uses `ExperimentOperator` to interface with lab equipment (simulated by a daemon).