# Campaigns and Workspaces

This guide details how scientific missions are organized (Workspaces) and executed (Campaigns) in MatterStack.

## Workspaces

A Workspace is the fundamental unit of organization. It maps a scientific problem to a directory structure that contains code, configuration, and data.

### Directory Structure

A standard MatterStack workspace follows this convention:

```text
my_mission/
├── main.py              # The Campaign definition and entry point
├── README.md            # Documentation specific to this mission
├── config/              # (Optional) Configuration files
├── data/                # Static input data (CSVs, structures, etc.)
├── scripts/             # Python scripts executed by Tasks
│   ├── simulate.py
│   └── analyze.py
└── results/             # (Generated) Execution artifacts
    ├── iteration_01/
    ├── iteration_02/
    └── summary.json
```

### Key Components

*   **`main.py`**: Instantiates the `Campaign` class (or subclass), defines the backend connection, and triggers `campaign.run()`.
*   **`scripts/`**: Contains the actual scientific code. MatterStack `Tasks` typically execute commands like `python3 scripts/simulate.py`. This decouples the orchestration logic from the scientific logic.
*   **`results/`**: The "Evidence Bundle." MatterStack automatically structures output here to ensure reproducibility.

## The Campaign Engine

The `Campaign` class (`matterstack.campaign.engine.Campaign`) abstracts the iterative scientific method.

### The Loop

The engine executes a loop consisting of four phases:

1.  **Plan (`plan()`)**
    *   **Input**: Current campaign state (history of previous iterations).
    *   **Logic**: Active Learning policies, Optimization algorithms, or simple heuristics.
    *   **Output**: A `Workflow` object containing the `Tasks` to run in this iteration. If `None` is returned, the campaign terminates.

2.  **Execute**
    *   **Action**: The engine submits the `Workflow` to the configured `ComputeBackend`.
    *   **Blocking**: The engine waits for the workflow to complete (or fail).
    *   **Artifacts**: stdout/stderr logs and output files are captured in the `results/` directory.

3.  **Analyze (`analyze()`)**
    *   **Input**: `WorkflowResult` containing the status and output of every task.
    *   **Action**: Parses results (e.g., reads a JSON file produced by a task), updates the internal model (e.g., retrains a Gaussian Process), and saves the new state.

4.  **Stop Check (`should_stop()`)**
    *   **Logic**: Checks termination criteria (e.g., target accuracy reached, budget exhausted).
    *   **Output**: Boolean.

### Example: Active Learning Loop

```python
class ActiveLearningCampaign(Campaign):
    def plan(self):
        # 1. Select candidates based on current model uncertainty
        candidates = self.model.select_next_batch()
        
        # 2. Create tasks for these candidates
        workflow = Workflow()
        for cand in candidates:
            workflow.add_task(Task(..., command=f"simulate {cand}"))
            
        return workflow

    def analyze(self, result):
        # 1. Parse simulation outputs
        new_data = self.parse_results(result)
        
        # 2. Update model
        self.model.train(new_data)
```

By subclassing `Campaign`, you can implement complex autonomous loops ranging from high-throughput screening to self-driving laboratories.