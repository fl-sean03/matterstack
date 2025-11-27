# Critical-Materials Battery Screening

**High-Throughput Robust Screening for Post-Lithium Ion Chemistries**

> **Disclaimer:** This project uses entirely simulated data. The "properties" calculated (formation energy, voltage) are generated using random distributions and simple heuristics for demonstration purposes. They do not represent real quantum mechanical calculations.

## 1. Abstract
This project simulates a large-scale high-throughput (HT) screening campaign to identify viable dopants for next-generation battery cathodes. The workflow processes 100+ candidates in parallel, calculating key performance metrics like formation energy ($E_{form}$) and voltage. Crucially, the system demonstrates the "Robust Aggregation" pattern: the ability to complete the campaign and generate a valid model card even when a significant fraction of individual tasks fail stochastically.

## 2. Scientific Background
Reducing reliance on Cobalt and Nickel in Li-ion batteries is a critical geopolitical and sustainability goal. Doping novel cathode structures with abundant elements (Al, Si, Ti, Mg) can stabilize the lattice, but the parameter space of dopant concentration and site preference is immense.
- **Goal**: Screen hundreds of doped structures to filter out those with high formation energy (unstable) or low voltage.
- **Method**: First-principles calculations (simulated here) are run for every candidate.

## 3. Computational Challenge
In massive HT campaigns running on High-Performance Computing (HPC) resources, a 100% success rate is statistically impossible due to:
- **Numerical Instability**: Some geometries fail to converge (SCF divergence).
- **System Issues**: Network timeouts, node failures, or preemptions.
- **Workflow Fragility**: A standard workflow engine typically aborts the entire DAG if a single dependency fails, wasting thousands of CPU hours.

## 4. MatterStack Solution
This project leverages **Soft Failure Handling** features in MatterStack to build resilient pipelines.
- **`continue_on_error=True`**: This execution flag instructs the Orchestrator to proceed with independent tasks even if their siblings fail.
- **Robust Aggregation**: The final `Aggregator` task is designed to be "tolerant"—it scans the workspace for *successful* outputs rather than assuming all inputs exist.
- **Partial Success**: The campaign is considered successful if enough data is gathered to train a surrogate model, even if 10-20% of individual calculations crashed.

## 5. Workflow Architecture
The pipeline defined in `main.py` follows a "Fan-Out / Fan-In" pattern:

1.  **Candidate Generation**: The script generates 10 candidate structures (scaled down for the demo) with random dopants (Al, Si, Ti, Mg) and concentrations.
2.  **Parallel Execution (Fan-Out)**:
    - 10 parallel `calc_properties.py` tasks are launched.
    - **Simulated Failure**: To mimic real-world HPC instability, each task has a probability of raising a purely stochastic exception (exit code 1).
3.  **Aggregation (Fan-In)**:
    - A single `train_model.py` task runs after the fan-out phase.
    - It iterates through the results directory, parsing only the valid `results.json` files.
    - It trains a linear regression model on the survivors and generates a `model_card.md`.

## 6. Execution & Results

### Running the Screening
```bash
python3 main.py
```

### Expected Behavior
You will observe a stream of task executions. Approximately 10% of the candidate tasks will report `FAILED` (simulated stochastic failure).

```text
...
Task cand_006 status changed: ExternalRunStatus.SUBMITTED -> ExternalRunStatus.FAILED
External Run cand_006 transitioned to ExternalRunStatus.FAILED
...
```

Despite the errors, the Aggregator will successfully run:
```text
Aggregator completed successfully.
Check model_card.md in the aggregator directory.
--- Model Card ---
Model trained on 7/10 successful candidates.
...
```
This demonstrates that the scientific insight (the trained model) was preserved despite the imperfections of the underlying infrastructure.