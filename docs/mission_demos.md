# Mission Demos

MatterStack includes four reference workspaces that demonstrate different scientific workflows and orchestration capabilities.

## 1. Coatings (Active Learning)
**Directory**: `workspaces/coatings_active_learning`

*   **Scientific Goal**: Optimize a protective coating for corrosion resistance (low dissolution rate) and durability (low friction coefficient).
*   **Orchestration Pattern**: **Iterative Active Learning Loop**.
*   **Narrative**: The campaign starts with a random seed of candidate materials. In each iteration, it:
    1.  Trains a Gaussian Process Regressor on existing data.
    2.  Selects new candidates that maximize the Expected Improvement (acquisition function).
    3.  Launches parallel simulations (`sim_dissolution.py`, `sim_friction.py`) for these candidates.
    4.  Updates the model and repeats until convergence.

## 2. Battery Screening (High Throughput & Resilience)
**Directory**: `workspaces/battery_screening`

*   **Scientific Goal**: Screen thousands of doped battery materials to find candidates with optimal voltage and stability.
*   **Orchestration Pattern**: **High-Throughput Fan-Out with Soft Failures**.
*   **Narrative**: The workflow generates 100 candidate structures (e.g., Li-oxide doped with varying elements). It submits 100 parallel `calc_properties.py` tasks.
    *   **Resilience**: Some tasks are designed to fail randomly (simulating convergence errors).
    *   **Aggregation**: A final `train_model.py` task runs even if some upstream tasks fail (`allow_dependency_failure=True`), aggregating the partial success data into a final report.

## 3. Thin Film Lab (Robot Integration)
**Directory**: `workspaces/thin_film_lab`

*   **Scientific Goal**: Synthesize and characterize thin film materials using a robotic platform.
*   **Orchestration Pattern**: **External Hardware Handoff**.
*   **Narrative**:
    1.  **AI Agent**: Proposes a set of synthesis parameters (spin speed, annealing temp).
    2.  **Handoff**: An `ExternalTask` writes these parameters to `experiment_request.json` and pauses.
    3.  **Robot (Simulated)**: Reads the request, performs the "experiment," and writes `experiment_results.json`.
    4.  **Loop**: The workflow resumes, analyzes the characterization data, and proposes the next experiment.

## 4. Catalyst Discovery (Human-in-the-Loop)
**Directory**: `workspaces/catalyst_human_in_loop`

*   **Scientific Goal**: Discover novel catalysts for CO2 reduction.
*   **Orchestration Pattern**: **Human Gating**.
*   **Narrative**: AI models can propose chemically valid but practically difficult-to-synthesize molecules. To prevent wasted compute/lab time:
    1.  **Proposal**: The system generates a list of candidates.
    2.  **Gate**: A `GateTask` pauses execution and sends a notification (e.g., "Please review candidates.csv").
    3.  **Review**: A human expert reviews the list and creates an `approved.txt` file (or edits the list).
    4.  **Compute**: The workflow proceeds to run expensive DFT calculations (`calc_adsorption.py`) only on the approved candidates.