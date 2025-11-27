# Thin-Film Discovery with Hardware-in-the-Loop

**Orchestrating Asynchronous Handshakes Between Simulation and Robotic Synthesis**

> **Disclaimer:** This project uses entirely simulated data. The "robotic synthesis" is performed by a mock daemon thread embedded in `main.py` that introduces random noise to the predicted values. No physical hardware is controlled.

## 1. Abstract
This workspace demonstrates a "Self-Driving Lab" architecture where digital simulations and physical experiments are coupled in a closed loop. The workflow manages the complex handoff between a computational prediction task and an external robotic agent. It simulates the "Sim-to-Real" gap—calculating the discrepancy between theoretical predictions and experimental realities to calibrate future models.

## 2. Scientific Background
In autonomous materials laboratories, the bottleneck is often the coordination between high-speed computational screening and slower physical synthesis/characterization.
- **The Challenge**: A simulation suggests a thin-film recipe (temperature, pressure, precursor ratio). This must be transmitted to a robotic coater, which physically deposits the film and measures its properties (e.g., bandgap or conductivity).
- **Sim-to-Real Gap**: Theoretical models are rarely perfect. The system must "Reconcile" the data, comparing predicted vs. actual values to detect model drift or calibration errors.

## 3. Computational Challenge
Standard workflow engines assume all tasks are computational processes they control directly (e.g., a Python script).
- **Asynchrony**: Robotic tasks take minutes to hours and happen "outside" the compute cluster.
- **Protocol**: The workflow cannot just "run" the robot; it must submit a request and wait for a signal.
- **Reliability**: The system must handle the waiting state without consuming active compute resources (polling or event-based).

## 4. MatterStack Solution
This project uses the **`ExperimentOperator`** capability to bridge the digital-physical divide.
- **Operator Pattern**: The `ExperimentOperator` manages the interaction with external lab equipment.
- **Structured Handoff**: The workflow creates a dedicated directory `runs/<run_id>/operators/experiment/<uuid>/` containing `experiment_request.json`.
- **Asynchronous Waiting**: The `robot_task` enters a `WAITING_EXTERNAL` state. A background daemon (simulating the robot) watches this directory, performs the "experiment", and writes `experiment_result.json`.
- **Decoupling**: The simulation logic is completely decoupled from the robot's control software, interacting only through standardized data contracts.

## 5. Workflow Architecture
The `main.py` script orchestrates a three-stage pipeline:

1.  **Simulation (`sim_task`)**:
    - Runs `sim_predict.py` to generate a synthetic prediction for a thin-film material.
    - Output: `sim_results.json` (Predicted Property: $P_{sim}$).
2.  **Robotic Handoff (`robot_task`)**:
    - Reads the simulation result.
    - Generates an `experiment_request.json`.
    - **Waits**: The workflow yields while a separate daemon (`mock_robot.py`) processes the request.
    - The `mock_robot.py` (simulating the hardware) reads the request, waits 2 seconds, and writes `experiment_results.json` with a random noise factor added.
3.  **Data Reconciliation (`reconcile_task`)**:
    - Reads both `sim_results.json` and `robot_data.json` (which the daemon writes to the run root for easy access).
    - Calculates the error $|P_{sim} - P_{exp}|$.
    - Generates a final report on the model's accuracy.

## 6. Execution & Results

### Running the Lab Loop
```bash
python3 main.py
```
*Note: The script automatically launches the robot simulator thread in the background to service the request.*

### Expected Output
```text
Initializing Run...
[Robot Daemon] Watching workspaces/thin_film_lab/runs/.../operators/experiment...
...
[Robot Daemon] Processing request in: ...
[Robot Daemon] Wrote specific output to .../robot_data.json
[Robot Daemon] Completed experiment. Result written.
...
Workflow Finished!
Status: COMPLETED
```

The final `reconcile_data.py` output will show the calculated "Sim-to-Real" error, simulating the feedback loop used to calibrate autonomous labs.