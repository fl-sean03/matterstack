# Experiment Adapter (External Tasks)

MatterStack is not just for simulation; it orchestrates the physical world too. The `ExternalTask` mechanism allows the workflow to pause and wait for an external agent—such as a robotic synthesis platform, a characterization instrument, or a human operator—to perform an action.

## Concept: File-Based Handoff

Since robots and lab equipment often run on isolated networks or proprietary control software, MatterStack uses a robust file-based interface for coordination. This decouples the HPC orchestration from the lab hardware.

1.  **Request**: MatterStack writes a JSON request file to a shared location (e.g., a mounted network drive or a cloud bucket synced to the robot).
2.  **Action**: The Robot Agent watches for this file, reads the instructions, and executes the experiment.
3.  **Response**: The Robot Agent writes a JSON response file containing the results or status.
4.  **Resume**: MatterStack detects the response and proceeds to the next step in the workflow.

## The `ExternalTask` Class

Located in `matterstack.core.external`, this specialized Task wraps the polling logic.

```python
from matterstack.core.external import ExternalTask

robot_task = ExternalTask(
    task_id="robot_synthesis_01",
    request_path="/mnt/lab_share/incoming/job_123.json",
    response_path="/mnt/lab_share/outgoing/job_123_result.json",
    request_data={
        "procedure": "spin_coat",
        "parameters": {
            "speed_rpm": 3000,
            "duration_s": 60,
            "solution_id": "sample_A"
        }
    },
    poll_interval=10.0,
    time_limit_minutes=120
)
```

## Protocol Specification

### Request File
The request is a standard JSON object containing whatever data the external agent needs.

```json
{
  "procedure": "spin_coat",
  "parameters": { ... }
}
```

### Response File
The external agent **must** provide a JSON response. The only required field is `status` if reporting failure, but typically it includes data.

**Success:**
```json
{
  "status": "success",
  "data": {
    "thickness_nm": 45.2,
    "image_path": "/mnt/data/img_001.tif"
  }
}
```

**Failure:**
```json
{
  "status": "failed",
  "reason": "Vacuum chuck error: sample slipped."
}
```

## Use Cases

1.  **Robot Handoff**: As demonstrated in the **Thin Film Lab** demo, the workflow generates experimental parameters, sends them to a "Robot" (simulated), and waits for characterization data before running analysis.
2.  **Human-in-the-Loop**: As seen in the **Catalyst Discovery** demo, a `GateTask` (a subclass of `ExternalTask`) pauses the workflow until a human expert reviews the proposed candidates and approves them via a text file.