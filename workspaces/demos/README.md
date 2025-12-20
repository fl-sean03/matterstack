# MatterStack Demo Workspaces

This directory contains demonstration workspaces showcasing MatterStack's key capabilities for orchestrating complex materials science workflows.

## Quick Start

Each demo can be run directly with Python:

```bash
cd workspaces/demos/<demo_name>
python main.py
```

Or using the MatterStack CLI with nested workspace paths:

```bash
# Initialize a run
matterstack init demos/battery_screening

# Execute steps until completion
matterstack loop <run_id>
```

## Demo Collection

| Demo | Description | Key Features |
|------|-------------|--------------|
| [battery_screening](./battery_screening/) | High-throughput cathode screening | Soft failure handling, parallel fan-out/fan-in |
| [thin_film_lab](./thin_film_lab/) | Self-driving lab with robot handoff | ExperimentOperator, hardware-in-the-loop |
| [catalyst_human_in_loop](./catalyst_human_in_loop/) | Expert-gated catalyst discovery | HumanOperator, approval gates |
| [coatings_active_learning](./coatings_active_learning/) | Autonomous coating optimization | Active learning, Surrogate AI |

## Feature Matrix

| Demo | Primary Feature | v0.2.6 Canonical Keys | v0.2.7 Operator Wiring | Evidence Export |
|------|-----------------|----------------------|------------------------|-----------------|
| battery_screening | Soft failure + parallel | ✅ `hpc.default` | ✅ `operators.yaml` | ❌ |
| thin_film_lab | ExperimentOperator | ✅ `experiment.default` | ✅ `operators.yaml` | ✅ (primary) |
| catalyst_human_in_loop | Human gates | ✅ `human.default` | ✅ `operators.yaml` | ❌ |
| coatings_active_learning | Active learning | ✅ `hpc.default` | ✅ `operators.yaml` | ❌ |

## MatterStack Features Demonstrated

### v0.2.7: Operator Wiring Auto-Discovery

Each demo includes an `operators.yaml` file that MatterStack automatically discovers. No `--operators-config` flag needed!

```yaml
# Example from battery_screening/operators.yaml
operators:
  local.default:
    kind: local
    backend:
      type: local

  hpc.default:
    kind: hpc
    backend:
      type: local  # Local backend for demo (simulates HPC behavior)
```

### v0.2.6: Canonical Operator Keys

Demos use structured operator references for portable, site-agnostic workflows:

- `hpc.default` - HPC compute tasks (simulated locally in demos)
- `human.default` - Human approval gates
- `experiment.default` - Lab equipment handoffs

### Campaign Architecture

All demos implement the `Campaign` interface with:

- `plan(state) -> Workflow`: Generate tasks based on current state
- `analyze(state, results) -> state`: Process results and update state

## Evidence Export Example

After running the `thin_film_lab` demo, export a reproducibility evidence bundle:

```bash
# Run the demo first
cd workspaces/demos/thin_film_lab
python main.py

# Export evidence for the completed run
matterstack export-evidence <run_id>
```

This creates an `evidence/` directory containing:

| File | Description |
|------|-------------|
| `bundle.json` | Complete run metadata including operator wiring provenance |
| `report.md` | Human-readable summary of the run |
| `operators_snapshot/` | Copy of operator configuration used during execution |

See the [thin_film_lab README](./thin_film_lab/README.md#7-evidence-export) for detailed examples.

## Directory Structure

```
workspaces/demos/
├── README.md                          # This file
├── battery_screening/
│   ├── main.py                        # Campaign definition
│   ├── operators.yaml                 # v0.2.7 operator wiring
│   ├── README.md                      # Demo documentation
│   └── scripts/                       # Task scripts
├── thin_film_lab/
│   ├── main.py
│   ├── operators.yaml
│   ├── README.md
│   ├── scripts/
│   └── data_exchange/                 # Robot handoff directory
├── catalyst_human_in_loop/
│   ├── main.py
│   ├── operators.yaml
│   ├── README.md
│   └── scripts/
└── coatings_active_learning/
    ├── main.py
    ├── operators.yaml
    ├── README.md
    └── scripts/
```

## Related Documentation

- [Campaigns and Workspaces](../../docs/campaigns_and_workspaces.md) - Core concepts
- [Operators Guide](../../docs/operators.md) - Operator configuration
- [Evidence Bundles](../../docs/evidence_bundles.md) - Reproducibility exports
- [Execution Backends](../../docs/execution_backends.md) - Local vs HPC execution
