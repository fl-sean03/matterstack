# MatterStack: Materials Discovery Platform

[![CI](https://github.com/fl-sean03/matterstack/actions/workflows/ci.yml/badge.svg)](https://github.com/fl-sean03/matterstack/actions/workflows/ci.yml)

MatterStack is a workflow orchestration platform designed for materials science research. It connects computational resources, simulation engines, and decision-making logic.

By automating campaigns—from high-throughput screening to active learning loops—MatterStack allows researchers to manage complex scientific workflows more efficiently.

## 🚀 5-Minute Quickstart

### 1. Installation

```bash
git clone https://github.com/matterstack/matterstack.git
cd matterstack
pip install -e .
```

### 2. Verify Installation

Run the self-test command to verify that your environment is correctly configured:

```bash
matterstack self-test
```
_If the command is not found, try `python3 -m matterstack.cli.main self-test`._

### 3. Run a Campaign

MatterStack uses **workspaces** to organize campaigns. Let's run the **Battery Electrolyte Screening** demo:

**Initialize the Run:**
```bash
matterstack init battery_screening
# Output: Run initialized: 20251126_...
```

**Execute the Campaign Loop:**
```bash
# Replace [RUN_ID] with the ID from the previous step
matterstack loop [RUN_ID]
```

**Export Results:**
```bash
matterstack export-evidence [RUN_ID]
# Generates a report in workspaces/battery_screening/runs/[RUN_ID]/evidence/report.md
```

---

## Key Features

*   **Workflow Orchestration**: Manage dependencies between simulation, analysis, and decision tasks.
*   **HPC Support**: Dispatch and monitor jobs across SLURM, PBS, and local environments.
*   **Human-in-the-Loop**: Pause workflows for expert feedback or decisions.
*   **Active Learning**: Components for connecting prediction models with experimental or simulation loops.

## Real-World Demos

Explore our "Mission-aligned" demonstration projects:

### 1. [Battery Electrolyte Screening](workspaces/battery_screening)
A high-throughput screening campaign for identifying promising solid-state electrolyte candidates. Demonstrates parallel execution and result aggregation.

### 2. [Protective Coatings Optimization](workspaces/coatings_active_learning)
An active learning workflow that optimizes friction coefficients for protective coatings. Showcases AI-driven candidate selection and iterative refinement.

### 3. [Thin Film Lab Automation](workspaces/thin_film_lab)
Simulation of a self-driving laboratory environment for thin film synthesis. Illustrates the integration of experimental constraints and automated parameter search.

### 4. [Catalyst Discovery (Human-in-the-Loop)](workspaces/catalyst_human_in_loop)
A catalyst design workflow that pauses for expert validation before proceeding to expensive compute steps. Demonstrates seamless human-AI collaboration.

## Development

### Quick Start

```bash
# Clone and install
git clone https://github.com/fl-sean03/matterstack.git
cd matterstack
uv sync

# Run tests
uv run pytest tests/ -v

# Run linting
uv run ruff check matterstack/

# Run type checking
uv run mypy matterstack/ --ignore-missing-imports
```

### Pre-commit Hooks

This project uses pre-commit for code quality:

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

### Quality Gates

All PRs must pass:
- Tests: `uv run pytest tests/`
- LOC limit: `./scripts/check_max_lines.sh` (500 lines max per file)
- Self-test: `uv run matterstack self-test`
- Lint (warning): `uv run ruff check matterstack/`

## Project Structure

*   `matterstack/`: Core framework source code.
*   `workspaces/`: Self-contained project environments (demos and campaigns).
*   `tests/`: Unit and integration tests.
*   `docs/`: Documentation and guides.

---
*Accelerating materials science research.*