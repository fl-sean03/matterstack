# MatterStack: Materials Discovery Platform

MatterStack is a workflow orchestration platform designed for materials science research. It connects computational resources, simulation engines, and decision-making logic.

By automating campaigns—from high-throughput screening to active learning loops—MatterStack allows researchers to manage complex scientific workflows more efficiently.

## Key Features

*   **Workflow Orchestration**: Manage dependencies between simulation, analysis, and decision tasks.
*   **HPC Support**: Dispatch and monitor jobs across SLURM, PBS, and local environments.
*   **Human-in-the-Loop**: Pause workflows for expert feedback or decisions.
*   **Active Learning**: Components for connecting prediction models with experimental or simulation loops.

## Real-World Demos

Explore our "Mission-aligned" demonstration projects, showcasing MatterStack in diverse scientific domains:

### 1. [Battery Electrolyte Screening](workspaces/battery_screening)
A high-throughput screening campaign for identifying promising solid-state electrolyte candidates. Demonstrates parallel execution and result aggregation.

### 2. [Protective Coatings Optimization](workspaces/coatings_active_learning)
An active learning workflow that optimizes friction coefficients for protective coatings. Showcases AI-driven candidate selection and iterative refinement.

### 3. [Thin Film Lab Automation](workspaces/thin_film_lab)
Simulation of a self-driving laboratory environment for thin film synthesis. Illustrates the integration of experimental constraints and automated parameter search.

### 4. [Catalyst Discovery (Human-in-the-Loop)](workspaces/catalyst_human_in_loop)
A catalyst design workflow that pauses for expert validation before proceeding to expensive compute steps. Demonstrates seamless human-AI collaboration.

## Quick Start

### Installation

```bash
git clone https://github.com/matterstack/matterstack.git
cd matterstack
pip install -e .
```

### Running a Demo

Navigate to any workspace and run the entry point:

```bash
cd workspaces/battery_screening
python main.py
```

## Project Structure

*   `matterstack/`: Core framework source code.
*   `workspaces/`: Self-contained project environments (demos and campaigns).
*   `tests/`: Unit and integration tests.
*   `docs/`: Documentation and guides.

---
*Accelerating materials science research.*