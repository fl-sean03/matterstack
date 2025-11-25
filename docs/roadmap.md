# Roadmap

MatterStack is evolving to become the standard operating system for self-driving laboratories.

## Current Status (v0.1)
*   **Core**: Workflow DAGs, Campaign Engine, Task abstraction.
*   **Backends**: Local execution, basic Slurm integration (SSH).
*   **Orchestration**: Sequential execution, basic failure handling (`continue_on_error`).
*   **Integration**: ExternalTask for file-based robot handoff.

## v0.2: The "Robustness" Release
Focus: Hardening the platform for long-running campaigns in unreliable environments.

*   **Database Integration**: Move state from in-memory/JSON to a persistent DB (SQLite/PostgreSQL) to survive crash/restart.
*   **Advanced Slurm**: Support for `srun` within allocations, multi-node MPI jobs, and heterogeneous clusters (CPU+GPU tasks).
*   **Globus Support**: Native integration for moving large datasets between facilities.
*   **Web Dashboard**: A lightweight UI to visualize the DAG status and Campaign progress in real-time.

## v0.3: The "Scale" Release
Focus: Enabling massive, multi-site campaigns.

*   **Cloud Backends**: Native support for AWS Batch and Google Cloud Batch.
*   **Multi-Backend Workflow**: Run cheap tasks locally and expensive tasks on HPC within the *same* workflow.
*   **Kubernetes Backend**: Native execution on K8s clusters (e.g., for inference servers).
*   **Event-Driven Architecture**: Replace polling with an event bus (RabbitMQ/Kafka) for lower latency robot coordination.

## Long Term Vision
*   **Marketplace**: A registry of standard "Scientific Skills" (e.g., "Run VASP", "Train GNN") that can be imported into any campaign.
*   **Federated Learning**: Train models across secure enclaves without sharing raw data.