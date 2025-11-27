# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2025-11-26
### Added
- **Smoke Test**: New `matterstack self-test` command for quick operational verification.
- **Quickstart Guide**: Updated `README.md` with a 5-minute onboarding section.

### Changed
- **API Visibility**: Explicitly exported public classes (`Campaign`, `Task`, `RunHandle`, `Operator`) in `matterstack/__init__.py`.
- **Versioning**: Aligned `pyproject.toml` and package version.

## [0.2.2] - 2025-11-26
### Added
- **Run Controls**: Support for `PAUSED` and `CANCELLED` run states.
- **Diagnostics**: `run explain` command to inspect stalled runs and generate operator hints.
- **HPC Concurrency**: Configurable `max_hpc_jobs_per_run` limits to prevent scheduler flooding.
- **Multi-Run Scheduler**: `run loop` can now round-robin between multiple active runs in a workspace.
- **Evidence Rebuild**: Idempotent `run export-evidence` command that rebuilds reports from the state store.

### Changed
- **HPC State Mapping**: Robust mapping of 20+ Slurm states (e.g., `TIMEOUT`, `NODE_FAIL`) to canonical MatterStack job states.
- **Safety**: Strict path traversal protection for file operations within the Run Root.
- **Validation**: Pydantic models for all Operator manifests (DirectHPC, ManualHPC, Human).

## [0.2.1] - 2025-11-26
### Added
- **Schema Versioning**: SQLite databases now include schema version checks to prevent corruption.
- **Concurrency Locking**: File-based locking (`run.lock`) to prevent race conditions during `step_run`.
- **Structured Logging**: Tick summaries for better observability.

### Fixed
- **Idempotency**: Handled partial failures and process crashes without duplicating task submissions.
- **Operator Robustness**: Graceful degradation when operators receive malformed inputs or missing files.

## [0.2.0] - 2025-11-26
### Added
- **Run-Centric Architecture**: Persistent, state-store backed execution model (replacing transient in-memory loops).
- **Stateless Orchestrator**: `matterstack run step` "tick" mechanism.
- **Unified Operators**: Abstraction for `ExternalRun` management (DirectHPC, ManualHPC, Human, Experiment).
- **SQLite Backend**: Transactional state storage for runs, tasks, and workflow state.
- **CLI**: New command structure `matterstack run [init|step|status|loop]`.

### Removed
- Legacy in-memory campaign engine.