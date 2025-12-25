# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.6] - 2025-12-25
### Added
- **Chronological Attempt IDs**: All generated IDs now use `YYYYMMDD_HHMMSS_<uuid8>` format for natural chronological sorting in directory listings.
- **First-Class Operator Key Routing**: Tasks can specify `operator_key` field directly instead of using environment variable workaround.
- **Per-Operator Concurrency Limits**: Configure `max_concurrent` per operator in `operators.yaml` with workspace-level `max_concurrent_global` fallback.
- **Attempt Lifecycle Hooks**: Plugin system (`AttemptLifecycleHook`) for metrics collection, notifications, and cleanup on attempt state transitions.
- **Orphan Attempt Cleanup**: New `matterstack cleanup-orphans` CLI command to detect and mark stuck attempts as FAILED_INIT.
- **FAILED_INIT Status**: New terminal status for attempts that fail during submission (before receiving an external_id).
- **Unified ID Generation**: Centralized `matterstack/core/id_generator.py` module with `generate_run_id()`, `generate_attempt_id()`, and `generate_task_id()`.
- **Workspace Path Discovery**: CLI commands now work from any subdirectory via `MATTERSTACK_WORKSPACES_ROOT` env var or pyproject.toml detection.
- **HPC Validation Demo**: New `workspaces/demos/attempt_id_validation/` workspace for testing chronological IDs on HPC infrastructure.

### Changed
- **Schema Version v4**: Added `tasks.operator_key` column for first-class operator routing with automatic migration.
- **Operator Dispatch Priority**: Task `operator_key` field now takes precedence over `env["MATTERSTACK_OPERATOR"]` routing.
- **Terminal States**: `FAILED_INIT` added to terminal states for accurate concurrency accounting.

### Fixed
- **Campaign State Explosion**: MXene campaign now stores file paths instead of base64 content, keeping `campaign_state.json` under 1KB.
- **Operator Routing on Resume**: Workspace drivers now load the run's saved `simulation_config.json` instead of workspace defaults.
- **Jinja2 Template Syntax**: Fixed `default()` filter usage in MXene shear templates.
- **Project Root Detection**: CLI commands now find workspaces from any subdirectory by walking up to pyproject.toml.

## [0.2.5] - 2025-12-14
### Added
- **Task Attempts**: First-class attempt history per logical task, enabling provenance-safe reruns without overwriting artifacts.
- **Attempt-aware CLI**: New commands `revive`, `rerun`, `attempts`, and `cancel-attempt` for operational control of long-running runs.
- **Canonical HPC validation workspace**: `workspaces/hpc_attempts_validation/` demonstrating fail → rerun → succeed with preserved attempt evidence (local + Slurm).
- **Opt-in pytest stability mitigation**: `sitecustomize.py` gated by `MATTERSTACK_PYTEST_STUB_READLINE=1` for environments where pytest triggers segfaults.

### Changed
- **Orchestration**: Run lifecycle now operates on attempts (submission, polling, concurrency accounting, and results collection).
- **HPC execution layout**: Attempt-scoped local evidence directories and attempt-scoped remote workdirs to prevent artifact collisions.
- **Evidence export**: Evidence bundles now include attempt history and current-attempt metadata, with legacy external-run fallback.

### Fixed
- **HPC backend API mismatch**: Aligned Slurm download keyword signature with the ComputeBackend interface to avoid runtime `TypeError`.
- **Local backend collection**: Corrected download/collection behavior when executing locally in “HPC mode” (LocalBackend + ComputeOperator).

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