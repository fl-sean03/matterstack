# Refactor Map: Max 500 LOC Enforcement

## Overview

This document maps the structural changes made during the max-500-lines-per-file refactor of MatterStack. The refactor successfully reduced all source files to ≤500 lines while maintaining full backward compatibility with existing public APIs.

**Refactor Date:** 2025-12-21  
**Total Files Over Limit Before:** 5  
**Total Files Over Limit After:** 0  
**All Tests Passing:** ✅ 213 tests

---

## Summary of Changes

| Original File | Before | After | New Modules Created |
|--------------|--------|-------|---------------------|
| `matterstack/orchestration/run_lifecycle.py` | 945 | 52 | 6 modules |
| `matterstack/cli/main.py` | 897 | 194 | 4 modules (commands/ subpackage) |
| `matterstack/storage/state_store.py` | 740 | 155 | 5 mixin modules |
| `matterstack/config/operator_wiring.py` | 640 | 290 | 4 internal modules |
| `matterstack/runtime/operators/hpc.py` | 564 | 435 | 1 module |

---

## File Mappings

### 1. orchestration/run_lifecycle.py (945 → 52 lines)

The orchestration module was the largest file and required the most extensive refactoring. It was split into 6 cohesive modules organized by workflow phase.

| Old Location | New Location | Lines | Description |
|--------------|--------------|-------|-------------|
| run_lifecycle.py (init logic) | [`initialization.py`](matterstack/orchestration/initialization.py) | 198 | `RunLifecycleError`, `initialize_run()`, `initialize_or_resume_run()` |
| run_lifecycle.py (POLL phase) | [`polling.py`](matterstack/orchestration/polling.py) | 246 | Attempt polling, status mapping, operator lookup |
| run_lifecycle.py (EXECUTE phase) | [`dispatch.py`](matterstack/orchestration/dispatch.py) | 355 | Task submission, operator dispatch, concurrency control |
| run_lifecycle.py (ANALYZE phase) | [`analyze.py`](matterstack/orchestration/analyze.py) | 182 | Results construction, campaign state persistence |
| run_lifecycle.py (utilities) | [`utilities.py`](matterstack/orchestration/utilities.py) | 142 | `run_until_completion()`, `list_active_runs()` |
| run_lifecycle.py (coordinator) | [`step_execution.py`](matterstack/orchestration/step_execution.py) | 290 | Main `step_run()` orchestrating all phases |

**Backward Compatibility:**
```python
# run_lifecycle.py is now a thin shim re-exporting all public APIs
from matterstack.orchestration.run_lifecycle import (
    initialize_run,           # ✅ Works
    initialize_or_resume_run, # ✅ Works
    step_run,                 # ✅ Works
    run_until_completion,     # ✅ Works
    list_active_runs,         # ✅ Works
    RunLifecycleError,        # ✅ Works
    RunHandle,                # ✅ Works (re-export)
)
```

---

### 2. cli/main.py (897 → 194 lines)

The CLI module was split into a `commands/` subpackage, with commands grouped by functional area.

| Old Location | New Location | Lines | Description |
|--------------|--------------|-------|-------------|
| main.py (run lifecycle) | [`commands/run_management.py`](matterstack/cli/commands/run_management.py) | 318 | `cmd_init`, `cmd_step`, `cmd_loop`, `cmd_cancel`, `cmd_pause`, `cmd_resume`, `cmd_revive` |
| main.py (task ops) | [`commands/task_management.py`](matterstack/cli/commands/task_management.py) | 216 | `cmd_rerun`, `cmd_attempts`, `cmd_cancel_attempt`, `_confirm_or_exit` |
| main.py (inspection) | [`commands/inspection.py`](matterstack/cli/commands/inspection.py) | 168 | `cmd_status`, `cmd_explain`, `cmd_monitor`, `cmd_export_evidence` |
| main.py (self-test) | [`commands/self_test.py`](matterstack/cli/commands/self_test.py) | 100 | `SelfTestCampaign`, `cmd_self_test` |
| main.py (entry) | [`main.py`](matterstack/cli/main.py) | 194 | `main()` entry point, argparse setup |

**Subpackage Structure:**
```
matterstack/cli/
├── main.py              # Entry point + argparse
├── commands/
│   ├── __init__.py      # Re-exports all cmd_* functions
│   ├── run_management.py
│   ├── task_management.py
│   ├── inspection.py
│   └── self_test.py
├── operator_registry.py # Unchanged (185 lines)
├── reset.py             # Unchanged (124 lines)
├── tui.py               # Unchanged (122 lines)
└── utils.py             # Unchanged (83 lines)
```

**Backward Compatibility:**
- CLI entry point `matterstack.cli.main:main` unchanged in pyproject.toml
- All CLI subcommands work identically

---

### 3. storage/state_store.py (740 → 155 lines)

The state store was refactored using a **mixin pattern**, with `SQLiteStateStore` inheriting from 5 specialized mixin classes.

| Old Location | New Location | Lines | Description |
|--------------|--------------|-------|-------------|
| state_store.py (migrations) | [`_migrations.py`](matterstack/storage/_migrations.py) | 149 | `_MigrationsMixin`: schema migrations v1→v2→v3 |
| state_store.py (run CRUD) | [`_run_operations.py`](matterstack/storage/_run_operations.py) | 121 | `_RunOperationsMixin`: run create/get/set methods |
| state_store.py (task CRUD) | [`_task_operations.py`](matterstack/storage/_task_operations.py) | 143 | `_TaskOperationsMixin`: workflow and task methods |
| state_store.py (external runs) | [`_external_run_ops.py`](matterstack/storage/_external_run_ops.py) | 155 | `_ExternalRunOperationsMixin`: v1 legacy support |
| state_store.py (attempts) | [`_attempt_operations.py`](matterstack/storage/_attempt_operations.py) | 222 | `_AttemptOperationsMixin`: v2 attempt system |
| state_store.py (core) | [`state_store.py`](matterstack/storage/state_store.py) | 155 | Class shell, `__init__`, `_check_schema`, `lock` |

**Mixin Architecture:**
```python
class SQLiteStateStore(
    _MigrationsMixin,
    _RunOperationsMixin,
    _TaskOperationsMixin,
    _ExternalRunOperationsMixin,
    _AttemptOperationsMixin,
):
    """Main state store class inheriting all operations from mixins."""
```

**Backward Compatibility:**
```python
from matterstack.storage.state_store import SQLiteStateStore  # ✅ Works
```

---

### 4. config/operator_wiring.py (640 → 290 lines)

The operator wiring module was split into type definitions, provenance handling, persistence logic, and legacy support.

| Old Location | New Location | Lines | Description |
|--------------|--------------|-------|-------------|
| operator_wiring.py (types) | [`_wiring_types.py`](matterstack/config/_wiring_types.py) | 71 | `OperatorWiringSource`, `ResolvedOperatorWiring`, `OperatorWiringProvenance` |
| operator_wiring.py (provenance) | [`_wiring_provenance.py`](matterstack/config/_wiring_provenance.py) | 72 | `load_wiring_provenance_from_run_root()`, `format_operator_wiring_explain_line()` |
| operator_wiring.py (persistence) | [`_wiring_persistence.py`](matterstack/config/_wiring_persistence.py) | 271 | Snapshot writing, history, metadata |
| operator_wiring.py (legacy) | [`_wiring_legacy.py`](matterstack/config/_wiring_legacy.py) | 48 | Legacy operators.yaml generation |
| operator_wiring.py (main) | [`operator_wiring.py`](matterstack/config/operator_wiring.py) | 290 | `resolve_operator_wiring()`, re-exports |

**Backward Compatibility:**
```python
from matterstack.config.operator_wiring import (
    resolve_operator_wiring,       # ✅ Works
    OperatorWiringSource,          # ✅ Works
    ResolvedOperatorWiring,        # ✅ Works
    OperatorWiringProvenance,      # ✅ Works
    format_operator_wiring_explain_line,  # ✅ Works
)
```

---

### 5. runtime/operators/hpc.py (564 → 435 lines)

The HPC operator module had config snapshot utilities extracted to a dedicated module.

| Old Location | New Location | Lines | Description |
|--------------|--------------|-------|-------------|
| hpc.py (config snapshot) | [`_config_snapshot.py`](matterstack/runtime/operators/_config_snapshot.py) | 139 | `_sha256_bytes()`, `_compute_combined_config_hash()`, `_write_attempt_config_snapshot()` |
| hpc.py (operator) | [`hpc.py`](matterstack/runtime/operators/hpc.py) | 435 | `ComputeOperator` class with all methods |

**Backward Compatibility:**
```python
from matterstack.runtime.operators.hpc import ComputeOperator  # ✅ Works
```

---

## Public API Compatibility

All original import paths continue to work via re-exports:

```python
# Orchestration
from matterstack.orchestration.run_lifecycle import execute_run      # ✅
from matterstack.orchestration import initialize_run, step_run       # ✅
from matterstack import initialize_run, run_until_completion         # ✅

# CLI
from matterstack.cli.main import main                                # ✅

# Storage
from matterstack.storage.state_store import SQLiteStateStore         # ✅

# Config
from matterstack.config.operator_wiring import resolve_operator_wiring  # ✅
```

---

## Internal Module Conventions

Files prefixed with underscore (`_`) are **internal implementation details** and not part of the public API:

- `_migrations.py` - Schema migration logic
- `_run_operations.py` - Run CRUD operations
- `_task_operations.py` - Task CRUD operations
- `_external_run_ops.py` - External run operations
- `_attempt_operations.py` - Attempt operations
- `_wiring_types.py` - Wiring type definitions
- `_wiring_provenance.py` - Provenance handling
- `_wiring_persistence.py` - Snapshot persistence
- `_wiring_legacy.py` - Legacy format support
- `_config_snapshot.py` - Config snapshot utilities

---

## Running the Line Limit Check

To verify all files comply with the 500-line limit:

```bash
./scripts/check_max_lines.sh
```

Or manually:
```bash
find matterstack -name "*.py" -exec wc -l {} + | sort -rn | head -30
```

---

## Test Verification

All 213 tests pass after the refactor:

```bash
uv run pytest tests/ -v
```

Test output saved to: `artifacts/final_checks.txt`

---

## Line Count Artifacts

- **Before refactor:** `artifacts/linecount_before.txt`
- **After refactor:** `artifacts/linecount_after.txt`

### Top 10 Largest Files After Refactor

| Lines | File |
|-------|------|
| 435 | matterstack/runtime/operators/hpc.py |
| 388 | matterstack/runtime/backends/local.py |
| 377 | matterstack/runtime/backends/hpc/ssh.py |
| 355 | matterstack/orchestration/dispatch.py |
| 345 | matterstack/orchestration/api.py |
| 336 | matterstack/runtime/backends/hpc/backend.py |
| 320 | matterstack/storage/export.py |
| 318 | matterstack/cli/commands/run_management.py |
| 290 | matterstack/orchestration/step_execution.py |
| 290 | matterstack/config/operator_wiring.py |

All files are now under the 500-line limit. ✅

---

## Dependency Direction

The refactor maintains proper dependency direction to avoid circular imports:

```
Lower-level (types, utilities)
         ↓
Core modules (models, schemas)
         ↓
Operations (mixins, phase modules)
         ↓
Coordinators (step_execution, main entry points)
         ↓
Shims (re-export modules for backward compatibility)
```

---

## Future Maintenance

When adding new code:

1. **Check line counts** before committing: `./scripts/check_max_lines.sh`
2. **Prefer new modules** over expanding existing ones
3. **Use mixins** for class methods that can be grouped
4. **Extract utilities** to dedicated `_internal.py` modules
5. **Maintain backward compatibility** via re-exports in shim modules
