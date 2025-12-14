# MatterStack DevGuide v0.2.7.7 – Subtask 7 (Workspace auto-discovery validation) + Stability Fixes

This guide is a “take a step back” planning document for completing **Subtask 7** (workspace-default operator wiring auto-discovery) after discovering the current E2E path has inconsistencies and at least one test failure.

It covers:
- what the system is supposed to do (v0.2.7)
- what is currently failing / looks like “stalling”
- the minimum set of fixes and validations needed before adding the new workspace + E2E test
- a concrete, ordered implementation/validation checklist

---

## 1) Target behavior recap (v0.2.7)

MatterStack v0.2.7 supports resolving operator wiring by precedence (highest to lowest) in [`resolve_operator_wiring()`](matterstack/config/operator_wiring.py:409):

1. CLI override: `--operators-config`
2. Run snapshot: `<run_root>/operators_snapshot/operators.yaml`
3. Workspace default: `workspaces/<workspace_slug>/operators.yaml`
4. Env var: `MATTERSTACK_OPERATORS_CONFIG`
5. Legacy fallback (`--hpc-config` / `--profile`) via generated snapshot

Once resolved, the system must:
- **persist** a run-local snapshot under `operators_snapshot/` (`operators.yaml`, `metadata.json`, `history.jsonl`)
- allow **zero-flag resume** (subsequent `step`/`loop` should not require `--operators-config`)
- include snapshot + provenance in **evidence export** (`evidence/operators_snapshot/*` plus a `data.operator_wiring` section in `bundle.json`)

Subtask 7 adds the missing “auto-discovery story” where no `--operators-config` is provided and the workspace default is used.

---

## 2) What happened: why the E2E “looked like stalling”

The E2E we used to sanity-check the current system is [`tests/e2e/test_cli_operators_config_routing_persistence_export.py`](tests/e2e/test_cli_operators_config_routing_persistence_export.py:1).

That test can appear to “stall” for three common reasons:

1) **Human gate never completes**
- The workflow includes a gate task. The test completes it by writing `response.json` into the attempt’s artifact directory.
- If the attempt directory isn’t created (or the attempt never reaches a waiting state), the loop keeps stepping.

2) **Operator registry mismatch**
- If the CLI builds an operator registry that cannot route `MATTERSTACK_OPERATOR=hpc.default`, tasks may fail/never progress.
- We observed a concrete failure caused by `RegistryConfig` not accepting `operators_config_path` (fixed by adding the field and loading operators.yaml in [`matterstack/cli/operator_registry.py`](matterstack/cli/operator_registry.py:22)).

3) **Evidence export expectations not met**
- The E2E asserts `evidence/operators_snapshot/*` exists and the report mentions the copied snapshot.
- If evidence export doesn’t copy the snapshot, the test fails late, which can be mistaken for a “stall” if one is watching CI logs rather than the final assertion.

Important: the most recent failure was not an infinite loop; it was a deterministic assertion failure about snapshot files missing from evidence export.

---

## 3) Current repo state (ground truth from re-read)

### 3.1 Operator wiring resolver: workspace-default discovery exists
Workspace default is checked at [`matterstack/config/operator_wiring.py`](matterstack/config/operator_wiring.py:521).

This is the mechanism Subtask 7 must validate end-to-end.

### 3.2 CLI operator registry building
`step`/`loop` build a registry with `operators_config_path=wiring.snapshot_path` in [`matterstack/cli/main.py`](matterstack/cli/main.py:87).

We have a compatibility adapter in [`matterstack/cli/operator_registry.py`](matterstack/cli/operator_registry.py:112) that:
- builds from canonical `operators.yaml` when `operators_config_path` is present
- provides aliases so orchestrator lookups still work with both canonical keys (`hpc.default`) and legacy types (`HPC`)

### 3.3 Attempt provenance: `operator_key` needed and now planned as schema v3
The E2E expects `TaskAttemptModel.operator_key` to exist and be populated.
- Schema definition: [`matterstack/storage/schema.py`](matterstack/storage/schema.py:124) (requires an `operator_key` column).
- Migration logic: [`matterstack/storage/state_store.py`](matterstack/storage/state_store.py:139) (v2 -> v3 migration adds `operator_key`).

The orchestrator now passes `operator_key` at attempt creation in [`matterstack/orchestration/run_lifecycle.py`](matterstack/orchestration/run_lifecycle.py:508).

### 3.4 Evidence export: required behaviors
Evidence export must:
- copy `operators_snapshot/*` into `evidence/operators_snapshot/*`
- include wiring provenance in `bundle.json` under `data.operator_wiring`
- mention the copied snapshot in `report.md`

Planned implementation location:
- [`build_evidence_bundle()`](matterstack/storage/export.py:50) should add `operator_wiring` to bundle `data`.
- [`export_evidence_bundle()`](matterstack/storage/export.py:208) should copy snapshot files into evidence.

Note: the E2E also asserts task/attempt `operator_key` fields in `bundle.json`. That requires the evidence serialization (`_attempt_to_dict`) to include `operator_key`.

---

## 4) What must be fixed/validated before Subtask 7

Subtask 7 depends on the system being correct for the already-existing E2E baseline. Therefore we must make the baseline pass first.

### 4.1 Baseline “known-good” E2E must pass
Target:
- [`tests/e2e/test_cli_operators_config_routing_persistence_export.py`](tests/e2e/test_cli_operators_config_routing_persistence_export.py:1)

Command:
```bash
MATTERSTACK_PYTEST_STUB_READLINE=1 python -m pytest -q \
  tests/e2e/test_cli_operators_config_routing_persistence_export.py
```

Pass criteria:
- snapshot persisted under `<run_root>/operators_snapshot/`
- `attempt.operator_key` exists and matches expected canonical keys
- evidence export:
  - copies `operators_snapshot/*` into `evidence/operators_snapshot/*`
  - report contains “Copied into this evidence export under:”
  - bundle contains `data.operator_wiring`
  - bundle includes per-task/per-attempt `operator_key` (requires evidence serialization work)

### 4.2 Confirm evidence export populates `operator_key` in JSON
Even if the DB stores it, evidence export must emit it.

Required evidence JSON shape (as asserted by E2E):
- `bundle["data"]["tasks"][<task_id>]["operator_key"] == "<canonical>"`
- `bundle["data"]["tasks"][<task_id>]["current_attempt"]["operator_key"] == "<canonical>"`
- every element in `bundle["data"]["tasks"][<task_id>]["attempts"]` has `operator_key`

This likely requires changes in [`matterstack/storage/export.py`](matterstack/storage/export.py:27):
- add `operator_key` to `_attempt_to_dict()`
- add a task-level `operator_key` summary field derived from current attempt

### 4.3 Confirm CLI `attempts` output includes `operator_key` column
The E2E asserts the TSV header contains `operator_key`. Ensure:
- `matterstack attempts <run_id> <task_id>` prints an `operator_key` column
- values match canonical keys

Entry point: [`cmd_attempts()`](matterstack/cli/main.py:433).

---

## 5) Subtask 7 deliverables (after baseline is green)

### 5.1 Add shipped workspace with workspace-default operators.yaml
Create:
- [`workspaces/operator_wiring_autodiscovery_validation/main.py`](workspaces/operator_wiring_autodiscovery_validation/main.py:1)
- [`workspaces/operator_wiring_autodiscovery_validation/operators.yaml`](workspaces/operator_wiring_autodiscovery_validation/operators.yaml:1)

Requirements for operators.yaml:
- must match the schema enforced by [`load_operators_config()`](matterstack/config/operators.py:233)
- include at minimum:
  - `local.default` (kind `local`)
  - `human.default` (kind `human`)
  - `experiment.default` (kind `experiment`)
  - `hpc.default` (kind `hpc`, backend local, `dry_run: true` is acceptable)

Campaign requirements:
- minimal workflow that executes at least one compute task (use env `MATTERSTACK_OPERATOR: hpc.default` or `local.default`)
- avoid external dependencies; run fully locally
- export `get_campaign()` so CLI init finds it via [`load_workspace_context()`](matterstack/cli/utils.py:7)

### 5.2 Add new E2E test: no `--operators-config`
Create:
- [`tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py`](tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py:1)

Flow:
1. Stage the shipped workspace into `tmp_path/workspaces/` and `monkeypatch.chdir(tmp_path)`
2. Run:
   - `matterstack init operator_wiring_autodiscovery_validation` (no wiring flags)
   - `matterstack step <run_id>` (no wiring flags)
3. Assert run snapshot exists:
   - `<run_root>/operators_snapshot/operators.yaml`
   - `<run_root>/operators_snapshot/metadata.json`
   - `<run_root>/operators_snapshot/history.jsonl`
4. Run `matterstack export-evidence <run_id>`
5. Assert:
   - `<run_root>/evidence/operators_snapshot/*` exists
   - `bundle.json` contains `data.operator_wiring` and `source == WORKSPACE_DEFAULT`

Command:
```bash
MATTERSTACK_PYTEST_STUB_READLINE=1 python -m pytest -q \
  tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py
```

### 5.3 Append completion report section
Append to:
- [`docs/DevGuides/v0.2.7_Report.md`](docs/DevGuides/v0.2.7_Report.md:1)

Include:
- workspace path + file list
- test path
- exact pytest command run + passing result

---

## 6) Execution plan (detailed checklist)

### Phase A — Stabilize baseline E2E + required plumbing
1. Fix/confirm `RegistryConfig.operators_config_path` is supported and used.
   - File: [`matterstack/cli/operator_registry.py`](matterstack/cli/operator_registry.py:22)
   - Validation: baseline E2E proceeds past first `step`.

2. Ensure DB stores canonical `operator_key` on attempts.
   - Files:
     - [`matterstack/storage/schema.py`](matterstack/storage/schema.py:124)
     - [`matterstack/storage/state_store.py`](matterstack/storage/state_store.py:139)
     - [`matterstack/orchestration/run_lifecycle.py`](matterstack/orchestration/run_lifecycle.py:508)
   - Validation: baseline E2E passes the `attempt.operator_key` assertions.

3. Ensure evidence export copies operators snapshot into evidence and includes provenance in bundle.
   - File: [`matterstack/storage/export.py`](matterstack/storage/export.py:208)
   - Validation: baseline E2E passes `evidence/operators_snapshot/*` assertions.

4. Ensure evidence JSON includes operator_key fields required by E2E.
   - File: [`matterstack/storage/export.py`](matterstack/storage/export.py:27)
   - Validation: baseline E2E passes task/attempt operator_key assertions.

5. Run baseline E2E:
   - [`tests/e2e/test_cli_operators_config_routing_persistence_export.py`](tests/e2e/test_cli_operators_config_routing_persistence_export.py:1)

### Phase B — Implement Subtask 7 (workspace-default auto-discovery)
6. Add workspace:
   - [`workspaces/operator_wiring_autodiscovery_validation/`](workspaces/operator_wiring_autodiscovery_validation:1)

7. Add E2E:
   - [`tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py`](tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py:1)

8. Run targeted pytest for new E2E:
```bash
MATTERSTACK_PYTEST_STUB_READLINE=1 python -m pytest -q \
  tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py
```

### Phase C — Report
9. Append Subtask 7 section to:
- [`docs/DevGuides/v0.2.7_Report.md`](docs/DevGuides/v0.2.7_Report.md:1)

---

## 7) Known risks / mitigations
- If a test “stalls”, it’s usually waiting for the gate completion. Ensure the test writes `response.json` into the correct `relative_path` directory (as done in the existing E2E).
- Ensure the operator registry includes both canonical keys and legacy aliases to avoid mismatched lookups between dispatch strings (`hpc.default`) and operator implementations reporting legacy `operator_type` (`HPC` / `Local`).

---

## 8) Definition of done (Subtask 7)
- Workspace exists at [`workspaces/operator_wiring_autodiscovery_validation/`](workspaces/operator_wiring_autodiscovery_validation:1) with `main.py` and `operators.yaml`.
- New E2E exists at [`tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py`](tests/e2e/test_cli_operators_config_autodiscovery_workspace_default.py:1).
- New E2E passes with mitigation flag:
  - `MATTERSTACK_PYTEST_STUB_READLINE=1 python -m pytest -q <new_test>`
- Report section appended to [`docs/DevGuides/v0.2.7_Report.md`](docs/DevGuides/v0.2.7_Report.md:1) confirming paths + command + pass.
