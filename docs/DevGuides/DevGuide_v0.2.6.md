# DevGuide_v0.2.6 – Operator System v2: Config-Driven, Multi-Instance, Extensible Operator Routing (HPC + Lab + Human)

## [AGENT::SUMMARY]

- **PROJECT:** MatterStack v0.2.6 (Operator System Refactor on top of v0.2.5 Attempts)
- **GOAL:** Make operator selection + operator configuration **clean, portable, and extensible** so campaigns can mix HPC, lab equipment, and human verification without hardcoding site-specific details.
- **PRIMARY DELIVERABLE:** Operator System v2:
  - **Structured operator references** per task/attempt
  - **Config-driven operator registry** supporting **multiple instances** per operator kind
  - **Factory/plugin architecture** for future operators (lab instruments, cloud APIs, etc.)
  - **Backward-compatible migration** from v0.2.5’s `operator_type` routing
- **BASELINE:** v0.2.5 introduced Task Attempts and attempt-aware orchestration. See [`docs/DevGuides/DevGuide_v0.2.5.md`](docs/DevGuides/DevGuide_v0.2.5.md:1).

---

## 1. Problem Statement (Why v0.2.6)

v0.2.5’s attempt system solved provenance-safe reruns, but the **operator model** is still “v1”:

- Orchestrator routes by `attempt.operator_type` string in [`step_run()`](matterstack/orchestration/run_lifecycle.py:181).
- The CLI builds a fixed registry with `"HPC"`, `"Local"`, `"Human"`, `"Experiment"` in [`build_operator_registry()`](matterstack/cli/operator_registry.py:101).
- HPC is configured via a single `--hpc-config`/`--profile` input (also in [`operator_registry.py`](matterstack/cli/operator_registry.py:1)).

This creates friction for real labs:

1. **Single-instance limitation:** you can’t cleanly support multiple HPC environments at once (e.g., dev vs prod, two clusters, two partitions with different policies) because there is only one `"HPC"` operator instance.
2. **Routing vs configuration are conflated:** the same `operator_type` token implicitly means “HPC” *and* implies which backend config it uses.
3. **Non-portable naming risk:** teams see examples like `HPC_atesting_config.yaml` and assume they must bake `atesting` or `amilan128c` into campaign logic. That’s the opposite of portability.
4. **Extensibility ceiling:** every new operator tends to require bespoke CLI flags and hardcoded registry entries.

v0.2.6 defines a model where campaigns stay **site-agnostic**, and site-specific details live in **operator configuration files**.

---

## 2. Design Goals

### 2.1 Core Goals

1. **Separate routing from wiring**
   - Routing: *which operator conceptually?* (HPC vs Human vs Experiment)
   - Wiring: *how do we connect to that system?* (cluster host, partition, robot endpoint, credentials)

2. **Multi-instance operators**
   - Support multiple independent operator instances per kind:
     - `hpc.default`, `hpc.dev`, `hpc.clusterA`, `experiment.robot_main`, etc.
   - Campaigns refer only to **aliases**, not partition names.

3. **Extensible operator kinds**
   - Add new operator types without editing orchestrator core logic.
   - Prefer a factory/plugin registration mechanism.

4. **Backwards compatibility**
   - Existing runs and workspaces using `operator_type="HPC"` continue to work.
   - v0.2.5 DB schema stays valid. Any schema changes must be additive and migratable.

5. **Testable**
   - Unit + integration + e2e coverage proving:
     - routing correctness
     - config parsing correctness
     - no evidence overwrite (already covered by v0.2.5+ tests)
     - multi-instance selection works deterministically

### 2.2 Non-Goals (Explicit)

- Full container runtime support (Apptainer, Docker orchestration) beyond existing behavior.
- Replacing the polling orchestrator loop with event-driven architecture.
- A universal, one-size-fits-all “lab device protocol”; we’ll provide a clean extension hook.

---

## 3. Target Architecture (Operator System v2)

### 3.1 New Concept: `OperatorRef` (structured routing)

Introduce a structured operator reference:

```text
OperatorRef:
  kind: string   # e.g. "hpc", "local", "human", "experiment"
  name: string   # e.g. "default", "dev", "robot_main"
  # Optional future fields:
  #   labels/capabilities: key/value selectors
```

**Canonical operator key**: `"{kind}.{name}"`

Examples:
- `hpc.default`
- `human.default`
- `experiment.default`

#### Canonical operator key rules (v0.2.6 core)
- **Casing:** lowercase canonical form.
- **Parsing:** split on the **first** `.` to obtain `(kind, name)`. This preserves flexibility for future hierarchical names like `hpc.clusterA.dev` (kind=`hpc`, name=`clusterA.dev`).
- **Validation (canonical):**
  - `kind` must match: `[a-z][a-z0-9_]*`
  - `name` must match: `[a-z0-9][a-z0-9_.-]*`
  - Full key regex: `^[a-z][a-z0-9_]*\.[a-z0-9][a-z0-9_.-]*$`
  - Reject whitespace and `..` (ambiguous keys)

Reference implementation lives in [`matterstack/core/operator_keys.py`](matterstack/core/operator_keys.py:1).

This allows campaigns to remain portable:
- Campaigns use `hpc.default`, not `amilan128c`
- Each site maps `hpc.default` to its own scheduler/account/partition.

### 3.2 Where `OperatorRef` lives (data model)

v0.2.6 should treat `OperatorRef` as **first-class routing metadata**. We have two viable approaches:

#### Option A (lowest-risk): store `operator_ref` inside attempt `operator_data`
- Pros: no schema bump required; works with v0.2.5 DB schema ([`TaskAttemptModel.operator_data`](matterstack/storage/schema.py:124)).
- Cons: routing metadata is buried in JSON and not indexed; harder to query/report cleanly.

#### Option B (cleaner): add a new column (schema v3) on attempts
Add `operator_key` or `operator_ref` column to `task_attempts`:
- `operator_key: str` storing `hpc.default`
- or `operator_ref: JSON` storing `{kind,name}`

Pros: queryable, explicit, stable.  
Cons: schema bump and migration needed.

**Recommendation:** start with Option A as a transitional step if we want a minimal refactor, but plan Option B as the durable outcome. If we do v0.2.6 as a “full vibe refactor”, Option B is preferred.

### 3.3 Operator registry becomes config-driven (not hardcoded)

Replace the fixed registry in [`build_operator_registry()`](matterstack/cli/operator_registry.py:101) with a config-driven registry.

**New config file:** `operators.yaml`
Contains multiple operator instances keyed by canonical operator key (see canonical rules in [`matterstack/core/operator_keys.py`](matterstack/core/operator_keys.py:1)).

Implementation reference:
- Parser/validation: [`load_operators_config()`](matterstack/config/operators.py:1) / [`OperatorInstanceConfig`](matterstack/config/operators.py:1)
- Factory + cached registry: [`get_cached_operator_registry_from_operators_config()`](matterstack/runtime/operators/registry.py:1)

#### operators.yaml schema (v0.2.6)

Top-level shape:

```yaml
operators:  # mapping: <operator_key> -> <instance_config>
  <kind>.<name>:
    kind: <kind>  # must match the key kind
    # kind-specific fields...
```

Supported operator kinds (v0.2.6):
- `hpc.*` (compute)
- `local.*` (compute)
- `human.*`
- `experiment.*`

Compute kinds (`hpc.*`, `local.*`) support a `backend` object with a required discriminator `type`:

- `backend.type: local`
- `backend.type: slurm`
- `backend.type: profile` (reuse execution profiles from [`matterstack/config/profiles.py`](matterstack/config/profiles.py:1))
- `backend.type: hpc_yaml` (legacy CURC HPC YAML adapter, via [`_profile_from_hpc_yaml()`](matterstack/cli/operator_registry.py:34))

Example `operators.yaml`:

```yaml
operators:
  # Slurm-backed compute
  hpc.default:
    kind: hpc
    backend:
      type: slurm
      workspace_root: /scratch/me/matterstack_runs
      ssh:
        host: login.cluster.edu
        user: me
        port: 22
        key_path: /home/me/.ssh/id_ed25519
      slurm:
        partition: compute
        account: myacct
        qos: normal
        ntasks: 128
        time: 60
        # modules, etc. stay in config (never hardcoded in code)

  # Local-backed compute (workspace_root optional; if omitted, defaults to the run root)
  local.default:
    kind: local
    backend:
      type: local

  # Alternate HPC instance reusing a named execution profile
  hpc.prod:
    kind: hpc
    backend:
      type: profile
      name: alpine_prod

  # Migration helper: reuse legacy single-HPC YAML config format as one instance
  hpc.from_legacy:
    kind: hpc
    backend:
      type: hpc_yaml
      path: /path/to/HPC_atesting_config.yaml

  # Non-compute operators
  human.default:
    kind: human

  experiment.default:
    kind: experiment
    api:
      base_url: https://lab-gateway.example
      token_env: LAB_API_TOKEN
```

#### Validation / error semantics (strict)

Parsing is intentionally strict (fail-fast with clear errors):
- Keys must be **lowercase canonical** `kind.name` (reject whitespace, bad chars, uppercase).
- The config `kind` must match the key kind (e.g., `hpc.default` must have `kind: hpc`).
- Unknown `kind` is rejected.
- Unknown `backend.type` is rejected.
- Extra/unknown fields are rejected (to prevent typos from silently being ignored).

### 3.4 Operator factories (extensibility)

Introduce an internal “factory registry”:

- `OperatorFactory` interface: create operator instance from config.
- Built-in factories:
  - `hpc` factory → creates a compute operator backed by LocalBackend/SlurmBackend depending on instance config
  - `human` factory → creates HumanOperator
  - `experiment` factory → creates ExperimentOperator (or a more explicit “LabOperator”)

This reduces hardcoding and makes adding a new operator kind predictable:
1. Implement `MyNewOperator` and `MyNewOperatorFactory`
2. Register factory (static mapping first; entry-points later if desired)
3. Add `operators.<key>` stanza in config

---

## 4. Workflow + Campaign Authoring Model (How tasks choose operators)

### 4.1 Campaign code should not contain site details

Campaign/workspace authors should choose:
- `hpc.default` for compute simulations
- `human.default` for manual review
- `experiment.default` for lab equipment runs

They should **not** mention:
- Slurm partitions
- accounts
- module loads
- robot IDs
- endpoints

### 4.2 Defaults without env vars

`MATTERSTACK_OPERATOR=HPC`-style behavior is a convenience but not robust. Instead:

- Workspaces should declare defaults in a workspace config file (e.g., `workspace_config.yaml` or run `config.json`).
- CLI can optionally override defaults with explicit flags:
  - `--operators-config path/to/operators.yaml`
  - `--default-compute-operator hpc.default`

This makes behavior explicit and reproducible, and avoids environment-variable magic.

---

## 5. Orchestrator Routing Changes (v0.2.6)

Today, orchestrator routing is:

- read `attempt.operator_type`
- select `operators[operator_type]`

See the polling portion in [`step_run()`](matterstack/orchestration/run_lifecycle.py:181).

v0.2.6 evolves this to **canonical operator key resolution**:

### 5.1 Operator key resolution precedence (v0.2.6 core)
When routing an attempt, resolve `operator_key` in this order:

1. `attempt.operator_key` (schema v3 column)
2. `attempt.operator_data["operator_key"]` (transition path)
3. `attempt.operator_type`:
   - if it already matches canonical key format, treat it as the key
   - else map legacy operator types:
     - `"HPC"` → `hpc.default`
     - `"Local"` → `local.default`
     - `"Human"` → `human.default`
     - `"Experiment"` → `experiment.default`

Reference implementation: [`matterstack/core/operator_keys.py`](matterstack/core/operator_keys.py:1).

### 5.2 Registry lookup semantics
- Orchestrator uses a registry keyed by canonical `operator_key` → Operator instance.
- Transitional behavior: registries may expose both legacy keys and canonical keys pointing to the same instances (until config-driven registry lands).

### 5.3 Failure behavior (important for robustness)
If an attempt resolves to an `operator_key` not present in the registry, the orchestrator must **fail the attempt deterministically** (record `status_reason` and mark task FAILED) rather than silently skipping forever.

**Acceptance Criteria:** existing workspaces that use `"HPC"` still work (they resolve to `hpc.default`), while new workspaces can specify `hpc.default` explicitly.

---

## 6. CLI Changes (v0.2.6)

### 6.1 Replace single-purpose flags with general operator config

Current: `--hpc-config`, `--profile` (HPC-only) used in [`cmd_step()`](matterstack/cli/main.py:49).

v0.2.6 CLI should prefer:
- `--operators-config path/to/operators.yaml`

Keep compatibility:
- If `--operators-config` is absent but `--hpc-config` is present:
  - auto-build a minimal registry with `hpc.default` from that YAML plus `human.default` etc.

### 6.2 CLI must be “orchestrator runner”, not “operator selector”

The CLI should:
- load operator config
- run ticks/loop
- offer diagnostics

The CLI should not be the place where “this task uses HPC” is decided. That belongs in campaign logic and/or workspace defaults.

---

## 7. Storage / Schema Plan

### 7.1 Final decision (v0.2.6): schema v3 adds `task_attempts.operator_key`

**Decision:** v0.2.6 uses the durable approach and defines schema v3:

- Add `task_attempts.operator_key` (string, nullable)
  - stores canonical operator key like `hpc.default`
- Keep `operator_type` for backward compatibility until v0.3.x+

Migration behavior (additive only):
- Supported upgrade path: v1 → v2 → v3
- For existing attempt rows (v2 → v3):
  - Add `operator_key` column if missing
  - Backfill `operator_key`:
    - prefer `operator_data["operator_key"]` if present
    - else map from `operator_type` using legacy mapping (`HPC` → `hpc.default`, etc.)
    - else leave NULL

Implementation reference:
- Model: [`TaskAttemptModel`](matterstack/storage/schema.py:124)
- Migration: [`SQLiteStateStore`](matterstack/storage/state_store.py:37)

### 7.2 Transitional JSON storage (deferred; not chosen)
Storing `operator_key` inside `operator_data` is a viable transition mechanism, but v0.2.6 chooses schema v3 so routing metadata is explicit and queryable.

---

## 8. Test Plan (Unit + Integration + E2E)

### 8.1 Unit tests
1. Config parsing
   - Parse `operators.yaml` with multiple operator instances.
   - Validate missing fields are reported clearly.
2. Registry construction
   - Ensure `operator_key` maps to correct operator instance.
   - Ensure multiple instances of the same kind are independent.
3. Operator resolution logic
   - Given attempts with:
     - only `operator_type`
     - operator key in operator_data
     - operator_key column (if v3)
   - Confirm the correct operator instance is selected.

### 8.2 Integration tests
1. Mixed-operator run:
   - one compute attempt runs under `hpc.default` (use local-backed compute in tests)
   - one human task goes WAITING_EXTERNAL and is completed via response file (exercise [`HumanOperator`](matterstack/runtime/operators/human.py:1))
2. Multi-instance compute:
   - `hpc.default` and `hpc.dev` route to different backends or different workspace roots (local in tests).

### 8.3 E2E tests
- CLI:
  - `matterstack step/loop` with `--operators-config` runs a demo workflow.
  - `attempts` TSV includes `operator_key` (if exposed) and continues to include `config_hash` (v0.2.5+) from attempt operator_data.

### 8.4 Validation (real HPC, optional but recommended)
- Provide a canonical validation workspace (like v0.2.5’s) that uses `operators.yaml`:
  - `hpc.default` points at a real site HPC config file
  - run produces attempts with operator_key explicitly stored

---

## 9. Acceptance / Exit Criteria (v0.2.6)

1. Campaigns can express routing via canonical operator keys (e.g., `hpc.default`) without embedding site-specific values.
2. Operator config supports multiple instances.
3. CLI uses a general operator config file, with backward-compatible support for current HPC YAML.
4. Orchestrator resolves operator consistently for old and new runs.
5. Tests cover:
   - config parsing + registry building
   - operator selection logic
   - at least one mixed-operator integration
6. Optional: one live HPC validation documented similarly to v0.2.5.

---

## 10. Implementation Plan (Detailed, Ordered)

> This section is intended to become the canonical “runbook” for implementing v0.2.6.

1. **Design final data model**
   - ✅ Chosen for v0.2.6: schema v3 adds `task_attempts.operator_key` (nullable).
   - Compatibility:
     - preserve `operator_type`
     - backfill operator_key during v2 → v3 migration using legacy mapping.

2. **Define config schema for operator instances**
   - Add `operators.yaml` schema documentation in this guide.
   - Include examples for:
     - local compute
     - slurm compute
     - human
     - experiment (lab API stub)

3. **Implement operator config loader**
   - New module: `matterstack/config/operators.py` (recommended) to parse/validate.
   - Provide strict validation and helpful errors.

4. **Implement OperatorFactory layer**
   - New module: `matterstack/runtime/operators/registry.py` (recommended)
   - Factories for `hpc`, `human`, `experiment`, `local`.

5. **Refactor CLI registry builder**
   - Update [`build_operator_registry()`](matterstack/cli/operator_registry.py:101) to:
     - accept `--operators-config`
     - build multi-instance registry keyed by canonical operator key
     - preserve `--hpc-config` adapter behavior for backward compatibility

6. **Refactor orchestrator routing**
   - Update [`step_run()`](matterstack/orchestration/run_lifecycle.py:181) routing from `operator_type` to canonical operator key resolution.
   - Ensure legacy mapping exists and remains stable.

7. **Update state store / schema if needed**
   - If schema v3:
     - Add schema model field
     - Add migration v2 → v3
     - Add tests for migration

8. **Update workspaces / examples**
   - Update at least one workspace to demonstrate `operators.yaml` usage (without hardcoding a particular cluster’s partition name in code).

9. **Add/upgrade tests**
   - Unit tests for config parsing and registry creation.
   - Integration test for mixed operators + multi-instance selection.
   - E2E CLI test invoking `--operators-config`.

10. **Validation**
   - Local validation: run targeted suites + full pytest.
   - Optional HPC validation: documented procedure (site-specific config lives outside code).

---

## 11. Notes on “No Hardcoding” (Answering the portability concern)

- Names like `atesting`, `amilan128c`, `robotA` are **site-specific** and should only exist inside user-provided config files.
- The codebase should use only:
  - operator kinds (`hpc`, `human`, `experiment`, `local`)
  - generic aliases (`default`, `dev`, `prod`) chosen by the user/site
- This makes MatterStack portable across labs without requiring any code changes.

---

## Appendix A – Current v0.2.5 references (for implementers)

- Orchestrator tick routing point: [`step_run()`](matterstack/orchestration/run_lifecycle.py:181)
- Current CLI registry builder: [`build_operator_registry()`](matterstack/cli/operator_registry.py:101)
- Profiles system (reusable for HPC instances): [`matterstack/config/profiles.py`](matterstack/config/profiles.py:1)
- Attempt storage model: [`TaskAttemptModel`](matterstack/storage/schema.py:124)
- Operator interface: [`Operator`](matterstack/core/operators.py:43)