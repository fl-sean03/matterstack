# DevGuide_v0.2.3 – Release Polish & Usability

## [AGENT::SUMMARY]

*   **PROJECT:** MatterStack v0.2.3 (Polish)
*   **GOAL:** Clean up the codebase for public consumption, establish clear versioning, and verify ease of use with a quickstart "smoke test".
*   **INPUT:** Feedback requesting a "micro" release focused on cosmetics and usability.
*   **FOCUS:** Versioning, API Clarity, and Onboarding.

---

## 1. Implementation Thrusts

### THRUST 1: Versioning & Changelog
**Goal:** Establish a clear release history.

*   **1.1 Package Version**:
    *   Update `matterstack/__init__.py` to expose `__version__ = "0.2.3"`.
    *   Update `pyproject.toml` to `version = "0.2.3"`.
*   **1.2 Changelog**:
    *   Create `CHANGELOG.md` in the root.
    *   Summarize v0.2.0 (Run-Centric), v0.2.1 (Hardening), v0.2.2 (Production Lifecycle).

### THRUST 2: Public API & Cleanup
**Goal:** Clearly distinguish between public interfaces and internal plumbing.

*   **2.1 API Audit**:
    *   Review `matterstack/__init__.py` and subpackage `__init__.py` files.
    *   Explicitly export public classes (e.g., `Campaign`, `Task`, `RunHandle`, `Operator`).
    *   Ensure internal modules (e.g., backend implementation details) are not unnecessarily exposed in top-level namespaces.

### THRUST 3: Quickstart & Smoke Test
**Goal:** Ensure a new user can run something in 5 minutes.

*   **3.1 Smoke Test Command**:
    *   Implement `matterstack self-test` (hidden CLI command).
    *   It creates a temp workspace, runs a trivial campaign (1 task), exports evidence, and cleans up.
    *   If successful, prints "MatterStack is operational."
*   **3.2 Quickstart Guide**:
    *   Update `README.md` with a "5-Minute Quickstart" section.
    *   Instructions: Install -> Init -> Run -> Explain -> Export.

---

## 2. Execution Plan

1.  **Versioning**: Set version strings and write Changelog.
2.  **API**: Clean up `__init__.py` exports.
3.  **Self-Test**: Implement the smoke test in CLI.
4.  **Docs**: Update README with quickstart.