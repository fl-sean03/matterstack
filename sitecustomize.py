"""
Pytest-only startup shim to avoid importing the CPython `readline` extension.

Why this exists:
- In this environment, importing the native `readline` module segfaults.
- `pytest` imports `readline` very early (before test collection), so test-only fixes in
  `conftest.py` are too late.

Why NOT `readline.py` at repo root:
- A top-level `readline.py` shadows the standard library extension for *all* entrypoints
  whenever this repo root is on `sys.path`, which is too risky for releases.

Approach:
- Python auto-imports `sitecustomize` on interpreter startup (via `site`).
- To avoid impacting *any* non-test entrypoints, we install the stub **only** when
  explicitly enabled via an environment variable.

Note:
- Enable this mitigation only for tests in this environment:
    MATTERSTACK_PYTEST_STUB_READLINE=1
"""

from __future__ import annotations

import os
import sys
import types


def _truthy_env(name: str) -> bool | None:
    val = os.environ.get(name)
    if val is None:
        return None
    val = val.strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    return None


def _install_readline_stub() -> None:
    if "readline" in sys.modules:
        # Something already imported it; do not override.
        return

    stub = types.ModuleType("readline")
    stub.__doc__ = "Stubbed readline module injected by sitecustomize for pytest stability."

    # Minimal surface area: some libraries expect these symbols to exist.
    stub.__all__ = [
        "parse_and_bind",
        "read_init_file",
        "set_completer",
        "get_completer",
        "set_history_length",
        "get_history_length",
    ]

    stub._completer = None
    stub._history_length = 0

    def parse_and_bind(_string: str) -> None:
        return

    def read_init_file(_filename: str | None = None) -> None:
        return

    def set_completer(func) -> None:
        stub._completer = func

    def get_completer():
        return stub._completer

    def set_history_length(length: int) -> None:
        stub._history_length = int(length)

    def get_history_length() -> int:
        return int(stub._history_length)

    stub.parse_and_bind = parse_and_bind
    stub.read_init_file = read_init_file
    stub.set_completer = set_completer
    stub.get_completer = get_completer
    stub.set_history_length = set_history_length
    stub.get_history_length = get_history_length

    sys.modules["readline"] = stub


_force = _truthy_env("MATTERSTACK_PYTEST_STUB_READLINE")
if _force is True:
    _install_readline_stub()
else:
    # Explicit opt-in only (default off): do not attempt to infer pytest.
    pass
