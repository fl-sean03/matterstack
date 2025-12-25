from __future__ import annotations

import json
from pathlib import Path

import pytest

from matterstack.config.operator_wiring import (
    OperatorWiringSource,
    resolve_operator_wiring,
)
from matterstack.core.run import RunHandle


def _mk_handle(tmp_path: Path, *, workspace_slug: str = "ws", run_id: str = "r1") -> RunHandle:
    run_root = tmp_path / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return RunHandle(workspace_slug=workspace_slug, run_id=run_id, root_path=run_root)


def _write_file(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _read_history_events(history_path: Path) -> list[dict]:
    if not history_path.is_file():
        return []
    out: list[dict] = []
    for ln in history_path.read_text(encoding="utf-8").splitlines():
        if ln.strip():
            out.append(json.loads(ln))
    return out


def test_override_refused_without_force_records_history_and_does_not_mutate_snapshot(tmp_path: Path) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws_override")
    workspace_base = tmp_path / "workspaces"

    # Create initial persisted snapshot via workspace default.
    ws_default = workspace_base / "ws_override" / "operators.yaml"
    _write_file(ws_default, b"workspace: initial\n")
    w1 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w1.snapshot_path is not None

    snap_path = Path(w1.snapshot_path)
    hist_path = Path(w1.history_path)

    before_bytes = snap_path.read_bytes()
    before_events = _read_history_events(hist_path)
    before_count = len(before_events)

    # Now attempt CLI override with different bytes, without force => must refuse.
    cli_cfg = _write_file(tmp_path / "cli_ops.yaml", b"cli: different\n")

    with pytest.raises(ValueError, match="Refusing to override persisted operator wiring"):
        resolve_operator_wiring(
            handle,
            cli_operators_config_path=str(cli_cfg),
            force_override=False,
            workspace_base_path=workspace_base,
        )

    after_bytes = snap_path.read_bytes()
    assert after_bytes == before_bytes, "Snapshot should not be modified on refusal"

    after_events = _read_history_events(hist_path)
    assert len(after_events) == before_count + 1, "Refusal should append exactly one history line"

    last = after_events[-1]
    assert last["event"] == "WIRING_OVERRIDE_REFUSED"
    assert last["source"] == OperatorWiringSource.CLI_OVERRIDE.value
    assert last["details"]["attempted_sha256"]
    assert "Override refused" in (last["details"].get("note") or "")


def test_override_allowed_with_force_replaces_snapshot_and_appends_history(tmp_path: Path) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws_force")
    workspace_base = tmp_path / "workspaces"

    ws_default = workspace_base / "ws_force" / "operators.yaml"
    _write_file(ws_default, b"workspace: initial\n")
    w1 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w1.snapshot_path is not None

    snap_path = Path(w1.snapshot_path)
    hist_path = Path(w1.history_path)

    prior_sha = w1.sha256
    prior_bytes = snap_path.read_bytes()
    before_events = _read_history_events(hist_path)
    before_count = len(before_events)

    cli_cfg = _write_file(tmp_path / "cli_ops.yaml", b"cli: new\n")
    w2 = resolve_operator_wiring(
        handle,
        cli_operators_config_path=str(cli_cfg),
        force_override=True,
        workspace_base_path=workspace_base,
    )

    assert w2.source == OperatorWiringSource.CLI_OVERRIDE
    assert w2.snapshot_path == str(snap_path)
    assert snap_path.read_bytes() == b"cli: new\n"
    assert snap_path.read_bytes() != prior_bytes

    after_events = _read_history_events(hist_path)
    assert len(after_events) >= before_count + 1

    # The last event should be forced override.
    last = after_events[-1]
    assert last["event"] == "WIRING_OVERRIDE_FORCED"
    assert last["source"] == OperatorWiringSource.CLI_OVERRIDE.value
    assert last["details"]["prior_sha256"] == prior_sha
