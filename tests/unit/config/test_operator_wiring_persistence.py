from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import pytest

from matterstack.config.operator_wiring import (
    OperatorWiringSource,
    resolve_operator_wiring,
)
from matterstack.core.run import RunHandle

_ENV_NAME = "MATTERSTACK_OPERATORS_CONFIG"


def _mk_handle(tmp_path: Path, *, workspace_slug: str = "ws", run_id: str = "r1") -> RunHandle:
    run_root = tmp_path / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return RunHandle(workspace_slug=workspace_slug, run_id=run_id, root_path=run_root)


def _write_file(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_snapshot_persistence_is_idempotent_for_same_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If the effective bytes are unchanged, we should not rewrite operators_snapshot/operators.yaml.
    """
    monkeypatch.delenv(_ENV_NAME, raising=False)

    handle = _mk_handle(tmp_path, workspace_slug="ws_idempotent")
    workspace_base = tmp_path / "workspaces"
    ws_default = workspace_base / "ws_idempotent" / "operators.yaml"

    payload = b"operators:\n  hpc.default:\n    kind: hpc\n"
    _write_file(ws_default, payload)

    w1 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w1.source == OperatorWiringSource.WORKSPACE_DEFAULT
    assert w1.snapshot_path is not None

    snap = Path(w1.snapshot_path)
    meta = Path(w1.metadata_path)
    hist = Path(w1.history_path)

    assert snap.is_file()
    assert meta.is_file()
    assert hist.is_file()

    mtime1 = snap.stat().st_mtime
    sha1 = w1.sha256
    assert sha1 == hashlib.sha256(payload).hexdigest()

    # Ensure filesystem mtime granularity won't trick us (some FS are 1s granularity).
    time.sleep(1.05)

    w2 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w2.source == OperatorWiringSource.RUN_PERSISTED
    assert w2.snapshot_path == w1.snapshot_path
    assert w2.sha256 == sha1

    mtime2 = snap.stat().st_mtime
    assert mtime2 == mtime1, "Expected snapshot file mtime to be unchanged for idempotent resolve"


def test_sha256_stable_across_resolves_and_matches_file_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENV_NAME, raising=False)

    handle = _mk_handle(tmp_path, workspace_slug="ws_sha")
    workspace_base = tmp_path / "workspaces"
    ws_default = workspace_base / "ws_sha" / "operators.yaml"

    payload = b"operators:\n  human.default:\n    kind: human\n"
    _write_file(ws_default, payload)

    w1 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w1.snapshot_path is not None
    sha_expected = hashlib.sha256(payload).hexdigest()
    assert w1.sha256 == sha_expected

    snap_bytes = Path(w1.snapshot_path).read_bytes()
    assert hashlib.sha256(snap_bytes).hexdigest() == sha_expected

    w2 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w2.sha256 == sha_expected


def test_metadata_and_history_reconstructed_when_only_snapshot_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Resilience behavior:
    - If operators_snapshot/operators.yaml exists but metadata.json is missing,
      resolver should recreate metadata.json and history.jsonl (best-effort) and return RUN_PERSISTED.
    """
    monkeypatch.delenv(_ENV_NAME, raising=False)

    handle = _mk_handle(tmp_path, workspace_slug="ws_recover")
    run_root = Path(handle.root_path)

    snap_dir = run_root / "operators_snapshot"
    snap_dir.mkdir(parents=True, exist_ok=True)

    snap_yaml = snap_dir / "operators.yaml"
    payload = b"operators:\n  local.default:\n    kind: local\n    backend:\n      type: local\n"
    snap_yaml.write_bytes(payload)

    meta_json = snap_dir / "metadata.json"
    hist_jsonl = snap_dir / "history.jsonl"

    # Ensure "corrupt/missing metadata" starting point.
    if meta_json.exists():
        meta_json.unlink()
    if hist_jsonl.exists():
        hist_jsonl.unlink()

    w = resolve_operator_wiring(handle, workspace_base_path=tmp_path / "workspaces")
    assert w.source == OperatorWiringSource.RUN_PERSISTED
    assert w.snapshot_path == str(snap_yaml)
    assert Path(w.metadata_path).is_file()
    assert Path(w.history_path).is_file()

    meta = json.loads(Path(w.metadata_path).read_text(encoding="utf-8"))
    assert meta["schema_version"] == 1
    assert meta["effective"]["source"] == OperatorWiringSource.RUN_PERSISTED.value
    assert meta["effective"]["snapshot_relpath"] == "operators_snapshot/operators.yaml"

    lines = [ln for ln in Path(w.history_path).read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "Expected at least one reconstructed history event"
    first = json.loads(lines[0])
    assert first["event"] == "WIRING_PERSISTED"
    assert first["source"] == OperatorWiringSource.RUN_PERSISTED.value
