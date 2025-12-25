from __future__ import annotations

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


def test_precedence_cli_over_workspace_and_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws1")

    workspace_base = tmp_path / "workspaces"
    workspace_default = workspace_base / "ws1" / "operators.yaml"
    _write_file(workspace_default, b"workspace: true\n")

    env_cfg = _write_file(tmp_path / "env_ops.yaml", b"env: true\n")
    monkeypatch.setenv(_ENV_NAME, str(env_cfg))

    cli_cfg = _write_file(tmp_path / "cli_ops.yaml", b"cli: true\n")

    wiring = resolve_operator_wiring(
        handle,
        cli_operators_config_path=str(cli_cfg),
        workspace_base_path=workspace_base,
    )

    assert wiring.source == OperatorWiringSource.CLI_OVERRIDE
    assert wiring.resolved_path == str(cli_cfg.resolve())
    assert wiring.snapshot_path is not None
    assert Path(wiring.snapshot_path).is_file()
    assert Path(wiring.snapshot_path).read_bytes() == b"cli: true\n"


def test_precedence_run_snapshot_over_workspace_env_and_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws2")
    workspace_base = tmp_path / "workspaces"

    # First resolution via workspace default => persists a run snapshot.
    workspace_default_initial = workspace_base / "ws2" / "operators.yaml"
    _write_file(workspace_default_initial, b"workspace: initial\n")
    w1 = resolve_operator_wiring(handle, workspace_base_path=workspace_base)
    assert w1.source == OperatorWiringSource.WORKSPACE_DEFAULT
    assert w1.snapshot_path is not None

    # Now change workspace/env/legacy inputs. Resolver should still prefer the run snapshot.
    _write_file(workspace_default_initial, b"workspace: changed\n")
    env_cfg = _write_file(tmp_path / "env_ops.yaml", b"env: changed\n")
    monkeypatch.setenv(_ENV_NAME, str(env_cfg))

    w2 = resolve_operator_wiring(
        handle,
        workspace_base_path=workspace_base,
        legacy_profile="some_profile",
    )

    assert w2.source == OperatorWiringSource.RUN_PERSISTED
    assert w2.snapshot_path == w1.snapshot_path
    assert Path(w2.snapshot_path).read_bytes() == b"workspace: initial\n"


def test_precedence_workspace_default_over_env_and_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws3")
    workspace_base = tmp_path / "workspaces"

    workspace_default = workspace_base / "ws3" / "operators.yaml"
    _write_file(workspace_default, b"workspace: wins\n")

    env_cfg = _write_file(tmp_path / "env_ops.yaml", b"env: loses\n")
    monkeypatch.setenv(_ENV_NAME, str(env_cfg))

    wiring = resolve_operator_wiring(
        handle,
        workspace_base_path=workspace_base,
        legacy_profile="legacy_profile_should_not_apply",
    )

    assert wiring.source == OperatorWiringSource.WORKSPACE_DEFAULT
    assert wiring.snapshot_path is not None
    assert Path(wiring.snapshot_path).read_bytes() == b"workspace: wins\n"


def test_precedence_env_over_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle = _mk_handle(tmp_path, workspace_slug="ws4")
    workspace_base = tmp_path / "workspaces"

    env_cfg = _write_file(tmp_path / "env_ops.yaml", b"env: wins\n")
    monkeypatch.setenv(_ENV_NAME, str(env_cfg))

    wiring = resolve_operator_wiring(
        handle,
        workspace_base_path=workspace_base,
        legacy_profile="legacy_profile_should_not_apply",
    )

    assert wiring.source == OperatorWiringSource.ENV_VAR
    assert wiring.snapshot_path is not None
    assert Path(wiring.snapshot_path).read_bytes() == b"env: wins\n"


def test_precedence_legacy_profile_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENV_NAME, raising=False)

    handle = _mk_handle(tmp_path, workspace_slug="ws5")
    workspace_base = tmp_path / "workspaces"

    wiring = resolve_operator_wiring(
        handle,
        workspace_base_path=workspace_base,
        legacy_profile="my_profile",
    )

    assert wiring.source == OperatorWiringSource.LEGACY_PROFILE
    assert wiring.resolved_path == "my_profile"
    assert wiring.snapshot_path is not None
    snap_text = Path(wiring.snapshot_path).read_text(encoding="utf-8")
    assert "profile" in snap_text
    assert "my_profile" in snap_text


def test_precedence_legacy_hpc_config_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENV_NAME, raising=False)

    handle = _mk_handle(tmp_path, workspace_slug="ws6")
    workspace_base = tmp_path / "workspaces"

    hpc_cfg = _write_file(tmp_path / "legacy_hpc.yaml", b"cluster: {}\n")

    wiring = resolve_operator_wiring(
        handle,
        workspace_base_path=workspace_base,
        legacy_hpc_config_path=str(hpc_cfg),
    )

    assert wiring.source == OperatorWiringSource.LEGACY_HPC_CONFIG
    assert wiring.resolved_path == str(hpc_cfg)
    assert wiring.snapshot_path is not None
    snap_text = Path(wiring.snapshot_path).read_text(encoding="utf-8")
    assert "hpc_yaml" in snap_text
    assert str(hpc_cfg) in snap_text
