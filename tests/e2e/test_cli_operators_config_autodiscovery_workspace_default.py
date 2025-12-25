import json
import re
import shutil
import sys
import time
from pathlib import Path

import pytest

from matterstack.cli.main import main

WORKSPACE_SLUG = "operator_wiring_autodiscovery_validation"


def _run_cli(capsys: pytest.CaptureFixture[str], argv: list[str]) -> str:
    """
    Run the CLI entrypoint with patched argv and return stdout.
    """
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(sys, "argv", ["main.py", *argv])
        main()
    return capsys.readouterr().out


def _parse_run_id(init_stdout: str) -> str:
    m = re.search(r"Run initialized:\s+([^\s]+)", init_stdout)
    assert m, f"Could not parse run_id from init output:\n{init_stdout}"
    return m.group(1)


def _run_root(tmp_path: Path, run_id: str) -> Path:
    return tmp_path / "workspaces" / WORKSPACE_SLUG / "runs" / run_id


def _copy_shipped_workspace_into_tmp(tmp_path: Path) -> None:
    """
    Copy the shipped workspace (in-repo) into tmp_path/workspaces/<slug> so the E2E
    does not write runs into the git checkout.
    """
    src = Path(__file__).resolve().parents[2] / "workspaces" / WORKSPACE_SLUG
    assert src.is_dir(), f"Shipped workspace not found at {src}"

    dst = tmp_path / "workspaces" / WORKSPACE_SLUG
    dst.mkdir(parents=True, exist_ok=True)

    for name in ["main.py", "operators.yaml"]:
        shutil.copy2(src / name, dst / name)


@pytest.fixture
def e2e_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Ensure Path("workspaces") in the CLI points at tmp_path/workspaces.
    monkeypatch.chdir(tmp_path)
    _copy_shipped_workspace_into_tmp(tmp_path)
    return tmp_path


def test_cli_operators_config_autodiscovery_workspace_default(
    e2e_workspace: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Avoid slow sleeps in CLI loop for fast CI.
    monkeypatch.setattr(time, "sleep", lambda _secs: None)

    # Prevent LocalBackend.download from copytree recursion when src==dst (LocalBackend case).
    # For this E2E we only care about auto-discovery + persistence + export, not download behavior.
    from matterstack.runtime.backends.local import LocalBackend

    async def _noop_download(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(LocalBackend, "download", _noop_download, raising=True)

    # 1) init (NO wiring flags)
    init_out = _run_cli(capsys, ["init", WORKSPACE_SLUG])
    run_id = _parse_run_id(init_out)

    run_root = _run_root(e2e_workspace, run_id)
    snap_dir = run_root / "operators_snapshot"
    snap_yaml = snap_dir / "operators.yaml"
    meta_json = snap_dir / "metadata.json"
    hist_jsonl = snap_dir / "history.jsonl"

    # 2) step (NO wiring flags) -> triggers WORKSPACE_DEFAULT resolution + snapshot persistence
    _run_cli(capsys, ["step", run_id])

    # 3) assert snapshot artifacts exist
    assert snap_yaml.exists(), f"Expected run snapshot at {snap_yaml}"
    assert meta_json.exists(), f"Expected run snapshot metadata at {meta_json}"
    assert hist_jsonl.exists(), f"Expected run snapshot history at {hist_jsonl}"

    # Sanity: metadata should record WORKSPACE_DEFAULT source
    meta = json.loads(meta_json.read_text(encoding="utf-8") or "{}")
    effective = meta.get("effective") if isinstance(meta, dict) else None
    assert isinstance(effective, dict)
    assert effective.get("source") == "WORKSPACE_DEFAULT"

    # 4) export-evidence
    _run_cli(capsys, ["export-evidence", run_id])

    # 5) assert evidence copy exists and bundle.json includes operator_wiring provenance
    evidence_dir = run_root / "evidence"
    ev_snap_dir = evidence_dir / "operators_snapshot"
    assert (ev_snap_dir / "operators.yaml").exists()
    assert (ev_snap_dir / "metadata.json").exists()
    assert (ev_snap_dir / "history.jsonl").exists()

    bundle_path = evidence_dir / "bundle.json"
    assert bundle_path.exists(), f"Expected evidence bundle at {bundle_path}"

    bundle = json.loads(bundle_path.read_text(encoding="utf-8") or "{}")
    ow = (bundle.get("data") or {}).get("operator_wiring") if isinstance(bundle, dict) else None
    assert isinstance(ow, dict), f"Expected data.operator_wiring dict, got: {type(ow)}"

    assert ow.get("source") == "WORKSPACE_DEFAULT"
