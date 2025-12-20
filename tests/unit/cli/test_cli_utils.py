"""
Unit tests for matterstack/cli/utils.py

Tests nested workspace path support for load_workspace_context() and find_run().
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import patch

from matterstack.cli.utils import load_workspace_context, find_run
from matterstack.core.run import RunHandle


class TestLoadWorkspaceContext:
    """Tests for load_workspace_context() nested path support."""

    def test_single_level_slug(self, tmp_path, monkeypatch):
        """Backward compatibility: single-level slug works."""
        # Setup: workspaces/test_workspace/main.py
        ws_dir = tmp_path / "workspaces" / "test_workspace"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"name": "test_workspace", "version": "1.0"}
'''
        )

        # Change to tmp_path so relative "workspaces" resolves correctly
        monkeypatch.chdir(tmp_path)

        result = load_workspace_context("test_workspace")

        assert result == {"name": "test_workspace", "version": "1.0"}
        # Verify module was registered with correct name
        assert "workspace.test_workspace" in sys.modules

    def test_nested_slug(self, tmp_path, monkeypatch):
        """Nested slug resolves correctly."""
        # Setup: workspaces/demos/battery_screening/main.py
        ws_dir = tmp_path / "workspaces" / "demos" / "battery_screening"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"name": "demos/battery_screening", "type": "demo"}
'''
        )

        monkeypatch.chdir(tmp_path)

        result = load_workspace_context("demos/battery_screening")

        assert result == {"name": "demos/battery_screening", "type": "demo"}
        # Verify module was registered with dots, not slashes
        assert "workspace.demos.battery_screening" in sys.modules
        assert "workspace.demos/battery_screening" not in sys.modules

    def test_deeply_nested_slug(self, tmp_path, monkeypatch):
        """Deeply nested slug (3+ levels) resolves correctly."""
        # Setup: workspaces/category/subcategory/workspace/main.py
        ws_dir = tmp_path / "workspaces" / "category" / "subcategory" / "workspace"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"name": "deeply_nested", "level": 3}
'''
        )

        monkeypatch.chdir(tmp_path)

        result = load_workspace_context("category/subcategory/workspace")

        assert result == {"name": "deeply_nested", "level": 3}
        assert "workspace.category.subcategory.workspace" in sys.modules

    def test_missing_workspace_raises_file_not_found(self, tmp_path, monkeypatch):
        """Non-existent workspace raises FileNotFoundError."""
        # Setup: empty workspaces directory
        (tmp_path / "workspaces").mkdir()
        monkeypatch.chdir(tmp_path)

        with pytest.raises(FileNotFoundError) as exc_info:
            load_workspace_context("nonexistent")

        assert "Workspace main file not found" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_missing_nested_workspace_raises_file_not_found(self, tmp_path, monkeypatch):
        """Non-existent nested workspace raises FileNotFoundError."""
        (tmp_path / "workspaces").mkdir()
        monkeypatch.chdir(tmp_path)

        with pytest.raises(FileNotFoundError) as exc_info:
            load_workspace_context("demos/nonexistent")

        assert "Workspace main file not found" in str(exc_info.value)
        assert "demos" in str(exc_info.value)

    def test_missing_get_campaign_raises_attribute_error(self, tmp_path, monkeypatch):
        """Workspace without get_campaign() raises AttributeError."""
        ws_dir = tmp_path / "workspaces" / "bad_workspace"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
# Missing get_campaign function
def some_other_function():
    pass
'''
        )

        monkeypatch.chdir(tmp_path)

        with pytest.raises(AttributeError) as exc_info:
            load_workspace_context("bad_workspace")

        assert "does not export 'get_campaign()'" in str(exc_info.value)

    def test_module_can_be_reloaded(self, tmp_path, monkeypatch):
        """Same workspace can be loaded multiple times (module reloading)."""
        ws_dir = tmp_path / "workspaces" / "reloadable"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
counter = 0
def get_campaign():
    global counter
    counter += 1
    return {"count": counter}
'''
        )

        monkeypatch.chdir(tmp_path)

        # First load
        result1 = load_workspace_context("reloadable")
        assert result1["count"] == 1

        # Second load - module is re-executed
        result2 = load_workspace_context("reloadable")
        # Note: counter resets because module is reloaded from file
        assert result2["count"] == 1


class TestFindRun:
    """Tests for find_run() recursive search."""

    def test_single_level_workspace(self, tmp_path):
        """Find run in single-level workspace."""
        # Setup: workspaces/battery_screening/runs/test_run_123/
        run_dir = tmp_path / "workspaces" / "battery_screening" / "runs" / "test_run_123"
        run_dir.mkdir(parents=True)
        # Create a marker file to make it look like a real run
        (run_dir / "state.sqlite").touch()

        result = find_run("test_run_123", base_path=tmp_path / "workspaces")

        assert result is not None
        assert isinstance(result, RunHandle)
        assert result.workspace_slug == "battery_screening"
        assert result.run_id == "test_run_123"
        assert result.root_path == run_dir

    def test_nested_workspace(self, tmp_path):
        """Find run in nested workspace returns full slug."""
        # Setup: workspaces/demos/battery_screening/runs/test_run_456/
        run_dir = (
            tmp_path
            / "workspaces"
            / "demos"
            / "battery_screening"
            / "runs"
            / "test_run_456"
        )
        run_dir.mkdir(parents=True)
        (run_dir / "state.sqlite").touch()

        result = find_run("test_run_456", base_path=tmp_path / "workspaces")

        assert result is not None
        assert result.workspace_slug == "demos/battery_screening"
        assert result.run_id == "test_run_456"
        assert result.root_path == run_dir

    def test_deeply_nested_workspace(self, tmp_path):
        """Find run in deeply nested workspace."""
        # Setup: workspaces/category/subcategory/workspace/runs/run_id/
        run_dir = (
            tmp_path
            / "workspaces"
            / "category"
            / "subcategory"
            / "workspace"
            / "runs"
            / "deep_run_789"
        )
        run_dir.mkdir(parents=True)

        result = find_run("deep_run_789", base_path=tmp_path / "workspaces")

        assert result is not None
        assert result.workspace_slug == "category/subcategory/workspace"
        assert result.run_id == "deep_run_789"

    def test_run_not_found_returns_none(self, tmp_path):
        """Non-existent run returns None."""
        # Setup: workspace exists but run doesn't
        runs_dir = tmp_path / "workspaces" / "battery_screening" / "runs"
        runs_dir.mkdir(parents=True)
        # Create a different run
        (runs_dir / "other_run").mkdir()

        result = find_run("nonexistent_run", base_path=tmp_path / "workspaces")

        assert result is None

    def test_empty_workspaces_returns_none(self, tmp_path):
        """Empty workspaces directory returns None."""
        (tmp_path / "workspaces").mkdir()

        result = find_run("any_run", base_path=tmp_path / "workspaces")

        assert result is None

    def test_nonexistent_base_path_returns_none(self, tmp_path):
        """Non-existent base_path returns None."""
        result = find_run("any_run", base_path=tmp_path / "nonexistent")

        assert result is None

    def test_multiple_workspaces_finds_correct_one(self, tmp_path):
        """When multiple workspaces exist, finds the one with the run."""
        # Setup: multiple workspaces, only one has the target run
        ws1_runs = tmp_path / "workspaces" / "workspace1" / "runs"
        ws1_runs.mkdir(parents=True)
        (ws1_runs / "run_a").mkdir()

        ws2_runs = tmp_path / "workspaces" / "workspace2" / "runs"
        ws2_runs.mkdir(parents=True)
        target_run = ws2_runs / "target_run"
        target_run.mkdir()

        ws3_runs = tmp_path / "workspaces" / "workspace3" / "runs"
        ws3_runs.mkdir(parents=True)
        (ws3_runs / "run_c").mkdir()

        result = find_run("target_run", base_path=tmp_path / "workspaces")

        assert result is not None
        assert result.workspace_slug == "workspace2"
        assert result.run_id == "target_run"

    def test_mixed_nested_and_top_level_workspaces(self, tmp_path):
        """Finds runs in both nested and top-level workspaces."""
        # Setup: top-level workspace with a run
        top_run = tmp_path / "workspaces" / "top_level" / "runs" / "top_run"
        top_run.mkdir(parents=True)

        # Setup: nested workspace with a run
        nested_run = (
            tmp_path / "workspaces" / "demos" / "nested" / "runs" / "nested_run"
        )
        nested_run.mkdir(parents=True)

        # Find top-level run
        result1 = find_run("top_run", base_path=tmp_path / "workspaces")
        assert result1 is not None
        assert result1.workspace_slug == "top_level"

        # Find nested run
        result2 = find_run("nested_run", base_path=tmp_path / "workspaces")
        assert result2 is not None
        assert result2.workspace_slug == "demos/nested"

    def test_runs_directory_without_target_run(self, tmp_path):
        """Workspace with runs/ but without target run returns None."""
        runs_dir = tmp_path / "workspaces" / "ws" / "runs"
        runs_dir.mkdir(parents=True)
        (runs_dir / "other_run_1").mkdir()
        (runs_dir / "other_run_2").mkdir()

        result = find_run("target_run", base_path=tmp_path / "workspaces")

        assert result is None

    def test_run_handle_properties(self, tmp_path):
        """Verify RunHandle has correct derived properties."""
        run_dir = (
            tmp_path / "workspaces" / "demos" / "test_ws" / "runs" / "test_run"
        )
        run_dir.mkdir(parents=True)

        result = find_run("test_run", base_path=tmp_path / "workspaces")

        assert result is not None
        assert result.db_path == run_dir / "state.sqlite"
        assert result.config_path == run_dir / "config.json"
        assert result.operators_path == run_dir / "operators"


class TestBackwardCompatibility:
    """Tests ensuring backward compatibility with existing usage patterns."""

    def test_load_workspace_context_single_level_compatible(
        self, tmp_path, monkeypatch
    ):
        """Existing single-level workspace slugs continue to work."""
        # This mimics the existing workspace structure
        ws_dir = tmp_path / "workspaces" / "mxene_shear_demo"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
class MockCampaign:
    name = "mxene_shear_demo"

def get_campaign():
    return MockCampaign()
'''
        )

        monkeypatch.chdir(tmp_path)

        result = load_workspace_context("mxene_shear_demo")

        assert result.name == "mxene_shear_demo"

    def test_find_run_single_level_compatible(self, tmp_path):
        """Existing single-level workspace runs are found correctly."""
        # Mimic existing structure: workspaces/battery_screening/runs/{run_id}/
        run_dir = (
            tmp_path
            / "workspaces"
            / "battery_screening"
            / "runs"
            / "20251127_021548_afaaeba1"
        )
        run_dir.mkdir(parents=True)
        (run_dir / "state.sqlite").touch()
        (run_dir / "campaign_state.json").touch()

        result = find_run(
            "20251127_021548_afaaeba1", base_path=tmp_path / "workspaces"
        )

        assert result is not None
        assert result.workspace_slug == "battery_screening"
        assert result.run_id == "20251127_021548_afaaeba1"
