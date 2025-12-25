"""
Unit tests for matterstack/cli/utils.py

Tests nested workspace path support for load_workspace_context() and find_run().
Tests multi-level resolution for workspaces root discovery.
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import patch

from matterstack.cli.utils import (
    load_workspace_context,
    find_run,
    _resolve_workspaces_root,
    _find_project_root,
    _ENV_WORKSPACES_ROOT,
)
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


class TestWorkspacesRootResolution:
    """Tests for _resolve_workspaces_root() multi-level resolution."""

    def test_env_var_takes_priority(self, tmp_path, monkeypatch):
        """Environment variable overrides all other resolution methods."""
        # Setup: both pyproject.toml and local workspaces exist
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        (tmp_path / "workspaces").mkdir()
        
        # Env var points to different location
        env_workspaces = tmp_path / "custom_workspaces"
        env_workspaces.mkdir()
        
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(env_workspaces))
        monkeypatch.chdir(tmp_path)
        
        result = _resolve_workspaces_root()
        
        assert result == env_workspaces

    def test_env_var_path_returned_even_if_nonexistent(self, tmp_path, monkeypatch):
        """Env var path returned even if doesn't exist (let caller handle)."""
        nonexistent = tmp_path / "does_not_exist"
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(nonexistent))
        monkeypatch.chdir(tmp_path)
        
        result = _resolve_workspaces_root()
        
        assert result == nonexistent
        assert not result.exists()

    def test_project_root_detection_via_pyproject(self, tmp_path, monkeypatch):
        """Finds workspaces via pyproject.toml when running from subdirectory."""
        # Setup: project root with pyproject.toml and workspaces
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "pyproject.toml").write_text("[project]\nname='test'\n")
        (project_root / "workspaces").mkdir()
        
        # CWD is a subdirectory
        subdir = project_root / "src" / "module"
        subdir.mkdir(parents=True)
        
        monkeypatch.chdir(subdir)
        
        result = _resolve_workspaces_root()
        
        assert result == project_root / "workspaces"

    def test_hardcoded_fallback_from_project_root(self, tmp_path, monkeypatch):
        """Hardcoded Path('workspaces') works when at project root."""
        # No pyproject.toml, just workspaces directory
        (tmp_path / "workspaces").mkdir()
        monkeypatch.chdir(tmp_path)
        
        result = _resolve_workspaces_root()
        
        assert result == Path("workspaces")

    def test_returns_none_when_nothing_found(self, tmp_path, monkeypatch):
        """Returns None when no resolution strategy succeeds."""
        # Empty directory, no pyproject.toml, no workspaces
        monkeypatch.chdir(tmp_path)
        
        result = _resolve_workspaces_root()
        
        assert result is None


class TestFindProjectRoot:
    """Tests for _find_project_root() helper."""

    def test_finds_pyproject_in_cwd(self, tmp_path, monkeypatch):
        """Finds pyproject.toml in current directory."""
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        monkeypatch.chdir(tmp_path)
        
        result = _find_project_root()
        
        assert result == tmp_path.resolve()

    def test_finds_pyproject_in_parent(self, tmp_path, monkeypatch):
        """Finds pyproject.toml in parent directory."""
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        subdir = tmp_path / "src" / "module"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)
        
        result = _find_project_root()
        
        assert result == tmp_path.resolve()

    def test_finds_pyproject_deeply_nested(self, tmp_path, monkeypatch):
        """Finds pyproject.toml from deeply nested subdirectory."""
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        deep_dir = tmp_path / "a" / "b" / "c" / "d" / "e"
        deep_dir.mkdir(parents=True)
        monkeypatch.chdir(deep_dir)
        
        result = _find_project_root()
        
        assert result == tmp_path.resolve()

    def test_returns_none_when_no_pyproject(self, tmp_path, monkeypatch):
        """Returns None when no pyproject.toml found."""
        monkeypatch.chdir(tmp_path)
        
        result = _find_project_root()
        
        assert result is None


class TestFindRunWithResolution:
    """Tests for find_run() using automatic resolution."""

    def test_find_run_with_env_var(self, tmp_path, monkeypatch):
        """find_run() works with MATTERSTACK_WORKSPACES_ROOT set."""
        # Setup: workspaces in non-standard location
        custom_ws = tmp_path / "custom" / "workspaces"
        run_dir = custom_ws / "my_workspace" / "runs" / "test_run_123"
        run_dir.mkdir(parents=True)
        
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(custom_ws))
        monkeypatch.chdir(tmp_path)
        
        result = find_run("test_run_123")
        
        assert result is not None
        assert result.workspace_slug == "my_workspace"
        assert result.run_id == "test_run_123"

    def test_find_run_from_workspace_subdirectory(self, tmp_path, monkeypatch):
        """find_run() works when CWD is inside a workspace."""
        # Setup: project with pyproject.toml and workspaces
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        run_dir = tmp_path / "workspaces" / "my_ws" / "runs" / "run_xyz"
        run_dir.mkdir(parents=True)
        
        # CWD is inside a workspace
        workspace_subdir = tmp_path / "workspaces" / "my_ws" / "scripts"
        workspace_subdir.mkdir(parents=True)
        monkeypatch.chdir(workspace_subdir)
        
        result = find_run("run_xyz")
        
        assert result is not None
        assert result.workspace_slug == "my_ws"
        assert result.run_id == "run_xyz"

    def test_find_run_explicit_base_path_overrides(self, tmp_path, monkeypatch):
        """Explicit base_path parameter overrides all resolution."""
        # Setup: env var points to one location
        env_ws = tmp_path / "env_workspaces"
        (env_ws / "ws1" / "runs" / "env_run").mkdir(parents=True)
        
        # Explicit path points to another
        explicit_ws = tmp_path / "explicit_workspaces"
        (explicit_ws / "ws2" / "runs" / "explicit_run").mkdir(parents=True)
        
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(env_ws))
        monkeypatch.chdir(tmp_path)
        
        # Explicit base_path should win
        result = find_run("explicit_run", base_path=explicit_ws)
        
        assert result is not None
        assert result.workspace_slug == "ws2"
        assert result.run_id == "explicit_run"
        
        # Verify env_run is not found with explicit path
        result2 = find_run("env_run", base_path=explicit_ws)
        assert result2 is None

    def test_find_run_returns_none_when_cannot_resolve(self, tmp_path, monkeypatch):
        """Returns None when workspaces root cannot be resolved."""
        # Empty directory, no pyproject.toml, no workspaces
        monkeypatch.chdir(tmp_path)
        
        result = find_run("any_run")
        
        assert result is None


class TestLoadWorkspaceContextWithResolution:
    """Tests for load_workspace_context() using automatic resolution."""

    def test_load_with_env_var(self, tmp_path, monkeypatch):
        """load_workspace_context() works with env var set."""
        # Setup: workspace in custom location
        custom_ws = tmp_path / "custom" / "workspaces"
        ws_dir = custom_ws / "my_workspace"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"name": "env_workspace"}
'''
        )
        
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(custom_ws))
        monkeypatch.chdir(tmp_path)
        
        result = load_workspace_context("my_workspace")
        
        assert result == {"name": "env_workspace"}

    def test_load_from_subdirectory(self, tmp_path, monkeypatch):
        """load_workspace_context() works from subdirectory."""
        # Setup: project with pyproject.toml
        (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
        ws_dir = tmp_path / "workspaces" / "test_ws"
        ws_dir.mkdir(parents=True)
        (ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"name": "test_workspace", "from": "subdirectory"}
'''
        )
        
        # CWD is a subdirectory
        subdir = tmp_path / "src" / "module"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)
        
        result = load_workspace_context("test_ws")
        
        assert result["name"] == "test_workspace"
        assert result["from"] == "subdirectory"

    def test_clear_error_when_cannot_resolve(self, tmp_path, monkeypatch):
        """RuntimeError with helpful message when resolution fails."""
        # Empty directory, no pyproject.toml, no workspaces
        monkeypatch.chdir(tmp_path)
        
        with pytest.raises(RuntimeError) as exc_info:
            load_workspace_context("any_workspace")
        
        error_msg = str(exc_info.value)
        assert "Cannot find workspaces directory" in error_msg
        assert "MATTERSTACK_WORKSPACES_ROOT" in error_msg
        assert "pyproject.toml" in error_msg

    def test_load_explicit_base_path_overrides(self, tmp_path, monkeypatch):
        """Explicit base_path parameter overrides all resolution."""
        # Setup: env var points to one location
        env_ws = tmp_path / "env_workspaces"
        env_ws_dir = env_ws / "ws1"
        env_ws_dir.mkdir(parents=True)
        (env_ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"source": "env"}
'''
        )
        
        # Explicit path points to another
        explicit_ws = tmp_path / "explicit_workspaces"
        explicit_ws_dir = explicit_ws / "ws2"
        explicit_ws_dir.mkdir(parents=True)
        (explicit_ws_dir / "main.py").write_text(
            '''
def get_campaign():
    return {"source": "explicit"}
'''
        )
        
        monkeypatch.setenv(_ENV_WORKSPACES_ROOT, str(env_ws))
        monkeypatch.chdir(tmp_path)
        
        # Explicit base_path should win
        result = load_workspace_context("ws2", base_path=explicit_ws)
        
        assert result["source"] == "explicit"
