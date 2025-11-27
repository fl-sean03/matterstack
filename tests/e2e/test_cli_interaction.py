import pytest
import sys
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

from matterstack.cli.main import main, find_run, load_workspace_context
from matterstack.core.run import RunHandle

# Mock data
WORKSPACE_SLUG = "test_workspace"
RUN_ID = "test_run_123"

@pytest.fixture
def mock_workspace(tmp_path):
    """
    Sets up a mock workspace directory structure and main.py
    """
    ws_path = tmp_path / "workspaces" / WORKSPACE_SLUG
    ws_path.mkdir(parents=True)
    
    # Create a mock main.py
    main_py = ws_path / "main.py"
    main_py.write_text("""
class MockCampaign:
    pass

def get_campaign():
    return MockCampaign()
""")
    
    return tmp_path

@pytest.fixture
def mock_run_dir(mock_workspace):
    """
    Creates a mock run directory inside the workspace
    """
    run_dir = mock_workspace / "workspaces" / WORKSPACE_SLUG / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    
    # Create dummy db file so validation passes if needed
    (run_dir / "state.sqlite").touch()
    
    return run_dir

def test_load_workspace_context(mock_workspace):
    """
    Test dynamic loading of workspace module
    """
    # The CLI uses Path("workspaces") to find the workspace directory.
    # When we patch Path to return mock_workspace, Path("workspaces") becomes mock_workspace / "workspaces"
    # which is what we want because mock_workspace is the tmp_path containing the workspaces dir.
    # HOWEVER, Path("workspaces") / slug / "main.py"
    # -> mock_workspace / "workspaces" / slug / "main.py"
    # The issue is likely that mock_workspace ALREADY contains "workspaces" dir as created in the fixture.
    
    # Let's inspect what's happening.
    # The CLI code: Path("workspaces") / workspace_slug / "main.py"
    # If we patch Path to return mock_workspace (which is a Path object),
    # Path("workspaces") call returns mock_workspace? No, Path("workspaces") instantiates a Path object.
    
    # `return_value=mock_workspace` means calling `Path(...)` returns `mock_workspace`.
    # So `Path("workspaces")` -> `mock_workspace`.
    # `mock_workspace` is `/tmp/...`
    # CLI does: `mock_workspace` / `workspace_slug` / "main.py"
    # -> `/tmp/.../test_workspace/main.py`
    
    # BUT my fixture created:
    # ws_path = tmp_path / "workspaces" / WORKSPACE_SLUG
    # So the file is at `/tmp/.../workspaces/test_workspace/main.py`
    
    # So if Path("workspaces") returns `/tmp/...`, then we are looking at `/tmp/.../test_workspace/main.py`
    # But file is at `/tmp/.../workspaces/test_workspace/main.py`
    
    # So I should mock Path such that Path("workspaces") returns mock_workspace / "workspaces"
    
    with patch("matterstack.cli.main.Path") as MockPath:
        # When Path("workspaces") is called, return the correct path
        def side_effect(arg=None):
            if arg == "workspaces":
                return mock_workspace / "workspaces"
            return Path(arg) if arg else Path(".")
            
        MockPath.side_effect = side_effect
        
        campaign = load_workspace_context(WORKSPACE_SLUG)
        assert campaign.__class__.__name__ == "MockCampaign"

def test_find_run(mock_run_dir, mock_workspace):
    """
    Test finding a run directory
    """
    # Because find_run iterates over "workspaces" in current dir,
    # we need to pass the base_path to it, but the CLI hardcodes "workspaces" mostly.
    # However, find_run takes a base_path arg.
    
    base_path = mock_workspace / "workspaces"
    
    handle = find_run(RUN_ID, base_path=base_path)
    assert handle is not None
    assert handle.run_id == RUN_ID
    assert handle.workspace_slug == WORKSPACE_SLUG
    assert handle.root_path == mock_run_dir

def test_cli_init_command(mock_workspace):
    """
    Test 'init' command calls initialize_run
    """
    with patch("matterstack.cli.main.initialize_run") as mock_init:
        # Mock RunHandle return
        mock_handle = RunHandle(
            workspace_slug=WORKSPACE_SLUG,
            run_id=RUN_ID,
            root_path=Path("/tmp/fake")
        )
        mock_init.return_value = mock_handle
        
        # Patch Path so it finds our mock workspace
        with patch("matterstack.cli.main.Path") as MockPath:
            # We need MockPath("workspaces") to return mock_workspace/workspaces
            # This is getting tricky because of how we used Path in the code.
            # Let's just mock load_workspace_context to avoid filesystem issues
            
            with patch("matterstack.cli.main.load_workspace_context") as mock_load:
                mock_load.return_value = MagicMock()
                
                # Execute CLI
                test_args = ["main.py", "init", WORKSPACE_SLUG]
                with patch.object(sys, 'argv', test_args):
                     main()
                
                mock_load.assert_called_once_with(WORKSPACE_SLUG)
                mock_init.assert_called_once()

def test_cli_step_command(mock_run_dir, mock_workspace):
    """
    Test 'step' command calls step_run
    """
    with patch("matterstack.cli.main.step_run") as mock_step:
        mock_step.return_value = "active"
        
        # We need to patch find_run to return a handle because of the hardcoded path
        with patch("matterstack.cli.main.find_run") as mock_find:
            mock_find.return_value = RunHandle(
                workspace_slug=WORKSPACE_SLUG,
                run_id=RUN_ID,
                root_path=mock_run_dir
            )
            
            with patch("matterstack.cli.main.load_workspace_context") as mock_load:
                 mock_load.return_value = MagicMock()
                 
                 test_args = ["main.py", "step", RUN_ID]
                 with patch.object(sys, 'argv', test_args):
                     main()
                     
                 mock_step.assert_called_once()

def test_cli_status_command(mock_run_dir, mock_workspace):
    """
    Test 'status' command prints summary
    """
    # We need to mock SQLiteStateStore
    with patch("matterstack.cli.main.SQLiteStateStore") as MockStore:
        mock_store_instance = MockStore.return_value
        
        # Mock get_tasks
        mock_task = MagicMock()
        mock_task.task_id = "task1"
        mock_store_instance.get_tasks.return_value = [mock_task]
        mock_store_instance.get_task_status.return_value = "COMPLETED"
        
        with patch("matterstack.cli.main.find_run") as mock_find:
            mock_find.return_value = RunHandle(
                workspace_slug=WORKSPACE_SLUG,
                run_id=RUN_ID,
                root_path=mock_run_dir
            )
            
            test_args = ["main.py", "status", RUN_ID]
            with patch.object(sys, 'argv', test_args):
                # We can verify it runs without error, verifying output is harder without capturing stdout
                # but valid for now.
                main()
                
            mock_store_instance.get_tasks.assert_called_once_with(RUN_ID)

def test_cli_loop_command(mock_run_dir):
    """
    Test 'loop' command loops until completion
    """
    # Mock run_until_completion to avoid infinite loop logic in CLI if mocks fail
    with patch("matterstack.cli.main.run_until_completion") as mock_loop:
        mock_loop.return_value = "COMPLETED"
        
        with patch("matterstack.cli.main.find_run") as mock_find:
            mock_find.return_value = RunHandle(
                workspace_slug=WORKSPACE_SLUG,
                run_id=RUN_ID,
                root_path=mock_run_dir
            )
            
            with patch("matterstack.cli.main.load_workspace_context") as mock_load:
                 mock_campaign = MagicMock()
                 mock_load.return_value = mock_campaign
                 
                 test_args = ["main.py", "loop", RUN_ID]
                 with patch.object(sys, 'argv', test_args):
                     # Patch time.sleep to run fast
                     with patch("time.sleep"):
                         main()
                     
                 mock_loop.assert_called_once()