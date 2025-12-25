from matterstack.core.campaign import Campaign
from matterstack.core.run import RunMetadata
from matterstack.orchestration.run_lifecycle import RunHandle, initialize_or_resume_run
from matterstack.storage.state_store import SQLiteStateStore


class MockCampaign(Campaign):
    def plan(self, state):
        return None
    def analyze(self, state, results):
        return None

def create_mock_run(base_path, workspace, run_id, status="PENDING"):
    """Helper to create a run directory and database with a specific status."""
    run_dir = base_path / workspace / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "state.sqlite"
    store = SQLiteStateStore(db_path)
    handle = RunHandle(workspace_slug=workspace, run_id=run_id, root_path=run_dir)
    store.create_run(handle, RunMetadata(status=status))
    return handle

def test_resume_no_existing_runs(tmp_path):
    """Should create a new run if no runs exist."""
    campaign = MockCampaign()
    workspace = "test_ws"

    # We expect initialize_run to be called, which returns a handle.
    # Since we are testing integration with file system mostly, we'll let it actually create it
    # OR we can mock initialize_run if we want to test just the logic.
    # Given the simplicity, let's let it run but we need to import initialize_or_resume_run first.

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path)

    assert handle.run_id is not None
    assert (tmp_path / workspace / "runs" / handle.run_id).exists()

def test_resume_active_run(tmp_path):
    """Should resume the latest run if it is active."""
    campaign = MockCampaign()
    workspace = "test_ws"

    # Create an old completed run
    create_mock_run(tmp_path, workspace, "run_1_old", "COMPLETED")

    # Create a newer active run
    create_mock_run(tmp_path, workspace, "run_2_active", "RUNNING")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path)

    assert handle.run_id == "run_2_active"

def test_resume_completed_run_starts_new(tmp_path):
    """Should start a new run if the latest run is completed."""
    campaign = MockCampaign()
    workspace = "test_ws"

    create_mock_run(tmp_path, workspace, "run_1", "COMPLETED")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path)

    assert handle.run_id != "run_1"
    assert (tmp_path / workspace / "runs" / handle.run_id).exists()

def test_resume_failed_run_starts_new(tmp_path):
    """Should start a new run if the latest run failed."""
    campaign = MockCampaign()
    workspace = "test_ws"

    create_mock_run(tmp_path, workspace, "run_1", "FAILED")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path)

    assert handle.run_id != "run_1"

def test_resume_cancelled_run_starts_new(tmp_path):
    """Should start a new run if the latest run was cancelled."""
    campaign = MockCampaign()
    workspace = "test_ws"

    create_mock_run(tmp_path, workspace, "run_1", "CANCELLED")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path)

    assert handle.run_id != "run_1"

def test_resume_specific_run_id(tmp_path):
    """Should resume a specific run ID if provided."""
    campaign = MockCampaign()
    workspace = "test_ws"

    create_mock_run(tmp_path, workspace, "run_target", "PAUSED")
    create_mock_run(tmp_path, workspace, "run_other", "RUNNING")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path, resume_run_id="run_target")

    assert handle.run_id == "run_target"

def test_resume_run_not_found(tmp_path):
    """Should raise error or create new? The spec says 'support explicit resume_run_id'. Usually if explicit fails, it's an error."""
    campaign = MockCampaign()
    workspace = "test_ws"

    # Depending on implementation preference.
    # If I ask for a specific run and it doesn't exist, I'd expect an error or a fresh run with that ID?
    # initialize_run can take a run_id to create a SPECIFIC ID.
    # So if we pass resume_run_id, it might just create it if not exists?
    # But 'resume' implies it should exist.
    # Let's assume for now it falls back to creating it if initialize_run supports custom ID.

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path, resume_run_id="run_new_specific")

    assert handle.run_id == "run_new_specific"
    # It should be a new run
    store = SQLiteStateStore(handle.db_path)
    # Check status
    assert store.get_run_status("run_new_specific") == "PENDING"

def test_resume_always_flag(tmp_path):
    """If resume_always=True, should verify behavior?
    The spec says: 'unless a resume_always flag is set, but default behavior should be start new if finished'.
    This implies we might need a flag to FORCE resumption of a completed run?
    Or maybe it means 'resume_always' means 'do not start new run if latest is terminal'?
    Re-reading spec: "create a new run (unless a resume_always flag is set...)"
    This sounds like if resume_always is True, and latest is COMPLETED, we should... return the completed run? Or error?
    Common pattern: if resume_always=True, return the handle to the existing run even if terminal.
    """
    campaign = MockCampaign()
    workspace = "test_ws"

    create_mock_run(tmp_path, workspace, "run_done", "COMPLETED")

    handle = initialize_or_resume_run(workspace, campaign, base_path=tmp_path, resume_always=True)

    assert handle.run_id == "run_done"
