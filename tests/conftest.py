
import pytest

from matterstack.core.campaign import Campaign
from matterstack.orchestration.run_lifecycle import initialize_run
from tests.unit.runtime.hpc_mocks import MockSSHClient


class SimpleMockCampaign(Campaign):
    """A minimal mock campaign for testing initialization."""
    def plan(self, state):
        return None

    def analyze(self, state, results):
        return {}

@pytest.fixture
def temp_run_handle(tmp_path):
    """
    Creates a temporary run environment and returns a RunHandle.
    Uses a minimal MockCampaign to satisfy initialization requirements.
    """
    workspace = "test_workspace"
    campaign = SimpleMockCampaign()
    # Unique run ID to avoid collisions if multiple tests share tmp_path (though tmp_path is usually unique per test)
    # But initialize_run might generate its own ID or we might need to pass one?
    # Let's check initialize_run signature.
    # In test_run_lifecycle_basic.py: initialize_run(workspace, campaign, base_path=tmp_path)
    # It seems to generate a run_id internally or use a default.

    run_handle = initialize_run(workspace, campaign, base_path=tmp_path)
    return run_handle

@pytest.fixture
def mock_ssh():
    """Returns a MockSSHClient class or instance factory if needed."""
    return MockSSHClient

@pytest.fixture
async def mock_ssh_client():
    """Returns an instance of MockSSHClient."""
    client = MockSSHClient()
    yield client
    await client.close()
