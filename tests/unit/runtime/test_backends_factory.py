
import pytest
import yaml

from matterstack.runtime.backends import create_backend_from_profile
from matterstack.runtime.backends.hpc.backend import SlurmBackend
from matterstack.runtime.backends.local import LocalBackend


@pytest.fixture
def mock_config(tmp_path):
    config_data = {
        "profiles": {
            "test_local": {
                "type": "local",
                "workspace_root": str(tmp_path / "local_ws"),
                "dry_run": True
            },
            "test_slurm": {
                "type": "slurm",
                "workspace_root": str(tmp_path / "slurm_ws"),
                "ssh": {
                    "host": "login.cluster.edu",
                    "user": "user",
                    "key_path": "/path/to/key"
                },
                "slurm": {
                    "partition": "debug"
                }
            }
        }
    }

    config_file = tmp_path / "matterstack.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    return str(config_file)

def test_create_local_backend(mock_config):
    backend = create_backend_from_profile("test_local", config_path=mock_config)

    assert isinstance(backend, LocalBackend)
    assert backend.dry_run is True
    # Check if workspace_root ends with "local_ws" (path handling might vary slightly)
    assert str(backend.workspace_root).endswith("local_ws")

def test_create_slurm_backend(mock_config):
    backend = create_backend_from_profile("test_slurm", config_path=mock_config)

    assert isinstance(backend, SlurmBackend)
    assert backend.ssh_config.host == "login.cluster.edu"
    assert backend.slurm_config["partition"] == "debug"

def test_create_backend_missing_profile(mock_config):
    with pytest.raises(KeyError, match="Profile 'non_existent' not found"):
        create_backend_from_profile("non_existent", config_path=mock_config)

def test_create_backend_invalid_config(tmp_path):
    # Test with a config file that has an unknown type
    config_data = {
        "profiles": {
            "invalid_type": {
                "type": "unknown_backend"
            }
        }
    }
    config_file = tmp_path / "invalid.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValueError, match="Unknown profile type"):
        create_backend_from_profile("invalid_type", config_path=str(config_file))
