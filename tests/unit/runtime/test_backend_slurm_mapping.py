import pytest

from matterstack.core.backend import JobState
from matterstack.runtime.backends.hpc.slurm import _map_slurm_state


@pytest.mark.parametrize("slurm_state,expected", [
    ("PENDING", JobState.QUEUED),
    ("PD", JobState.QUEUED),
    ("REQUEUED", JobState.QUEUED),
    ("RUNNING", JobState.RUNNING),
    ("R", JobState.RUNNING),
    ("COMPLETING", JobState.RUNNING),
    ("CG", JobState.RUNNING),
    ("COMPLETED", JobState.COMPLETED_OK),
    ("CD", JobState.COMPLETED_OK),
    ("FAILED", JobState.COMPLETED_ERROR),
    ("F", JobState.COMPLETED_ERROR),
    ("TIMEOUT", JobState.COMPLETED_ERROR),
    ("TO", JobState.COMPLETED_ERROR),
    ("NODE_FAIL", JobState.COMPLETED_ERROR),
    ("NF", JobState.COMPLETED_ERROR),
    ("BOOT_FAIL", JobState.COMPLETED_ERROR),
    ("BF", JobState.COMPLETED_ERROR),
    ("OUT_OF_MEMORY", JobState.COMPLETED_ERROR),
    ("OOM", JobState.COMPLETED_ERROR),
    ("CANCELLED", JobState.CANCELLED),
    ("CANCELLED+", JobState.CANCELLED),
    ("CA", JobState.CANCELLED),
    ("UNKNOWN_GARBAGE", JobState.UNKNOWN),
])
def test_slurm_state_mapping(slurm_state, expected):
    assert _map_slurm_state(slurm_state) == expected
