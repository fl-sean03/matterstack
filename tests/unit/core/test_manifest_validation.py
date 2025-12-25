
import pytest
from pydantic import ValidationError

from matterstack.runtime.manifests import (
    ExperimentResultManifest,
    ExternalStatus,
    HumanResponseManifest,
    ManualHPCStatusManifest,
)


def test_human_response_valid():
    data = {"status": "COMPLETED", "data": {"foo": "bar"}}
    m = HumanResponseManifest.model_validate(data)
    assert m.status == ExternalStatus.COMPLETED
    assert m.data["foo"] == "bar"

def test_human_response_invalid_status():
    data = {"status": "UNKNOWN_STATUS"}
    with pytest.raises(ValidationError):
        HumanResponseManifest.model_validate(data)

def test_manual_hpc_status_missing():
    data = {"error": "oops"}
    with pytest.raises(ValidationError):
        ManualHPCStatusManifest.model_validate(data)

def test_experiment_result_extra_fields():
    # Pydantic ignores extra fields by default, so this should pass
    data = {"status": "FAILED", "extra": 123}
    m = ExperimentResultManifest.model_validate(data)
    assert m.status == ExternalStatus.FAILED

def test_integration_human_operator_validation():
    # Test JSON parsing
    json_str = '{"status": "COMPLETED", "data": {"x": 1}}'
    m = HumanResponseManifest.model_validate_json(json_str)
    assert m.status == ExternalStatus.COMPLETED

    # Test invalid type for data
    bad_json = '{"status": "COMPLETED", "data": "should be dict"}'
    with pytest.raises(ValidationError):
        HumanResponseManifest.model_validate_json(bad_json)
