"""
ANALYZE phase logic for the run lifecycle.

This module contains functions for building task results, loading/persisting
campaign state, and orchestrating the analyze-replan cycle.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from matterstack.core.campaign import Campaign
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow

logger = logging.getLogger(__name__)


def build_task_results(
    tasks: List[Task],
    task_status_map: Dict[str, str],
    store: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Construct rich results dict from completed tasks.

    Retrieves attempt-scoped output metadata if available (v2 primary),
    falling back to legacy external_runs (v1).

    Args:
        tasks: List of all tasks.
        task_status_map: Mapping of task_id to status.
        store: The SQLiteStateStore instance.

    Returns:
        Dict mapping task_id to result entry with status, files, and data.
    """
    results = {}

    for t in tasks:
        status = task_status_map.get(t.task_id, "UNKNOWN")
        res_entry: Dict[str, Any] = {"status": status}

        # Retrieve attempt-scoped output metadata if available (v2 primary)
        attempt = store.get_current_attempt(t.task_id)
        if attempt is None:
            # Defensive: if attempts exist but pointer is missing, pick the latest.
            attempts = store.list_attempts(t.task_id)
            if attempts:
                attempt = attempts[-1]

        if attempt and attempt.operator_data:
            if "output_files" in attempt.operator_data:
                res_entry["files"] = attempt.operator_data["output_files"]
            if "output_data" in attempt.operator_data:
                res_entry["data"] = attempt.operator_data["output_data"]
        else:
            # Legacy fallback (v1)
            ext_run = store.get_external_run(t.task_id)
            if ext_run and ext_run.operator_data:
                if "output_files" in ext_run.operator_data:
                    res_entry["files"] = ext_run.operator_data["output_files"]
                if "output_data" in ext_run.operator_data:
                    res_entry["data"] = ext_run.operator_data["output_data"]

        results[t.task_id] = res_entry

    return results


def load_campaign_state(run_handle: RunHandle) -> Optional[Any]:
    """
    Load Campaign State from JSON file in run root.

    This is a mock persistence mechanism. The campaign.analyze() method
    expects a state object, which may be a dict or Pydantic model.

    Args:
        run_handle: The run handle.

    Returns:
        The loaded state dict, or None if not found or error.
    """
    state_file = run_handle.root_path / "campaign_state.json"

    if not state_file.exists():
        return None

    try:
        # We need to deserialize it properly.
        # Ideally, campaign.deserialize_state(json) but we don't have that interface.
        # We'll pass the dict and let Pydantic handle it if possible,
        # or rely on Campaign.analyze handling a dict (which it might not).
        # The CoatingsCampaign expects CoatingsState object.
        # We'll rely on our specific implementation knowledge for now.
        state_dict = json.loads(state_file.read_text())

        # Hack: We pass the dict. The CoatingsCampaign will need to handle dict input.
        return state_dict
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
        return None


def persist_campaign_state(run_handle: RunHandle, new_state: Any) -> None:
    """
    Persist new campaign state to JSON file.

    Supports both Pydantic models (with model_dump_json) and plain dicts.

    Args:
        run_handle: The run handle.
        new_state: The new state to persist.
    """
    if new_state is None:
        return

    state_file = run_handle.root_path / "campaign_state.json"

    # Assume it's a Pydantic model
    if hasattr(new_state, "model_dump_json"):
        state_file.write_text(new_state.model_dump_json())
    elif isinstance(new_state, dict):
        state_file.write_text(json.dumps(new_state))


def execute_analyze_phase(
    run_handle: RunHandle,
    campaign: Campaign,
    tasks: List[Task],
    task_status_map: Dict[str, str],
    store: Any,
) -> Optional[Workflow]:
    """
    Execute the ANALYZE phase: analyze results and replan.

    This orchestrates:
    1. Building task results
    2. Loading campaign state
    3. Calling campaign.analyze()
    4. Persisting new state
    5. Calling campaign.plan()

    Args:
        run_handle: The run handle.
        campaign: The campaign instance.
        tasks: List of all tasks.
        task_status_map: Mapping of task_id to status.
        store: The SQLiteStateStore instance.

    Returns:
        The new workflow from campaign.plan(), or None if campaign is done.
    """
    logger.info("Current workflow completed. Analyzing...")

    # Construct rich results dict
    results = build_task_results(tasks, task_status_map, store)

    # Load Campaign State from JSON file
    current_state = load_campaign_state(run_handle)

    # Analyze
    # Note: CoatingCampaign.analyze expects CoatingsState object or None.
    # If we pass a dict, it might fail if not handled.
    new_state = campaign.analyze(current_state, results)

    # Persist new state
    persist_campaign_state(run_handle, new_state)

    # Plan next steps
    new_workflow = campaign.plan(new_state)

    return new_workflow


__all__ = [
    "build_task_results",
    "load_campaign_state",
    "persist_campaign_state",
    "execute_analyze_phase",
]
