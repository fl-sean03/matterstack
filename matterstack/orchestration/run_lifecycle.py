from __future__ import annotations
import logging
import json
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, List, Set, Union
from datetime import datetime, timezone

from matterstack.core.run import RunHandle, RunMetadata
from matterstack.core.campaign import Campaign
from matterstack.core.workflow import Workflow, Task
from matterstack.core.operators import ExternalRunHandle, ExternalRunStatus
from matterstack.storage.state_store import SQLiteStateStore
from matterstack.core.external import ExternalTask
from matterstack.core.gate import GateTask

logger = logging.getLogger(__name__)

class RunLifecycleError(Exception):
    pass

def initialize_run(
    workspace_slug: str,
    campaign: Campaign,
    base_path: Path = Path("workspaces"),
    run_id: Optional[str] = None
) -> RunHandle:
    """
    Initialize a new run environment.
    
    1. Resolve run ID and paths.
    2. Create directory structure.
    3. Initialize SQLite DB.
    4. Execute initial plan() and store workflow.
    """
    if run_id is None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]

    # workspace/runs/run_id/
    root_path = base_path / workspace_slug / "runs" / run_id
    
    logger.info(f"Initializing run {run_id} at {root_path}")
    
    handle = RunHandle(
        workspace_slug=workspace_slug,
        run_id=run_id,
        root_path=root_path
    )
    
    # Create directories
    handle.root_path.mkdir(parents=True, exist_ok=True)
    # handle.operators_path.mkdir(exist_ok=True) # Lazy creation preferred
    # handle.evidence_path.mkdir(exist_ok=True) # Lazy creation preferred
    
    # Initialize State Store
    store = SQLiteStateStore(handle.db_path)
    
    with store.lock():
        store.create_run(handle, RunMetadata(status="PENDING"))
        
        # Initial Plan
        # We pass None as state for the first plan, or we could pass an empty dict
        # depending on Campaign contract. Assuming None is fine for "fresh start".
        workflow = campaign.plan(state=None)
        
        if workflow:
            store.add_workflow(workflow, run_id=handle.run_id)
            logger.info(f"Initialized run {run_id} with {len(workflow.tasks)} tasks.")
        else:
            logger.info(f"Initialized run {run_id} with no initial workflow (done?).")
            # store.update_run_status(run_id, "completed") # Method not yet available
        
    return handle

def initialize_or_resume_run(
    workspace_slug: str,
    campaign: Campaign,
    base_path: Path = Path("workspaces"),
    resume_run_id: Optional[str] = None,
    resume_always: bool = False
) -> RunHandle:
    """
    Initialize a new run or resume an existing one.

    Logic:
    1. If resume_run_id is provided, try to resume that specific run.
       If it doesn't exist, initialize it (specific ID creation).
    2. If no ID provided:
       - Scan workspace/runs/ for existing runs.
       - Sort by ID (timestamp based) desc.
       - Check status of latest run.
       - If Active (PENDING, RUNNING, PAUSED) -> Resume.
       - If Terminal (COMPLETED, FAILED, CANCELLED) -> Start New (unless resume_always=True).
    
    Args:
        workspace_slug: The workspace identifier.
        campaign: The campaign to run.
        base_path: Root path for workspaces.
        resume_run_id: Explicit run ID to resume.
        resume_always: If True, will resume the latest run even if it is terminal.
    
    Returns:
        RunHandle for the active run.
    """
    runs_dir = base_path / workspace_slug / "runs"
    
    # Case 1: Explicit Run ID
    if resume_run_id:
        target_path = runs_dir / resume_run_id
        if target_path.exists():
            logger.info(f"Resuming explicit run: {resume_run_id}")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=resume_run_id,
                root_path=target_path
            )
        else:
            logger.info(f"Run {resume_run_id} not found. Creating new run with this ID.")
            return initialize_run(workspace_slug, campaign, base_path, run_id=resume_run_id)

    # Case 2: Auto-Resume Logic
    if not runs_dir.exists():
        logger.info("No runs directory found. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)
    
    # List all subdirectories
    # Assuming run_ids are sortable (e.g. YYYYMMDD_HHMMSS_uuid)
    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not run_dirs:
        logger.info("No existing runs found. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)
        
    # Sort by name descending (latest first)
    run_dirs.sort(key=lambda p: p.name, reverse=True)
    latest_run_dir = run_dirs[0]
    latest_run_id = latest_run_dir.name
    
    logger.info(f"Found latest run: {latest_run_id}")
    
    # Check Status
    try:
        store = SQLiteStateStore(latest_run_dir / "state.sqlite")
        # We need a handle to query status via helper methods, or just use direct SQL if we didn't have helper.
        # But SQLiteStateStore takes db_path.
        status = store.get_run_status(latest_run_id)
        
        if status is None:
             # Maybe DB init failed or empty? Treat as terminal/broken -> Start new?
             # Or maybe it's fresh? status defaults to None if not found.
             # If DB exists but run record missing, it's corrupted.
             logger.warning(f"Could not determine status for {latest_run_id}. Starting new run.")
             return initialize_run(workspace_slug, campaign, base_path)
             
        logger.info(f"Latest run status: {status}")
        
        active_statuses = ["PENDING", "RUNNING", "PAUSED"]
        
        if status in active_statuses:
            logger.info(f"Resuming active run {latest_run_id}")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=latest_run_id,
                root_path=latest_run_dir
            )
        elif resume_always:
            logger.info(f"Resuming terminal run {latest_run_id} (resume_always=True)")
            return RunHandle(
                workspace_slug=workspace_slug,
                run_id=latest_run_id,
                root_path=latest_run_dir
            )
        else:
            logger.info(f"Latest run is terminal ({status}). Starting new run.")
            return initialize_run(workspace_slug, campaign, base_path)
            
    except Exception as e:
        logger.warning(f"Error checking run {latest_run_id}: {e}. Starting new run.")
        return initialize_run(workspace_slug, campaign, base_path)


def step_run(
    run_handle: RunHandle,
    campaign: Campaign,
    operator_registry: Dict[str, Any] = None # Placeholder for Thrust 5/6
) -> str:
    """
    Execute one 'tick' of the run lifecycle.
    
    Returns:
        Current status of the run ("active", "completed", "failed")
    """
    store = SQLiteStateStore(run_handle.db_path)
    
    with store.lock():
        # 0. Check Run Status
        run_status = store.get_run_status(run_handle.run_id)
        if run_status == "PENDING":
             logger.info(f"Run {run_handle.run_id} started (transition from PENDING to RUNNING)")
             store.set_run_status(run_handle.run_id, "RUNNING")
             run_status = "RUNNING"
        
        if run_status in ["CANCELLED", "FAILED", "COMPLETED"]:
            logger.info(f"Run {run_handle.run_id} is {run_status}. Skipping execution.")
            return run_status
            
        if run_status == "PAUSED":
             logger.info(f"Run {run_handle.run_id} is PAUSED. Skipping EXECUTE phase.")
             # We might still want to POLL external tasks, but for now we skip everything.
             # This prevents new tasks from being submitted.
             # If we wanted to allow polling, we'd restructure this.
             return "PAUSED"

        # 1. POLL Phase: attempt-aware polling (schema v2 primary path)
        #
        # Status mapping (attempt/external status -> task status):
        # - SUBMITTED maps to WAITING_EXTERNAL for user-facing stability in v0.2.5.
        # - We do NOT create attempts for local "simulated" tasks (the branch that directly marks COMPLETED).
        # - Back-compat: we only poll legacy `external_runs` for tasks that have no attempts.
        def _task_status_from_external_status(s: ExternalRunStatus) -> str:
            if s == ExternalRunStatus.CREATED:
                return "PENDING"
            if s == ExternalRunStatus.SUBMITTED:
                return "WAITING_EXTERNAL"
            if s == ExternalRunStatus.RUNNING:
                return "RUNNING"
            if s == ExternalRunStatus.WAITING_EXTERNAL:
                return "WAITING_EXTERNAL"
            if s == ExternalRunStatus.COMPLETED:
                return "COMPLETED"
            if s == ExternalRunStatus.FAILED:
                return "FAILED"
            if s == ExternalRunStatus.CANCELLED:
                return "CANCELLED"
            return "UNKNOWN"

        # Simple Operator Registry
        from matterstack.runtime.operators.human import HumanOperator
        from matterstack.runtime.operators.experiment import ExperimentOperator
        from matterstack.runtime.operators.hpc import ComputeOperator
        from matterstack.runtime.backends.local import LocalBackend

        # Instantiate operators
        if operator_registry:
            operators = operator_registry
        else:
            local_backend = LocalBackend(workspace_root=run_handle.root_path)
            operators = {
                "Human": HumanOperator(),
                "Experiment": ExperimentOperator(),
                "Local": ComputeOperator(backend=local_backend, slug="local", operator_name="Local"),
                "HPC": ComputeOperator(backend=local_backend, slug="hpc", operator_name="HPC"),
            }

        attempt_task_ids = store.get_attempt_task_ids(run_handle.run_id)
        active_attempts = store.get_active_attempts(run_handle.run_id)

        # Poll active attempts (v2)
        for attempt in active_attempts:
            if not attempt.operator_type:
                # "stub" / incomplete attempts won't be polled; we still heal task status below.
                store.update_task_status(
                    attempt.task_id,
                    _task_status_from_external_status(ExternalRunStatus(attempt.status)),
                )
                continue

            op_type = attempt.operator_type
            if op_type not in operators:
                # Unknown operator type: skip polling but still heal task status
                store.update_task_status(
                    attempt.task_id,
                    _task_status_from_external_status(ExternalRunStatus(attempt.status)),
                )
                continue

            op = operators[op_type]
            try:
                ext_handle = ExternalRunHandle(
                    task_id=attempt.task_id,
                    operator_type=attempt.operator_type,
                    external_id=attempt.external_id,
                    status=ExternalRunStatus(attempt.status),
                    operator_data=attempt.operator_data or {},
                    relative_path=Path(attempt.relative_path)
                    if attempt.relative_path
                    else None,
                )

                old_status = ext_handle.status
                updated_handle = op.check_status(ext_handle)

                if updated_handle.status != old_status:
                    logger.info(
                        f"Attempt {attempt.attempt_id} (task {attempt.task_id}) transitioned to {updated_handle.status}"
                    )

                # If completed or failed, try to collect results (logs are important on failure)
                if updated_handle.status in [ExternalRunStatus.COMPLETED, ExternalRunStatus.FAILED]:
                    try:
                        result = op.collect_results(updated_handle)
                        if result.files:
                            files_dict = {k: str(v) for k, v in result.files.items()}
                            updated_handle.operator_data["output_files"] = files_dict
                        if result.data:
                            updated_handle.operator_data["output_data"] = result.data
                    except Exception as e:
                        logger.error(
                            f"Failed to collect results for attempt {attempt.attempt_id} (task {attempt.task_id}): {e}"
                        )

                # Persist attempt state (always, for "healing" + operator_data updates)
                store.update_attempt(
                    attempt.attempt_id,
                    status=updated_handle.status.value,
                    operator_type=updated_handle.operator_type,
                    external_id=updated_handle.external_id,
                    operator_data=updated_handle.operator_data,
                    relative_path=updated_handle.relative_path,
                )

                # Heal/sync task status from attempt status (even if unchanged)
                store.update_task_status(
                    attempt.task_id, _task_status_from_external_status(updated_handle.status)
                )

            except Exception as e:
                logger.error(
                    f"Error checking status for attempt {attempt.attempt_id} (task {attempt.task_id}): {e}"
                )

        # Poll legacy external runs (v1 fallback) ONLY for tasks that have no attempts
        active_external = store.get_active_external_runs(run_handle.run_id)
        for ext_handle in active_external:
            if ext_handle.task_id in attempt_task_ids:
                continue

            op_type = ext_handle.operator_type
            if op_type in operators:
                op = operators[op_type]
                try:
                    old_status = ext_handle.status
                    updated_handle = op.check_status(ext_handle)

                    if updated_handle.status != old_status:
                        logger.info(
                            f"Legacy External Run {ext_handle.task_id} transitioned to {updated_handle.status}"
                        )

                    if updated_handle.status in [ExternalRunStatus.COMPLETED, ExternalRunStatus.FAILED]:
                        try:
                            result = op.collect_results(updated_handle)
                            if result.files:
                                files_dict = {k: str(v) for k, v in result.files.items()}
                                updated_handle.operator_data["output_files"] = files_dict
                            if result.data:
                                updated_handle.operator_data["output_data"] = result.data
                        except Exception as e:
                            logger.error(
                                f"Failed to collect results for legacy external run {ext_handle.task_id}: {e}"
                            )

                    store.update_external_run(updated_handle)

                    # Heal/sync task status from legacy run status (SUBMITTED -> WAITING_EXTERNAL)
                    store.update_task_status(
                        ext_handle.task_id,
                        _task_status_from_external_status(updated_handle.status),
                    )

                except Exception as e:
                    logger.error(f"Error checking status for {ext_handle.task_id}: {e}")
            else:
                pass

        # 2. PLAN Phase: Check dependencies and find ready tasks
        tasks = store.get_tasks(run_handle.run_id)
    
        # Map task_id -> status
        task_status_map = {t.task_id: store.get_task_status(t.task_id) for t in tasks}
        
        # Calculate stats for logging
        stats = {
            "total": len(tasks),
            "completed": 0,
            "failed": 0,
            "active": 0,
            "ready": 0,
            "submitted": 0
        }

        # Identify tasks that are ready to run
        # Ready = Created (None/PENDING) AND All dependencies are COMPLETED
        tasks_to_run = []
        
        has_active_tasks = False
        has_failed_tasks = False
        
        for task in tasks:
            current_status = task_status_map.get(task.task_id)
            
            if current_status in ["COMPLETED", "SKIPPED"]:
                stats["completed"] += 1
                continue
                
            if current_status in ["FAILED", "CANCELLED"]:
                if task.allow_failure:
                    # Allow run to proceed even if this task failed
                    pass
                else:
                    has_failed_tasks = True
                stats["failed"] += 1
                continue
                
            if current_status in ["RUNNING", "SUBMITTED", "WAITING_EXTERNAL"]:
                has_active_tasks = True
                stats["active"] += 1
                continue

            # If status is still PENDING but there is an active attempt, don't resubmit
            if task.task_id in {a.task_id for a in store.get_active_attempts(run_handle.run_id)}:
                has_active_tasks = True
                stats["active"] += 1
                continue

            # Status is None or PENDING

            # Check dependencies
            deps_met = True
            for dep_id in task.dependencies:
                dep_status = task_status_map.get(dep_id)
                if dep_status != "COMPLETED":
                    # Check if we should allow failure
                    # Note: We need to access the task object to check allow_dependency_failure
                    # but 'tasks' is a list of Task objects which have that field.
                    # However, get_tasks returns re-hydrated objects.
                    # Let's assume strict dependency for now unless we look up the dep task object
                    deps_met = False
                    break
            
            if deps_met:
                tasks_to_run.append(task)
                stats["ready"] += 1
            else:
                # Still waiting on deps
                has_active_tasks = True

        # Update submitted count based on what we are about to do
        stats["submitted"] = len(tasks_to_run)

        # Log Tick Summary
        logger.info(
            f"Tick Summary: "
            f"Ready={stats['ready']}, "
            f"Submitted={stats['submitted']}, "
            f"Completed={stats['completed']}, "
            f"Failed={stats['failed']}, "
            f"Active={stats['active']}"
        )

        # 3. EXECUTE Phase
        
        # Determine concurrency limit
        max_hpc_jobs = 10
        config_path = run_handle.root_path / "config.json"
        logger.info(f"Checking for config at: {config_path}")
        if config_path.exists():
            logger.info(f"Found config.json at {config_path}")
            try:
                import json
                cfg = json.loads(config_path.read_text())
                max_hpc_jobs = cfg.get("max_hpc_jobs_per_run", 10)
            except Exception as e:
                logger.warning(f"Failed to read config.json, using default limit: {e}")

        # Count active executions for concurrency (attempt-aware)
        # We consider SUBMITTED/RUNNING/WAITING_EXTERNAL as occupying a slot.
        active_external_count = 0

        attempt_task_ids = store.get_attempt_task_ids(run_handle.run_id)
        active_attempts_for_slots = store.get_active_attempts(run_handle.run_id)
        for a in active_attempts_for_slots:
            if a.status in [
                ExternalRunStatus.SUBMITTED.value,
                ExternalRunStatus.RUNNING.value,
                ExternalRunStatus.WAITING_EXTERNAL.value,
            ]:
                active_external_count += 1

        # Legacy external_runs count ONLY for tasks that have no attempts
        active_external = store.get_active_external_runs(run_handle.run_id)
        for ext in active_external:
            if ext.task_id in attempt_task_ids:
                continue
            if ext.status in [
                ExternalRunStatus.RUNNING,
                ExternalRunStatus.WAITING_EXTERNAL,
                ExternalRunStatus.SUBMITTED,
            ]:
                active_external_count += 1

        slots_available = max_hpc_jobs - active_external_count
        if slots_available < 0:
            slots_available = 0
            
        logger.info(f"Concurrency Check: Active={active_external_count}, Limit={max_hpc_jobs}, Slots={slots_available}")

        # Submit ready tasks (up to limit)
        for task in tasks_to_run:
            # Check environment for explicit operator request
            explicit_operator = task.env.get("MATTERSTACK_OPERATOR")
            
            # Check config for default execution mode
            default_mode = "Simulation" # Default changed to explicit Simulation
            if config_path.exists():
                try:
                    import json
                    cfg = json.loads(config_path.read_text())
                    default_mode = cfg.get("execution_mode", "Simulation")
                    logger.info(f"Default execution mode: {default_mode}")
                except:
                    pass
            
            # Determine effective operator
            # Priority: Task Env > Task Type > Config Default
            operator_type = None
            
            if explicit_operator:
                operator_type = explicit_operator
            elif isinstance(task, GateTask):
                operator_type = "Human" # GateTask maps to HumanOperator
            elif isinstance(task, ExternalTask):
                 pass
            elif default_mode == "HPC":
                operator_type = "HPC"
            elif default_mode == "Local":
                operator_type = "Local"
            
            # Apply concurrency limit if it's an external run (Operator)
            is_external = operator_type is not None or isinstance(task, (ExternalTask, GateTask))
            
            if is_external:
                if slots_available <= 0:
                    logger.info(f"Concurrency limit reached ({max_hpc_jobs}). Postponing task {task.task_id}")
                    continue
                slots_available -= 1

            logger.info(f"Submitting task {task.task_id}")

            if operator_type:
                # v2: Create attempt first, then dispatch to operator
                logger.info(f"Dispatching to Operator: {operator_type}")

                if operator_type in operators:
                    op = operators[operator_type]
                    attempt_id: Optional[str] = None
                    try:
                        attempt_id = store.create_attempt(
                            run_id=run_handle.run_id,
                            task_id=task.task_id,
                            operator_type=operator_type,
                            status=ExternalRunStatus.CREATED.value,
                            operator_data={},
                            relative_path=None,
                        )

                        # 1. Prepare (operator directory, manifests, etc.)
                        ext_handle = op.prepare_run(run_handle, task)
                        store.update_attempt(
                            attempt_id,
                            status=ext_handle.status.value,
                            operator_type=ext_handle.operator_type,
                            operator_data=ext_handle.operator_data,
                            relative_path=ext_handle.relative_path,
                        )

                        # 2. Submit
                        ext_handle = op.submit(ext_handle)
                        store.update_attempt(
                            attempt_id,
                            status=ext_handle.status.value,
                            operator_type=ext_handle.operator_type,
                            external_id=ext_handle.external_id,
                            operator_data=ext_handle.operator_data,
                            relative_path=ext_handle.relative_path,
                        )

                        # Update Task Status (SUBMITTED -> WAITING_EXTERNAL)
                        if ext_handle.status == ExternalRunStatus.SUBMITTED:
                            store.update_task_status(task.task_id, "WAITING_EXTERNAL")
                        else:
                            store.update_task_status(
                                task.task_id,
                                "WAITING_EXTERNAL"
                                if ext_handle.status == ExternalRunStatus.WAITING_EXTERNAL
                                else ext_handle.status.value,
                            )
                        has_active_tasks = True

                    except Exception as e:
                        logger.error(f"Failed to dispatch operator {operator_type}: {e}")
                        import traceback

                        traceback.print_exc()
                        if attempt_id is not None:
                            try:
                                store.update_attempt(
                                    attempt_id,
                                    status=ExternalRunStatus.FAILED.value,
                                    operator_data={"error": str(e)},
                                    status_reason=str(e),
                                )
                            except Exception:
                                pass
                        store.update_task_status(task.task_id, "FAILED")
                else:
                    logger.error(f"Unknown operator type requested: {operator_type}")
                    store.update_task_status(task.task_id, "FAILED")

            elif isinstance(task, (ExternalTask, GateTask)):
                # Attempt-aware placeholder for legacy "external coordination" tasks.
                # No operator execution yet; we record a WAITING_EXTERNAL attempt for provenance/idempotency.
                attempt_id = store.create_attempt(
                    run_id=run_handle.run_id,
                    task_id=task.task_id,
                    operator_type="stub",
                    status=ExternalRunStatus.WAITING_EXTERNAL.value,
                    operator_data={},
                    relative_path=None,
                )
                store.update_task_status(task.task_id, "WAITING_EXTERNAL")
                has_active_tasks = True

            else:
                # Local Compute Task - SIMULATION MODE for Verification (no attempt record)
                logger.info(f"Simulating Local execution for {task.task_id}")
                store.update_task_status(task.task_id, "COMPLETED")

        # 4. ANALYZE Phase
        # Check if workflow is complete (no active tasks, no pending tasks)
        # If complete, run campaign.analyze() -> plan()
        
        if not has_active_tasks and not tasks_to_run:
            # All tasks are terminal.
            # Check for failures
            if has_failed_tasks:
                logger.error("Workflow has failed tasks. Stopping.")
                store.set_run_status(run_handle.run_id, "FAILED", reason="Workflow tasks failed")
                return "FAILED"
                
            # All completed successfully?
            # Re-fetch tasks to be sure we have latest status
            # Actually we iterated all tasks above.
            # If we reached here, all tasks are COMPLETED/SKIPPED/FAILED.
            # Since has_failed_tasks is False, all must be COMPLETED/SKIPPED.
            
            logger.info("Current workflow completed. Analyzing...")
            
            # Construct rich results dict
            results = {}
            for t in tasks:
                status = task_status_map.get(t.task_id, "UNKNOWN")
                res_entry = {"status": status}
                
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
            
            # Load Campaign State from JSON file in run root (Mock Persistence)
            state_file = run_handle.root_path / "campaign_state.json"
            current_state = None
            
            if state_file.exists():
                try:
                    # We need to deserialize it properly.
                    # Ideally, campaign.deserialize_state(json) but we don't have that interface.
                    # We'll pass the dict and let Pydantic handle it if possible,
                    # or rely on Campaign.analyze handling a dict (which it might not).
                    # The CoatingsCampaign expects CoatingsState object.
                    # We'll rely on our specific implementation knowledge for now.
                    import json
                    state_dict = json.loads(state_file.read_text())
                    
                    # Dynamic import to get the class? Or assume campaign handles dict?
                    # Let's assume campaign.analyze can take the dict if we modify it,
                    # OR we try to instantiate CoatingsState here if we knew the type.
                    # But we are in generic orchestrator.
                    
                    # Hack: We pass the dict. The CoatingsCampaign will need to handle dict input.
                    # I will modify CoatingsCampaign to accept dict.
                    current_state = state_dict
                except Exception as e:
                    logger.error(f"Failed to load state: {e}")

            # Analyze
            # Note: CoatingCampaign.analyze expects CoatingsState object or None.
            # If we pass a dict, it might fail if not handled.
            # I will update CoatingsCampaign to handle dict.
            new_state = campaign.analyze(current_state, results)
            
            # Persist new state
            if new_state:
                # Assume it's a Pydantic model
                if hasattr(new_state, "model_dump_json"):
                    state_file.write_text(new_state.model_dump_json())
                elif isinstance(new_state, dict):
                    import json
                    state_file.write_text(json.dumps(new_state))

            # Plan next steps
            new_workflow = campaign.plan(new_state)
            
            if new_workflow:
                logger.info(f"Campaign generated new workflow with {len(new_workflow.tasks)} tasks.")
                store.add_workflow(new_workflow, run_handle.run_id)
                return "RUNNING"
            else:
                logger.info("Campaign has no further work. Run Completed.")
                store.set_run_status(run_handle.run_id, "COMPLETED")
                return "COMPLETED"

        return "RUNNING"

def run_until_completion(
    run_handle: RunHandle,
    campaign: Campaign,
    poll_interval: float = 1.0,
    *,
    operator_registry: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Execute the campaign loop locally until the run is completed, failed, or cancelled.

    This function blocks until the run reaches a terminal state.
    It handles:
    - Calling step_run() repeatedly.
    - Waiting if the run is PAUSED.
    - Retrying if the run is locked by another process (graceful contention).

    Args:
        run_handle: The handle to the run.
        campaign: The campaign instance.
        poll_interval: Time to wait between ticks (seconds).
        operator_registry: Optional operator registry passed through to step_run().

    Returns:
        The final status of the run.
    """
    import time

    logger.info(f"Starting local execution loop for run {run_handle.run_id}")

    while True:
        try:
            status = step_run(run_handle, campaign, operator_registry=operator_registry)

            if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                logger.info(f"Run {run_handle.run_id} finished with status: {status}")
                return status

            if status == "PAUSED":
                logger.info(f"Run {run_handle.run_id} is PAUSED. Waiting...")
                time.sleep(5)
                continue

        except RuntimeError as re:
            if "Could not acquire lock" in str(re):
                logger.warning(
                    f"Run {run_handle.run_id} is locked by another process. Retrying..."
                )
                time.sleep(1)
                continue
            else:
                raise re
        except Exception as e:
            logger.error(f"Error in execution loop: {e}")
            raise

        time.sleep(poll_interval)

def list_active_runs(base_path: Path = Path("workspaces")) -> List[RunHandle]:
    """
    Scan for runs that are active (PENDING, RUNNING, PAUSED).
    
    Iterates through all workspaces in base_path and their runs.
    """
    active_runs = []
    
    if not base_path.exists():
        return active_runs
        
    # Iterate workspaces
    for ws_dir in base_path.iterdir():
        if not ws_dir.is_dir():
            continue
            
        runs_dir = ws_dir / "runs"
        if not runs_dir.exists():
            continue
            
        # Iterate runs
        for run_dir in runs_dir.iterdir():
            if not run_dir.is_dir():
                continue
                
            db_path = run_dir / "state.sqlite"
            if not db_path.exists():
                continue
                
            try:
                # Construct RunHandle
                run_id = run_dir.name
                handle = RunHandle(
                    workspace_slug=ws_dir.name,
                    run_id=run_id,
                    root_path=run_dir
                )
                
                # Check status
                # Note: We create a new store instance for each check.
                # This is lightweight enough for discovery.
                store = SQLiteStateStore(handle.db_path)
                status = store.get_run_status(run_id)
                
                if status in ["PENDING", "RUNNING", "PAUSED"]:
                    active_runs.append(handle)
                    
            except Exception as e:
                logger.warning(f"Failed to inspect run at {run_dir}: {e}")
                continue
                
    return active_runs