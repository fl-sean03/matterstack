from __future__ import annotations
import json
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Any

from matterstack.core.evidence import EvidenceBundle
from matterstack.core.run import RunHandle

if TYPE_CHECKING:
    from matterstack.storage.state_store import SQLiteStateStore

def build_evidence_bundle(run_handle: RunHandle, store: SQLiteStateStore) -> EvidenceBundle:
    """
    Query the store for all run data and construct an EvidenceBundle object.
    Rebuilds evidence from scratch using DB state and filesystem verification.
    
    Args:
        run_handle: Handle for the run to export.
        store: StateStore instance containing run data.
        
    Returns:
        Populated EvidenceBundle.
    """
    # 1. Fetch Full Run Metadata
    run_meta = store.get_run_metadata(run_handle.run_id)
    if not run_meta:
        raise ValueError(f"Run {run_handle.run_id} not found in store.")

    run_status = run_meta.status
    status_reason = store.get_run_status_reason(run_handle.run_id)
    is_complete = (run_status == "COMPLETED")

    tasks_data = {}
    artifacts = {}
    task_counts = {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0
    }
    
    # 2. Get all tasks
    tasks = store.get_tasks(run_handle.run_id)
    task_counts["total"] = len(tasks)
    
    for task in tasks:
        task_info: Dict[str, Any] = {
            "image": task.image,
            "command": task.command,
            "status": "UNKNOWN"
        }
        
        # 3. Get external run data (status, results)
        ext_run = store.get_external_run(task.task_id)
        
        if ext_run:
            status_val = ext_run.status.value
            task_info["status"] = status_val
            task_info["operator_type"] = ext_run.operator_type
            task_info["external_id"] = ext_run.external_id
            task_info["results"] = ext_run.operator_data
            
            # Update counts
            if status_val == "COMPLETED":
                task_counts["completed"] += 1
            elif status_val == "FAILED":
                task_counts["failed"] += 1
            elif status_val == "CANCELLED":
                task_counts["cancelled"] += 1
            
            # Verify Artifacts
            if ext_run.relative_path:
                full_path = run_handle.root_path / ext_run.relative_path
                if full_path.exists():
                    artifacts[task.task_id] = full_path
                else:
                    # Log warning but don't fail export?
                    # Or mark artifact as missing in data?
                    task_info["artifact_missing"] = True
        else:
            # Check internal status if no external run (e.g., GateTask or pending)
            internal_status = store.get_task_status(task.task_id)
            if internal_status:
                task_info["status"] = internal_status
        
        tasks_data[task.task_id] = task_info
        
    # Construct Bundle
    bundle = EvidenceBundle(
        run_id=run_handle.run_id,
        workspace_slug=run_handle.workspace_slug,
        run_status=run_status,
        status_reason=status_reason,
        is_complete=is_complete,
        task_counts=task_counts,
        data={"tasks": tasks_data},
        artifacts=artifacts,
        tags=list(run_meta.tags.keys()) if run_meta.tags else []
    )
    
    return bundle

def export_evidence_bundle(bundle: EvidenceBundle, run_root: Path) -> None:
    """
    Serialize the bundle to evidence/bundle.json and generate a evidence/report.md.
    
    Args:
        bundle: The EvidenceBundle to export.
        run_root: Root directory of the run.
    """
    evidence_dir = run_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Export JSON
    json_path = evidence_dir / "bundle.json"
    with json_path.open("w") as f:
        f.write(bundle.model_dump_json(indent=2))
        
    # 2. Generate Markdown Report
    report_path = evidence_dir / "report.md"
    report_content = _generate_markdown_report(bundle)
    report_path.write_text(report_content)
    
    # Update bundle with report content (optional, but good for completeness if re-used)
    bundle.report_content = report_content

def _generate_markdown_report(bundle: EvidenceBundle) -> str:
    """Helper to generate MD content."""
    lines = []
    lines.append(f"# Evidence Report: Run {bundle.run_id}")
    lines.append(f"**Workspace:** {bundle.workspace_slug}")
    lines.append(f"**Generated At:** {bundle.generated_at.isoformat()}")
    
    # Run Status Header
    status_icon = "✅" if bundle.is_complete else "❌" if bundle.run_status == "FAILED" else "⚠️"
    lines.append(f"**Status:** {status_icon} {bundle.run_status}")
    if bundle.status_reason:
        lines.append(f"**Reason:** {bundle.status_reason}")
        
    # Stats
    counts = bundle.task_counts
    lines.append(f"**Progress:** {counts.get('completed', 0)}/{counts.get('total', 0)} Tasks Completed ({counts.get('failed', 0)} Failed)")
    lines.append("")
    
    lines.append("## Tasks Summary")
    tasks = bundle.data.get("tasks", {})
    
    if not tasks:
        lines.append("_No tasks found._")
    else:
        # Table Header
        lines.append("| Task ID | Status | Operator | Results |")
        lines.append("|---|---|---|---|")
        
        for task_id, info in tasks.items():
            status = info.get("status", "UNKNOWN")
            op_type = info.get("operator_type", "-")
            
            # Format simple results string
            results = info.get("results", {})
            results_str = ", ".join(f"{k}={v}" for k,v in results.items()) if results else "-"
            # Truncate if too long
            if len(results_str) > 50:
                results_str = results_str[:47] + "..."
                
            lines.append(f"| {task_id} | {status} | {op_type} | {results_str} |")
            
    lines.append("")
    lines.append("## Artifacts")
    if not bundle.artifacts:
        lines.append("_No artifacts registered._")
    else:
        for key, path in bundle.artifacts.items():
            lines.append(f"- **{key}**: `{path}`")
            
    return "\n".join(lines)