from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from matterstack.config.operator_wiring import load_wiring_provenance_from_run_root
from matterstack.core.evidence import EvidenceBundle
from matterstack.core.operator_keys import resolve_operator_key_for_attempt
from matterstack.core.run import RunHandle

if TYPE_CHECKING:
    from matterstack.storage.schema import TaskAttemptModel
    from matterstack.storage.state_store import SQLiteStateStore


def _dt_to_iso(dt: Optional[object]) -> Optional[str]:
    # `TaskAttemptModel` datetime fields may be naive (utcnow) or tz-aware.
    # Evidence export only needs stable string representation.
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        return dt.isoformat()  # type: ignore[no-any-return]
    return str(dt)


def _attempt_to_dict(attempt: "TaskAttemptModel", run_root: Path) -> Dict[str, Any]:
    rel = Path(attempt.relative_path) if attempt.relative_path else None
    artifact_path = (run_root / rel) if rel else None
    artifact_missing = bool(artifact_path and not artifact_path.exists())

    resolved = resolve_operator_key_for_attempt(attempt)
    operator_key = resolved.operator_key if resolved else None

    return {
        "attempt_id": attempt.attempt_id,
        "attempt_index": attempt.attempt_index,
        "status": attempt.status,
        "status_reason": attempt.status_reason,
        "operator_type": attempt.operator_type,
        "operator_key": operator_key,
        "external_id": attempt.external_id,
        "operator_data": attempt.operator_data,
        "relative_path": str(rel) if rel else None,
        # Store as string for JSON stability (pydantic can still serialize it).
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_missing": artifact_missing,
        "created_at": _dt_to_iso(attempt.created_at),
        "submitted_at": _dt_to_iso(attempt.submitted_at),
        "ended_at": _dt_to_iso(attempt.ended_at),
    }


def build_evidence_bundle(run_handle: RunHandle, store: "SQLiteStateStore") -> EvidenceBundle:
    """
    Query the store for all run data and construct an EvidenceBundle object.
    Rebuilds evidence from scratch using DB state and filesystem verification.

    Schema v2 (attempts) is preferred. Legacy `external_runs` is used only when
    a task has zero attempts.
    """
    # 1. Fetch Full Run Metadata
    run_meta = store.get_run_metadata(run_handle.run_id)
    if not run_meta:
        raise ValueError(f"Run {run_handle.run_id} not found in store.")

    run_status = run_meta.status
    status_reason = store.get_run_status_reason(run_handle.run_id)
    is_complete = run_status == "COMPLETED"

    tasks_data: Dict[str, Dict[str, Any]] = {}
    artifacts: Dict[str, Path] = {}
    task_counts = {"total": 0, "completed": 0, "failed": 0, "cancelled": 0}

    # 2. Get all tasks
    tasks = store.get_tasks(run_handle.run_id)
    task_counts["total"] = len(tasks)

    for task in tasks:
        task_info: Dict[str, Any] = {
            "image": task.image,
            "command": task.command,
            "status": "UNKNOWN",
            # v2 attempt-aware fields (always present for schema stability)
            "attempts": [],
            "current_attempt": None,
            # v1 compatibility shim (populated only if attempts == [])
            "legacy_external_run": None,
        }

        attempts = store.list_attempts(task.task_id)

        # ---- v2 preferred: attempt-first ----
        if attempts:
            current_attempt = store.get_current_attempt(task.task_id) or attempts[-1]

            # Export full attempt history
            task_info["attempts"] = [
                _attempt_to_dict(a, run_handle.root_path) for a in attempts
            ]
            task_info["current_attempt"] = _attempt_to_dict(
                current_attempt, run_handle.root_path
            )

            # Stable task summary fields derived from current attempt
            status_val = current_attempt.status
            task_info["status"] = status_val
            task_info["operator_type"] = current_attempt.operator_type
            resolved_ok = resolve_operator_key_for_attempt(current_attempt)
            task_info["operator_key"] = resolved_ok.operator_key if resolved_ok else None
            task_info["external_id"] = current_attempt.external_id
            task_info["results"] = current_attempt.operator_data

            # Update counts from summary status
            if status_val == "COMPLETED":
                task_counts["completed"] += 1
            elif status_val == "FAILED":
                task_counts["failed"] += 1
            elif status_val == "CANCELLED":
                task_counts["cancelled"] += 1

            # Compatibility: single artifact path per task points at CURRENT attempt evidence
            if current_attempt.relative_path:
                full_path = run_handle.root_path / Path(current_attempt.relative_path)
                if full_path.exists():
                    artifacts[task.task_id] = full_path
                else:
                    task_info["artifact_missing"] = True

            tasks_data[task.task_id] = task_info
            continue

        # ---- v1 fallback: legacy external_runs only when zero attempts ----
        ext_run = store.get_external_run(task.task_id)
        if ext_run:
            status_val = ext_run.status.value
            task_info["status"] = status_val
            task_info["operator_type"] = ext_run.operator_type
            task_info["external_id"] = ext_run.external_id
            task_info["results"] = ext_run.operator_data

            legacy: Dict[str, Any] = {
                "status": status_val,
                "operator_type": ext_run.operator_type,
                "external_id": ext_run.external_id,
                "operator_data": ext_run.operator_data,
                "relative_path": str(ext_run.relative_path) if ext_run.relative_path else None,
                "artifact_path": None,
                "artifact_missing": False,
            }

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
                legacy["artifact_path"] = str(full_path)
                if full_path.exists():
                    artifacts[task.task_id] = full_path
                else:
                    legacy["artifact_missing"] = True
                    task_info["artifact_missing"] = True

            task_info["legacy_external_run"] = legacy
        else:
            # Check internal status if no external run (e.g., GateTask or pending)
            internal_status = store.get_task_status(task.task_id)
            if internal_status:
                task_info["status"] = internal_status

        tasks_data[task.task_id] = task_info

    # --- v0.2.7: run-level operator wiring provenance (best-effort, backward compatible) ---
    prov = load_wiring_provenance_from_run_root(run_handle.root_path)
    operator_wiring: Dict[str, Any]
    if prov is None:
        operator_wiring = {
            "source": "UNKNOWN",
            "sha256": None,
            "snapshot_relpath": None,
            "resolved_path": None,
            "created_at_utc": None,
        }
    else:
        operator_wiring = {
            "source": prov.source,
            "sha256": prov.sha256,
            "snapshot_relpath": prov.snapshot_relpath,
            "resolved_path": prov.resolved_path,
            "created_at_utc": prov.created_at_utc,
        }

    # Construct Bundle
    bundle = EvidenceBundle(
        run_id=run_handle.run_id,
        workspace_slug=run_handle.workspace_slug,
        run_status=run_status,
        status_reason=status_reason,
        is_complete=is_complete,
        task_counts=task_counts,
        data={"tasks": tasks_data, "operator_wiring": operator_wiring},
        artifacts=artifacts,
        tags=list(run_meta.tags.keys()) if run_meta.tags else [],
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

    # --- v0.2.7: copy operator wiring snapshot artifacts into evidence export (best-effort) ---
    snap_src_dir = run_root / "operators_snapshot"
    snap_dst_dir = evidence_dir / "operators_snapshot"
    if snap_src_dir.is_dir():
        snap_dst_dir.mkdir(parents=True, exist_ok=True)
        for name in ["operators.yaml", "metadata.json", "history.jsonl"]:
            src = snap_src_dir / name
            if src.is_file():
                shutil.copy2(src, snap_dst_dir / name)

    # 1. Export JSON
    json_path = evidence_dir / "bundle.json"
    with json_path.open("w") as f:
        f.write(bundle.model_dump_json(indent=2))
        
    # 2. Generate Markdown Report
    report_path = evidence_dir / "report.md"
    report_content = _generate_markdown_report(bundle, run_root=run_root)
    report_path.write_text(report_content)
    
    # Update bundle with report content (optional, but good for completeness if re-used)
    bundle.report_content = report_content

def _generate_markdown_report(bundle: EvidenceBundle, *, run_root: Optional[Path] = None) -> str:
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

    # --- v0.2.7: operator wiring provenance (from bundle.json data) ---
    ow = (bundle.data or {}).get("operator_wiring") if isinstance(bundle.data, dict) else None
    if isinstance(ow, dict):
        lines.append("")
        lines.append("## Operator Wiring")
        lines.append(f"- **source**: `{ow.get('source')}`")
        if ow.get("sha256"):
            lines.append(f"- **sha256**: `{ow.get('sha256')}`")
        if ow.get("snapshot_relpath"):
            lines.append(f"- **snapshot_relpath**: `{ow.get('snapshot_relpath')}`")
        if ow.get("resolved_path"):
            lines.append(f"- **resolved_path**: `{ow.get('resolved_path')}`")
        if ow.get("created_at_utc"):
            lines.append(f"- **created_at_utc**: `{ow.get('created_at_utc')}`")

        # Mention evidence copy locations (required by E2E assertions).
        if run_root is not None and (run_root / "operators_snapshot").is_dir():
            lines.append("")
            lines.append("Copied into this evidence export under:")
            lines.append("- `operators_snapshot/operators.yaml`")
            lines.append("- `operators_snapshot/metadata.json`")
            lines.append("- `operators_snapshot/history.jsonl` (if present)")

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