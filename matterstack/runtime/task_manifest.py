from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping

from matterstack.core.workflow import FileFromContent, FileFromPath, Task

TASK_MANIFEST_SCHEMA_VERSION = 2


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_ref_for_task_file(dest_path: str, value: Any) -> Dict[str, Any]:
    """
    Convert a Task.files entry into a lean reference-only representation.

    IMPORTANT:
    - This is for persistence/debug manifests only (manifest.json). It MUST NOT affect runtime behavior.
    - We intentionally DO NOT embed full file contents.
    - We only include sha256 when it can be computed cheaply from already-in-memory bytes (inline content).
    """
    ref: Dict[str, Any] = {"path": dest_path}

    # Inline content (cheap to hash because we already have the bytes in-memory)
    if isinstance(value, FileFromContent):
        data = value.content.encode("utf-8")
        ref["bytes"] = len(data)
        ref["sha256"] = _sha256_bytes(data)
        ref["source"] = "inline"
        return ref

    if isinstance(value, str):
        data = value.encode("utf-8")
        ref["bytes"] = len(data)
        ref["sha256"] = _sha256_bytes(data)
        ref["source"] = "inline"
        return ref

    # Local file path sources (size can be obtained cheaply via stat; sha256 is omitted)
    src_path: Path | None = None
    if isinstance(value, FileFromPath):
        src_path = value.source_path
        ref["source"] = "local_path"
    elif isinstance(value, Path):
        src_path = value
        ref["source"] = "local_path"

    if src_path is not None:
        try:
            st = src_path.stat()
            ref["bytes"] = int(st.st_size)
        except Exception:
            # Best-effort only: do not fail manifest generation if stat fails.
            pass

    return ref


def task_to_persistence_manifest(task: Task) -> Dict[str, Any]:
    """
    Serialize a Task to a persistence/debug manifest dict (schema v2).

    Schema v2 changes:
    - Task.files values are stored as reference objects, not embedded contents:
        files: { "<dest>": { "path": "<dest>", "bytes": N, "sha256": "...", "source": "inline|local_path" } }
    """
    payload = task.model_dump(mode="json")
    payload["schema_version"] = TASK_MANIFEST_SCHEMA_VERSION

    files_ref: Dict[str, Any] = {}
    for dest, value in (task.files or {}).items():
        files_ref[dest] = _file_ref_for_task_file(dest, value)

    payload["files"] = files_ref
    return payload


def write_task_manifest_json(path: Path, task: Task) -> None:
    """
    Write a lean persistence/debug manifest.json for a Task.
    """
    payload = task_to_persistence_manifest(task)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def iter_strings(obj: Any):
    """
    Yield all string values found in a nested JSON-like structure.
    """
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, Mapping):
        for v in obj.values():
            yield from iter_strings(v)
        return
    if isinstance(obj, list):
        for v in obj:
            yield from iter_strings(v)
        return