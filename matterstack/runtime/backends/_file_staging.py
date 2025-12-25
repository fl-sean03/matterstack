"""
File staging utilities for compute backends.

This module provides shared logic for staging files (handling FileFromPath,
FileFromContent, Path, and legacy str heuristics) used by both LocalBackend
and SlurmBackend.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from ...core.workflow import FileFromContent, FileFromPath

logger = logging.getLogger(__name__)


@dataclass
class StagedFile:
    """
    Represents a file ready to be staged in a work directory.

    Attributes:
        filename: Target filename (may include subdirectories)
        source_path: Path to copy from (if staging from a file/directory)
        content: String content to write (if staging content directly)
        is_directory: True if source_path points to a directory
    """

    filename: str
    source_path: Optional[Path] = None
    content: Optional[str] = None
    is_directory: bool = False

    @property
    def is_path_based(self) -> bool:
        """Return True if this file should be copied from a source path."""
        return self.source_path is not None

    @property
    def is_content_based(self) -> bool:
        """Return True if this file should be written from string content."""
        return self.content is not None


def classify_file_entry(
    filename: str,
    content_or_path: Union[FileFromPath, FileFromContent, Path, str, Any],
) -> StagedFile:
    """
    Classify a single file entry from a Task's files dict.

    This implements the shared heuristic for determining whether a file entry
    represents:
    - An explicit path reference (FileFromPath or Path object)
    - Explicit string content (FileFromContent)
    - Legacy string that could be either a path or content

    Args:
        filename: The target filename in the work directory
        content_or_path: The file specification from Task.files

    Returns:
        A StagedFile describing how to stage the file

    Raises:
        FileNotFoundError: If a FileFromPath or Path source doesn't exist
        ValueError: If the content_or_path type is not recognized
    """
    if isinstance(content_or_path, FileFromPath):
        source = content_or_path.source_path
        if not source.exists():
            raise FileNotFoundError(f"Input file not found: {source}")
        return StagedFile(
            filename=filename,
            source_path=source,
            is_directory=source.is_dir(),
        )

    if isinstance(content_or_path, FileFromContent):
        return StagedFile(
            filename=filename,
            content=content_or_path.content,
        )

    if isinstance(content_or_path, Path):
        if not content_or_path.exists():
            raise FileNotFoundError(f"Input file not found: {content_or_path}")
        return StagedFile(
            filename=filename,
            source_path=content_or_path,
            is_directory=content_or_path.is_dir(),
        )

    if isinstance(content_or_path, str):
        # Legacy heuristic: check if it looks like a path AND exists
        is_likely_path = (
            len(content_or_path) > 0
            and len(content_or_path) < 1024
            and "\n" not in content_or_path
        )
        if is_likely_path and Path(content_or_path).exists():
            source = Path(content_or_path)
            return StagedFile(
                filename=filename,
                source_path=source,
                is_directory=source.is_dir(),
            )
        # Treat as content
        return StagedFile(
            filename=filename,
            content=content_or_path,
        )

    # Unknown type - log warning and raise
    logger.warning(f"Unknown content type for file {filename}: {type(content_or_path)}")
    raise ValueError(f"Unknown file type for {filename}: {type(content_or_path)}")


def classify_files(
    files: Dict[str, Union[FileFromPath, FileFromContent, Path, str, Any]],
) -> List[StagedFile]:
    """
    Classify all file entries from a Task's files dict.

    Args:
        files: The Task.files dictionary mapping filenames to specifications

    Returns:
        List of StagedFile objects ready for staging
    """
    return [
        classify_file_entry(filename, content_or_path)
        for filename, content_or_path in files.items()
    ]


def stage_files_to_directory(
    files: Dict[str, Union[FileFromPath, FileFromContent, Path, str, Any]],
    work_dir: Path,
) -> Dict[str, Path]:
    """
    Stage files into a local work directory.

    This is the main utility for LocalBackend to stage files. It handles:
    - Creating parent directories for nested files
    - Copying files/directories from paths
    - Writing string content to files

    Args:
        files: The Task.files dictionary mapping filenames to specifications
        work_dir: The target work directory (must exist)

    Returns:
        Dictionary mapping filenames to their staged paths

    Raises:
        FileNotFoundError: If a source file doesn't exist
        ValueError: If a file type is not recognized
    """
    staged_paths: Dict[str, Path] = {}

    for filename, content_or_path in files.items():
        staged = classify_file_entry(filename, content_or_path)
        dest_path = work_dir / filename

        # Ensure parent directory exists (for nested files like "subdir/file.txt")
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if staged.is_path_based:
            assert staged.source_path is not None  # for type checker
            if staged.is_directory:
                if dest_path.exists():
                    shutil.rmtree(dest_path)
                shutil.copytree(staged.source_path, dest_path)
            else:
                shutil.copy2(staged.source_path, dest_path)
        else:
            assert staged.content is not None  # for type checker
            with open(dest_path, "w") as f:
                f.write(staged.content)

        staged_paths[filename] = dest_path

    return staged_paths


def get_dry_run_description(
    filename: str,
    content_or_path: Union[FileFromPath, FileFromContent, Path, str, Any],
    task_dir: Path,
) -> str:
    """
    Generate a dry-run description for a file staging operation.

    Args:
        filename: Target filename
        content_or_path: The file specification
        task_dir: The target work directory

    Returns:
        Human-readable description of what would be staged
    """
    if isinstance(content_or_path, FileFromPath):
        return f"[DRY-RUN] cp {content_or_path.source_path} {task_dir}/{filename}"

    if isinstance(content_or_path, FileFromContent):
        return f"[DRY-RUN] write string to {task_dir}/{filename} ({len(content_or_path.content)} chars)"

    if isinstance(content_or_path, Path):
        return f"[DRY-RUN] cp {content_or_path} {task_dir}/{filename}"

    if isinstance(content_or_path, str):
        is_likely_path = (
            len(content_or_path) > 0
            and len(content_or_path) < 1024
            and "\n" not in content_or_path
        )
        if is_likely_path and Path(content_or_path).exists():
            return f"[DRY-RUN] cp {content_or_path} {task_dir}/{filename}"
        return f"[DRY-RUN] write string to {task_dir}/{filename} ({len(content_or_path)} chars)"

    return f"[DRY-RUN] Unknown type for {filename}: {type(content_or_path)}"


def get_files_for_upload(
    files: Dict[str, Union[FileFromPath, FileFromContent, Path, str, Any]],
) -> List[Tuple[str, StagedFile]]:
    """
    Classify files for remote upload (used by SlurmBackend).

    This returns a list of (filename, StagedFile) tuples that the SSH client
    can use to either upload from a path or write content.

    Args:
        files: The Task.files dictionary

    Returns:
        List of (filename, StagedFile) tuples
    """
    return [(filename, classify_file_entry(filename, spec)) for filename, spec in files.items()]
