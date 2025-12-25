"""
Unit tests for the _file_staging utility module.

These tests verify the file staging logic that is shared between
LocalBackend and SlurmBackend.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from matterstack.core.workflow import FileFromContent, FileFromPath
from matterstack.runtime.backends._file_staging import (
    StagedFile,
    classify_file_entry,
    classify_files,
    get_dry_run_description,
    get_files_for_upload,
    stage_files_to_directory,
)


class TestStagedFile:
    """Tests for the StagedFile dataclass."""

    def test_path_based_file(self):
        """Test StagedFile with source path."""
        staged = StagedFile(filename="test.txt", source_path=Path("/tmp/test.txt"))
        assert staged.is_path_based
        assert not staged.is_content_based
        assert not staged.is_directory

    def test_content_based_file(self):
        """Test StagedFile with content."""
        staged = StagedFile(filename="test.txt", content="hello world")
        assert not staged.is_path_based
        assert staged.is_content_based
        assert not staged.is_directory

    def test_directory_file(self):
        """Test StagedFile for a directory."""
        staged = StagedFile(
            filename="mydir", source_path=Path("/tmp/mydir"), is_directory=True
        )
        assert staged.is_path_based
        assert not staged.is_content_based
        assert staged.is_directory


class TestClassifyFileEntry:
    """Tests for classify_file_entry function."""

    def test_file_from_path_exists(self, tmp_path: Path):
        """Test FileFromPath with existing file."""
        test_file = tmp_path / "source.txt"
        test_file.write_text("content")

        staged = classify_file_entry("dest.txt", FileFromPath(source_path=test_file))

        assert staged.filename == "dest.txt"
        assert staged.source_path == test_file
        assert not staged.is_directory
        assert staged.is_path_based

    def test_file_from_path_not_exists(self, tmp_path: Path):
        """Test FileFromPath with non-existent file raises error."""
        nonexistent = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError, match="Input file not found"):
            classify_file_entry("dest.txt", FileFromPath(source_path=nonexistent))

    def test_file_from_path_directory(self, tmp_path: Path):
        """Test FileFromPath with a directory."""
        test_dir = tmp_path / "mydir"
        test_dir.mkdir()

        staged = classify_file_entry("dest_dir", FileFromPath(source_path=test_dir))

        assert staged.filename == "dest_dir"
        assert staged.source_path == test_dir
        assert staged.is_directory
        assert staged.is_path_based

    def test_file_from_content(self):
        """Test FileFromContent."""
        staged = classify_file_entry(
            "script.py", FileFromContent(content="print('hello')")
        )

        assert staged.filename == "script.py"
        assert staged.content == "print('hello')"
        assert staged.is_content_based
        assert not staged.is_path_based

    def test_path_object_exists(self, tmp_path: Path):
        """Test raw Path object with existing file."""
        test_file = tmp_path / "source.txt"
        test_file.write_text("content")

        staged = classify_file_entry("dest.txt", test_file)

        assert staged.filename == "dest.txt"
        assert staged.source_path == test_file
        assert staged.is_path_based

    def test_path_object_not_exists(self, tmp_path: Path):
        """Test raw Path object with non-existent file raises error."""
        nonexistent = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError, match="Input file not found"):
            classify_file_entry("dest.txt", nonexistent)

    def test_string_path_exists(self, tmp_path: Path):
        """Test string that looks like a path and exists."""
        test_file = tmp_path / "source.txt"
        test_file.write_text("content")

        staged = classify_file_entry("dest.txt", str(test_file))

        assert staged.filename == "dest.txt"
        assert staged.source_path == test_file
        assert staged.is_path_based

    def test_string_path_not_exists_treated_as_content(self, tmp_path: Path):
        """Test string that looks like a path but doesn't exist is treated as content."""
        path_string = str(tmp_path / "nonexistent.txt")

        staged = classify_file_entry("dest.txt", path_string)

        assert staged.filename == "dest.txt"
        assert staged.content == path_string
        assert staged.is_content_based

    def test_string_content_with_newlines(self):
        """Test string with newlines is treated as content."""
        content = "line1\nline2\nline3"

        staged = classify_file_entry("script.py", content)

        assert staged.filename == "script.py"
        assert staged.content == content
        assert staged.is_content_based

    def test_string_long_content(self):
        """Test long string (>1024 chars) is treated as content."""
        content = "x" * 2000

        staged = classify_file_entry("data.txt", content)

        assert staged.filename == "data.txt"
        assert staged.content == content
        assert staged.is_content_based

    def test_unknown_type_raises(self):
        """Test unknown type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown file type"):
            classify_file_entry("test.txt", 12345)  # type: ignore


class TestClassifyFiles:
    """Tests for classify_files function."""

    def test_multiple_files(self, tmp_path: Path):
        """Test classifying multiple files."""
        source_file = tmp_path / "source.txt"
        source_file.write_text("from file")

        files = {
            "from_path.txt": FileFromPath(source_path=source_file),
            "from_content.txt": FileFromContent(content="inline content"),
            "legacy.txt": "legacy content\nmultiline",
        }

        staged_list = classify_files(files)

        assert len(staged_list) == 3
        filenames = [s.filename for s in staged_list]
        assert "from_path.txt" in filenames
        assert "from_content.txt" in filenames
        assert "legacy.txt" in filenames


class TestStageFilesToDirectory:
    """Tests for stage_files_to_directory function."""

    def test_stage_file_from_path(self, tmp_path: Path):
        """Test staging a file from path."""
        source = tmp_path / "source"
        source.mkdir()
        source_file = source / "input.txt"
        source_file.write_text("source content")

        dest = tmp_path / "dest"
        dest.mkdir()

        files = {"output.txt": FileFromPath(source_path=source_file)}
        staged_paths = stage_files_to_directory(files, dest)

        assert "output.txt" in staged_paths
        assert staged_paths["output.txt"] == dest / "output.txt"
        assert (dest / "output.txt").read_text() == "source content"

    def test_stage_file_from_content(self, tmp_path: Path):
        """Test staging a file from content."""
        dest = tmp_path / "dest"
        dest.mkdir()

        files = {"script.py": FileFromContent(content="print('hello')")}
        staged_paths = stage_files_to_directory(files, dest)

        assert "script.py" in staged_paths
        assert (dest / "script.py").read_text() == "print('hello')"

    def test_stage_nested_file(self, tmp_path: Path):
        """Test staging a file to a nested path."""
        dest = tmp_path / "dest"
        dest.mkdir()

        files = {"subdir/nested/file.txt": FileFromContent(content="nested content")}
        staged_paths = stage_files_to_directory(files, dest)

        assert "subdir/nested/file.txt" in staged_paths
        assert (dest / "subdir" / "nested" / "file.txt").read_text() == "nested content"

    def test_stage_directory(self, tmp_path: Path):
        """Test staging a directory."""
        source = tmp_path / "source_dir"
        source.mkdir()
        (source / "file1.txt").write_text("file1")
        (source / "file2.txt").write_text("file2")

        dest = tmp_path / "dest"
        dest.mkdir()

        files = {"copied_dir": FileFromPath(source_path=source)}
        stage_files_to_directory(files, dest)

        assert (dest / "copied_dir").is_dir()
        assert (dest / "copied_dir" / "file1.txt").read_text() == "file1"
        assert (dest / "copied_dir" / "file2.txt").read_text() == "file2"


class TestGetDryRunDescription:
    """Tests for get_dry_run_description function."""

    def test_file_from_path(self, tmp_path: Path):
        """Test dry-run description for FileFromPath."""
        source = tmp_path / "source.txt"
        desc = get_dry_run_description(
            "dest.txt", FileFromPath(source_path=source), tmp_path
        )
        assert "[DRY-RUN] cp" in desc
        assert str(source) in desc

    def test_file_from_content(self):
        """Test dry-run description for FileFromContent."""
        desc = get_dry_run_description(
            "script.py", FileFromContent(content="hello"), Path("/work")
        )
        assert "[DRY-RUN] write string" in desc
        assert "5 chars" in desc

    def test_path_object(self, tmp_path: Path):
        """Test dry-run description for Path object."""
        source = tmp_path / "source.txt"
        desc = get_dry_run_description("dest.txt", source, tmp_path)
        assert "[DRY-RUN] cp" in desc

    def test_string_content(self):
        """Test dry-run description for string content."""
        content = "line1\nline2"
        desc = get_dry_run_description("file.txt", content, Path("/work"))
        assert "[DRY-RUN] write string" in desc
        assert "11 chars" in desc


class TestGetFilesForUpload:
    """Tests for get_files_for_upload function."""

    def test_returns_tuples(self, tmp_path: Path):
        """Test that get_files_for_upload returns correct tuples."""
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        files = {
            "file1.txt": FileFromPath(source_path=source_file),
            "file2.txt": FileFromContent(content="content"),
        }

        result = get_files_for_upload(files)

        assert len(result) == 2
        names = [name for name, _ in result]
        assert "file1.txt" in names
        assert "file2.txt" in names
