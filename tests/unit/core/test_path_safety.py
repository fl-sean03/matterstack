import pytest
import os
from pathlib import Path
from matterstack.runtime.fs_safety import ensure_under_run_root, operator_run_dir, PathSafetyError

def test_ensure_under_run_root_valid(tmp_path):
    run_root = tmp_path / "run_root"
    run_root.mkdir()
    target = run_root / "subdir" / "file.txt"
    
    # It handles non-existent targets too, as long as they would be inside
    res = ensure_under_run_root(run_root, target)
    assert str(res).startswith(str(run_root.resolve()))

def test_ensure_under_run_root_traversal(tmp_path):
    run_root = tmp_path / "run_root"
    run_root.mkdir()
    
    # Attempt traversal
    target = run_root / ".." / "outside.txt"
    
    with pytest.raises(PathSafetyError):
        ensure_under_run_root(run_root, target)

def test_ensure_under_run_root_absolute_outside(tmp_path):
    run_root = tmp_path / "run_root"
    run_root.mkdir()
    outside = tmp_path / "outside.txt"
    
    with pytest.raises(PathSafetyError):
        ensure_under_run_root(run_root, outside)

def test_ensure_under_run_root_symlink_attack(tmp_path):
    run_root = tmp_path / "run_root"
    run_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    
    # Create symlink inside root pointing outside
    symlink = run_root / "symlink_dir"
    symlink.symlink_to(outside)
    
    # Target through symlink
    target = symlink / "file.txt"
    
    # Should resolve to outside and fail
    with pytest.raises(PathSafetyError):
        ensure_under_run_root(run_root, target)

def test_operator_run_dir_sanitization(tmp_path):
    run_root = tmp_path / "run_root"
    run_root.mkdir()
    
    # Should clean "Bad/Type" -> "badtype" and "uuid/bad" -> "uuidbad"
    op_dir = operator_run_dir(run_root, "Bad/Type", "uuid/bad")
    
    # Verify structure
    assert (run_root / "operators" / "badtype" / "uuidbad").resolve() == op_dir