"""Tests for path validation checker."""

import subprocess
import sys
from pathlib import Path

import pytest


def test_check_paths_runs_successfully():
    """Test that the path checker runs and validates paths successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "src.io.check_paths", "--paths", "configs/paths.yaml"],
        capture_output=True,
        text=True,
        cwd=Path.cwd()
    )
    
    # Should complete successfully
    assert result.returncode == 0, (
        f"Path checker failed with return code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    
    # Should indicate success
    assert "PATH VALIDATION COMPLETE" in result.stdout, "Expected success message not found"
    assert "✓ OK" in result.stdout, "Expected OK status not found"
    
    # Verify artifacts_root exists after run
    artifacts_root = Path("artifacts")
    assert artifacts_root.exists(), "artifacts_root not created"
    assert artifacts_root.is_dir(), "artifacts_root is not a directory"
    
    # Verify subdirectories exist
    for subdir in ["tables", "models", "metrics"]:
        subdir_path = artifacts_root / subdir
        assert subdir_path.exists(), f"{subdir}/ subdirectory not created"
        assert subdir_path.is_dir(), f"{subdir}/ is not a directory"
    
    print("✓ Path checker validated successfully")


def test_check_paths_validates_data_root():
    """Test that the path checker properly validates data_root existence."""
    result = subprocess.run(
        [sys.executable, "-m", "src.io.check_paths", "--paths", "configs/paths.yaml"],
        capture_output=True,
        text=True,
        cwd=Path.cwd()
    )
    
    # Should check data_root
    assert "data_root" in result.stdout, "data_root not mentioned in output"
    
    # If successful, should show OK for data_root
    if result.returncode == 0:
        assert "data_root exists and is a directory" in result.stdout or \
               "OK: data_root" in result.stdout, \
               "data_root validation message not found"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
