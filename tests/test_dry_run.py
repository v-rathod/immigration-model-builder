"""
Test dry-run mode: Verify that --dry-run discovers files without creating outputs.
"""
import subprocess
import sys
from pathlib import Path
import pytest


def test_dry_run_no_writes(tmpdir):
    """
    Test that --dry-run mode does not create any new parquet files.
    """
    # Create a temporary artifacts directory
    temp_artifacts = Path(tmpdir) / "artifacts_dry_run_test"
    temp_artifacts.mkdir(parents=True)
    
    # Create a temporary paths config
    paths_config = Path(tmpdir) / "paths_test.yaml"
    paths_config.write_text(f"""
data_root: downloads
artifacts_root: {temp_artifacts}
""")
    
    # Count existing parquet files before dry-run
    parquet_files_before = list(temp_artifacts.rglob("*.parquet"))
    count_before = len(parquet_files_before)
    
    # Run curation pipeline with --dry-run
    result = subprocess.run(
        [sys.executable, "-m", "src.curate.run_curate", "--paths", str(paths_config), "--dry-run"],
        capture_output=True,
        text=True
    )
    
    # Check exit code (should succeed)
    assert result.returncode == 0, f"Dry run failed:\n{result.stdout}\n{result.stderr}"
    
    # Verify output mentions dry-run
    assert "DRY RUN" in result.stdout, "Output should mention DRY RUN mode"
    
    # Count parquet files after dry-run
    parquet_files_after = list(temp_artifacts.rglob("*.parquet"))
    count_after = len(parquet_files_after)
    
    # Assert no new files were created
    assert count_after == count_before, (
        f"Dry run should not create new parquet files. "
        f"Before: {count_before}, After: {count_after}"
    )


def test_dry_run_discovers_files():
    """
    Test that --dry-run mode reports discovered files and partitions.
    """
    # Use actual paths config
    paths_config = Path("configs/paths.yaml")
    
    if not paths_config.exists():
        pytest.skip("configs/paths.yaml not found")
    
    # Run curation pipeline with --dry-run
    result = subprocess.run(
        [sys.executable, "-m", "src.curate.run_curate", "--paths", str(paths_config), "--dry-run"],
        capture_output=True,
        text=True
    )
    
    # Check exit code
    assert result.returncode == 0, f"Dry run failed:\n{result.stdout}\n{result.stderr}"
    
    # Verify discovery messages
    output = result.stdout
    
    # Should mention files would be processed
    assert "Would process" in output or "DRY RUN" in output, "Should report files to be processed"
    
    # Should mention partitions
    assert "Planned partitions" in output or "partition" in output.lower(), "Should report planned partitions"
    
    # Should mention no files were created
    assert "No files were created" in output or "no writes" in output.lower(), "Should confirm no writes"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
