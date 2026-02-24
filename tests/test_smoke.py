"""Smoke tests to verify the scaffold works."""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def test_paths_yaml_exists():
    """Verify configs/paths.yaml exists."""
    config_path = Path("configs/paths.yaml")
    assert config_path.exists(), f"Config file not found: {config_path}"


def test_data_root_exists():
    """Load configs/paths.yaml and assert data_root path exists on disk."""
    config_path = Path("configs/paths.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    assert "data_root" in config, "data_root not defined in paths.yaml"
    data_root = Path(config["data_root"])
    assert data_root.exists(), f"Data root does not exist: {data_root}"


def test_entrypoints_import():
    """Import all three entrypoint modules."""
    # Import and verify they have main() functions
    from src.curate import run_curate
    from src.features import run_features
    from src.models import run_models
    
    assert hasattr(run_curate, 'main'), "run_curate.main() not found"
    assert hasattr(run_features, 'main'), "run_features.main() not found"
    assert hasattr(run_models, 'main'), "run_models.main() not found"


def test_entrypoints_run_noop():
    """Run each entrypoint with --paths; assert return code 0 and artifacts_root exists."""
    # Load config to get artifacts_root
    config_path = Path("configs/paths.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    artifacts_root = Path(config["artifacts_root"])
    
    # Run each entrypoint.
    # run_curate uses --dry-run so it does not overwrite production artifacts
    # (dim_employer, dim_soc, etc.).  run_features and run_models read & write
    # derived tables only (they do not rebuild base dimensions).
    entrypoints_args = [
        ([sys.executable, "-m", "src.curate.run_curate", "--paths", "configs/paths.yaml", "--dry-run"],
         "src.curate.run_curate"),
        ([sys.executable, "-m", "src.features.run_features", "--paths", "configs/paths.yaml"],
         "src.features.run_features"),
        ([sys.executable, "-m", "src.models.run_models", "--paths", "configs/paths.yaml"],
         "src.models.run_models"),
    ]

    for cmd, entrypoint in entrypoints_args:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )

        assert result.returncode == 0, (
            f"{entrypoint} failed with return code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
    
    # Verify artifacts_root exists after running all entrypoints
    assert artifacts_root.exists(), f"Artifacts root not created: {artifacts_root}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
