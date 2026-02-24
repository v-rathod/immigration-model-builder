"""CLI tool to check and validate configured paths."""

import argparse
import sys
from pathlib import Path
import yaml


def main():
    """Check and validate data_root and artifacts_root paths."""
    parser = argparse.ArgumentParser(description="Check configured paths")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    args = parser.parse_args()
    
    # Read configuration
    config_path = Path(args.paths)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print("="*60)
    print("PATH VALIDATION")
    print("="*60)
    
    # Get paths
    data_root = config.get("data_root")
    artifacts_root = config.get("artifacts_root")
    
    if not data_root:
        print("ERROR: data_root not defined in config")
        sys.exit(1)
    
    if not artifacts_root:
        print("ERROR: artifacts_root not defined in config")
        sys.exit(1)
    
    # Convert to absolute paths
    data_root_path = Path(data_root).expanduser().resolve()
    artifacts_root_path = Path(artifacts_root).expanduser().resolve()
    
    print(f"\ndata_root (absolute):")
    print(f"  {data_root_path}")
    
    print(f"\nartifacts_root (absolute):")
    print(f"  {artifacts_root_path}")
    print()
    
    # Check data_root
    print("Checking data_root...")
    if not data_root_path.exists():
        print(f"  ✗ ERROR: data_root does not exist: {data_root_path}")
        print(f"  This directory should contain downloads from Project 1.")
        sys.exit(1)
    
    if not data_root_path.is_dir():
        print(f"  ✗ ERROR: data_root is not a directory: {data_root_path}")
        sys.exit(1)
    
    print(f"  ✓ OK: data_root exists and is a directory")
    
    # Check/create artifacts_root
    print("\nChecking artifacts_root...")
    if not artifacts_root_path.exists():
        print(f"  Creating artifacts_root: {artifacts_root_path}")
        artifacts_root_path.mkdir(parents=True, exist_ok=True)
        print(f"  ✓ OK: artifacts_root created")
    else:
        if not artifacts_root_path.is_dir():
            print(f"  ✗ ERROR: artifacts_root exists but is not a directory: {artifacts_root_path}")
            sys.exit(1)
        print(f"  ✓ OK: artifacts_root exists and is a directory")
    
    # Create subdirectories if needed
    subdirs = ["tables", "models", "metrics"]
    for subdir in subdirs:
        subdir_path = artifacts_root_path / subdir
        if not subdir_path.exists():
            subdir_path.mkdir(parents=True, exist_ok=True)
            print(f"  ✓ Created: {subdir}/")
    
    print("\n" + "="*60)
    print("PATH VALIDATION COMPLETE")
    print("All configured paths are valid and accessible.")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
