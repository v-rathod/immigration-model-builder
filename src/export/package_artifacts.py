"""Package artifacts for Project 3 consumption."""

from pathlib import Path
import shutil
from typing import List


def package_artifacts(artifacts_root: str, output_dir: str, manifest: List[str]) -> None:
    """Bundle essential artifacts for Project 3.
    
    Args:
        artifacts_root: Source artifacts directory
        output_dir: Destination export directory
        manifest: List of files to include (relative paths)
        
    Manifest example:
        - models/pd_forecast_model.json
        - tables/visa_bulletin.parquet
        - tables/employer_scores.parquet
        - tables/pd_forecasts.parquet
        
    TODO:
        - Copy artifacts to staging directory
        - Create metadata.json with version, timestamp, file checksums
        - Create lightweight archive (tar.gz or zip)
        - Validate all required files are present
    """
    print(f"[PACKAGE ARTIFACTS]")
    print(f"  Source: {artifacts_root}")
    print(f"  Destination: {output_dir}")
    print(f"  Manifest: {len(manifest)} files")
    print(f"  TODO: Copy artifacts, create metadata, generate archive")
    
    # Placeholder - just log the plan
    for item in manifest:
        print(f"    - {item}")


# TODO: Add versioning logic (semantic versioning for artifact bundles)
# TODO: Add checksum validation
# TODO: Add export to cloud storage (S3/Azure Blob) if needed
