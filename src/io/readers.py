"""Helpers for file path resolution and simple data loaders."""

import os
from pathlib import Path
from typing import Dict, List, Optional
import yaml


def load_paths_config(config_path: str) -> Dict[str, str]:
    """Load paths from YAML config file.
    
    Args:
        config_path: Path to paths.yaml
        
    Returns:
        Dictionary with data_root and artifacts_root
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def resolve_data_path(data_root: str, *parts: str) -> Path:
    """Build absolute path within data root.
    
    Args:
        data_root: Base directory for raw data
        *parts: Path components to join
        
    Returns:
        Resolved Path object
    """
    return Path(data_root) / Path(*parts)


def resolve_artifact_path(artifacts_root: str, *parts: str) -> Path:
    """Build absolute path within artifacts root.
    
    Args:
        artifacts_root: Base directory for artifacts
        *parts: Path components to join
        
    Returns:
        Resolved Path object
    """
    path = Path(artifacts_root) / Path(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def list_files_by_pattern(directory: Path, pattern: str) -> List[Path]:
    """List files matching glob pattern.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern (e.g., "*.pdf", "PERM_*.xlsx")
        
    Returns:
        List of matching file paths
    """
    if not directory.exists():
        print(f"Warning: Directory does not exist: {directory}")
        return []
    return sorted(directory.glob(pattern))


# TODO: Add CSV/Excel/Parquet reader helpers as needed
# TODO: Add PDF text extraction helper (e.g., using PyPDF2 or pdfplumber)
