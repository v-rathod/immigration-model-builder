"""CLI entrypoint for feature engineering pipeline."""

import argparse
from pathlib import Path
import sys

# ── Commentary capture (permanent) ───────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
try:
    from src.utils import chat_tap as _tap
except Exception:
    _tap = None  # type: ignore

from src.io.readers import load_paths_config, resolve_artifact_path
from src.features.employer_features import build_employer_features
from src.features.salary_benchmarks import build_salary_benchmarks


def main():
    parser = argparse.ArgumentParser(description="Engineer features from curated tables")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    args = parser.parse_args()
    if _tap:
        _tap.intercept_chat("agent", "run_features START", task="features", level="INFO")
    
    # Load configuration
    config = load_paths_config(args.paths)
    artifacts_root = config["artifacts_root"]
    
    print("="*60)
    print("FEATURE ENGINEERING PIPELINE")
    print("="*60)
    print(f"Artifacts root: {artifacts_root}")
    print()
    
    # Input/output paths
    in_tables = Path(artifacts_root) / "tables"
    
    # Build features
    print("\n--- Employer Features ---")
    employer_out = resolve_artifact_path(artifacts_root, "tables", "employer_features.parquet")
    build_employer_features(in_tables, employer_out)
    
    print("\n--- Salary Benchmarks ---")
    salary_out = resolve_artifact_path(artifacts_root, "tables", "salary_benchmarks.parquet")
    build_salary_benchmarks(in_tables, salary_out)
    
    print("\n" + "="*60)
    print("FEATURE ENGINEERING COMPLETE")
    print(f"Feature tables written to: {artifacts_root}/tables/")
    print("="*60)
    if _tap:
        _tap.intercept_chat("agent", "run_features COMPLETE", task="features", level="INFO")


if __name__ == "__main__":
    main()
