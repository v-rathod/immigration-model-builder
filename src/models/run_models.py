"""CLI entrypoint for model training pipeline."""

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
from src.models.pd_forecast import fit_pd_forecast
from src.models.employer_score import fit_employer_score


def main():
    parser = argparse.ArgumentParser(description="Train models on curated data")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    parser.add_argument("--efs-ml", action="store_true",
                        help="Also run EFS v2 ML model (non-destructive; writes side-by-side)")
    args = parser.parse_args()
    if _tap:
        _tap.intercept_chat("agent", "run_models START", task="models", level="INFO")

    # Load configuration
    config = load_paths_config(args.paths)
    artifacts_root = config["artifacts_root"]

    print("=" * 60)
    print("MODEL TRAINING PIPELINE")
    print("=" * 60)
    print(f"Artifacts root: {artifacts_root}")
    print()

    # Setup paths
    in_tables = Path(artifacts_root) / "tables"
    out_models = Path(artifacts_root) / "models"
    out_tables = Path(artifacts_root) / "tables"

    # Train models
    print("\n--- Priority Date Forecast ---")
    fit_pd_forecast(in_tables, out_models, out_tables)

    print("\n--- Employer Friendliness Score v1 (rules-based) ---")
    fit_employer_score(in_tables, out_tables)

    if args.efs_ml:
        print("\n--- Employer Friendliness Score v2 (ML-based) ---")
        try:
            from src.models.employer_score_ml import fit_employer_score_ml
            fit_employer_score_ml(in_tables, out_tables)
        except ImportError as e:
            print(f"  WARNING: EFS ML skipped — missing dependency: {e}")
        except Exception as e:
            print(f"  WARNING: EFS ML failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("\n--- EFS v2 ML skipped (pass --efs-ml to enable) ---")

    print("\n" + "=" * 60)
    print("MODEL TRAINING COMPLETE")
    print(f"Models written to: {artifacts_root}/models/")
    print(f"Predictions written to: {artifacts_root}/tables/")
    print("=" * 60)
    if _tap:
        _tap.intercept_chat("agent", "run_models COMPLETE", task="models", level="INFO")


if __name__ == "__main__":
    main()
