#!/bin/bash
# Full pipeline execution script
#
# This script runs the entire immigration model builder pipeline:
#   1. Curate raw data into canonical tables
#   1b. Patch dim_employer from all fact_perm partitions
#   2. Engineer features from curated tables
#   3. Train models and generate predictions
#
# Usage: bash scripts/build_all.sh
#
# BEFORE RUNNING: Check for P1 data changes first:
#   python3 scripts/check_p1_readiness.py
#
# For incremental builds (only rebuild what changed):
#   bash scripts/build_incremental.sh --execute
#
# After a full build, save the manifest for future incremental runs:
#   bash scripts/build_incremental.sh --init

set -e  # Exit on error

echo "============================================================"
echo "IMMIGRATION MODEL BUILDER - FULL PIPELINE"
echo "============================================================"
echo ""

# Define paths
CONFIGS_DIR="configs"
PATHS_CONFIG="$CONFIGS_DIR/paths.yaml"

# Check config exists
if [ ! -f "$PATHS_CONFIG" ]; then
    echo "ERROR: Config file not found: $PATHS_CONFIG"
    exit 1
fi

echo "Using config: $PATHS_CONFIG"
echo ""

# Stage 1: Curate
echo "------------------------------------------------------------"
echo "Stage 1: CURATION"
echo "------------------------------------------------------------"
python3 -m src.curate.run_curate --paths "$PATHS_CONFIG"
echo ""

# Stage 1b: Patch dim_employer (expand to cover ALL fact_perm employers)
echo "------------------------------------------------------------"
echo "Stage 1b: PATCH DIM_EMPLOYER (fact_perm expansion)"
echo "------------------------------------------------------------"
python3 scripts/patch_dim_employer_from_fact_perm.py
echo ""

# Stage 2: Features
echo "------------------------------------------------------------"
echo "Stage 2: FEATURE ENGINEERING"
echo "------------------------------------------------------------"
python3 -m src.features.run_features --paths "$PATHS_CONFIG"
echo ""

# Stage 3: Models
echo "------------------------------------------------------------"
echo "Stage 3: MODEL TRAINING"
echo "------------------------------------------------------------"
python3 -m src.models.run_models --paths "$PATHS_CONFIG"
echo ""

echo "============================================================"
echo "PIPELINE COMPLETE"
echo "============================================================"
echo "Artifacts generated in: ./artifacts/"
echo "  - tables/    : Curated data and feature tables"
echo "  - models/    : Trained model artifacts"
echo ""
echo "Next: Review artifacts and implement parsing logic"
echo "============================================================"
