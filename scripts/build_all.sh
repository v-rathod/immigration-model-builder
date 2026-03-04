#!/bin/bash
# Full pipeline execution script
#
# This script runs the entire immigration model builder pipeline:
#   1.    Curate raw data into canonical tables
#   1b.   Patch dim_employer + expand dim_soc from fact_perm
#   1c.   Additional P1-sourced fact tables (H1B hub, BLS CES, IV Post)
#   2.    Engineer core features (employer_features, salary_benchmarks, queue_depth)
#   2b.   Salary profiles (employer×role data for P3 Wage page)
#   2c.   Approval/denial dashboard artifacts
#   2d.   P3 export artifacts — ALL dashboard data (cutoff trends, CMM, backlog,
#         geo, SOC demand, processing times, employer monthly, risk, visa demand)
#   3.    Train models and generate predictions (pd_forecasts, EFS/SRS)
#   4.    RAG export (pre-compute chunks & Q&A cache for Compass chat)
#
# IMPORTANT: Stage 2d MUST run on every full build. It applies the blended-
# velocity formula (CMM, backlog) and all other data-quality corrections.
# Never apply data fixes as one-off patches outside this pipeline.
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
#           + expand dim_soc with legacy SOC-2010 codes from fact_perm
echo "------------------------------------------------------------"
echo "Stage 1b: PATCH DIM_EMPLOYER + EXPAND DIM_SOC"
echo "------------------------------------------------------------"
python3 scripts/patch_dim_employer_from_fact_perm.py
python3 scripts/expand_dim_soc_legacy.py
echo ""

# Stage 1c: Additional P1-sourced fact tables
echo "------------------------------------------------------------"
echo "Stage 1c: ADDITIONAL FACT TABLES (H1B Employer Hub, BLS CES, IV Post)"
echo "------------------------------------------------------------"
python3 scripts/build_fact_h1b_employer_hub.py
python3 scripts/build_fact_bls_ces.py
python3 scripts/build_fact_iv_post.py
echo ""

# Stage 2: Features
echo "------------------------------------------------------------"
echo "Stage 2: FEATURE ENGINEERING"
echo "------------------------------------------------------------"
python3 -m src.features.run_features --paths "$PATHS_CONFIG"
echo ""

# Stage 2b: Salary Profiles (employer×role salary data for P3 Salary page)
echo "------------------------------------------------------------"
echo "Stage 2b: SALARY PROFILES (employer×role salary artifact)"
echo "------------------------------------------------------------"
python3 scripts/make_employer_salary_profiles.py
echo ""

# Stage 2c: Approval/Denial Dashboard (for P3 approval/denial trends)
echo "------------------------------------------------------------"
echo "Stage 2c: APPROVAL/DENIAL TRENDS DASHBOARD"
echo "------------------------------------------------------------"
python3 scripts/build_approval_denial_trends.py
python3 scripts/build_approval_denial_detailed.py
python3 scripts/export_approval_denial_for_p3.py
echo ""

# Stage 2d: P3 Export Artifacts — dashboard and visualization data
#
# These scripts produce the derived artifacts consumed by Compass (P3).
# They MUST run on every full build so that fresh P1 data produces correct
# outputs.  The blended-velocity fix (CMM + backlog) lives inside these
# scripts — NOT as a one-off patch.
#
# Dependency order:
#   STEP 1  make_fact_cutoff_trends        (requires: fact_cutoffs_all)
#   STEP 2  make_employer_monthly_metrics  (requires: fact_perm, dim_employer)
#   STEP 3  make_category_movement_metrics (requires: fact_cutoff_trends STEP 1)
#   STEP 4  make_worksite_geo_metrics      (requires: fact_perm, fact_lca, dim_area)
#   STEP 5  make_salary_benchmarks         (already run via run_features above)
#   STEP 6  make_soc_demand_metrics        (requires: fact_perm, fact_lca, dim_soc)
#   STEP 7  make_processing_times_trends   (requires: P1 USCIS processing CSV)
#   STEP 8  make_backlog_estimates         (requires: fact_cutoff_trends STEP 1, fact_perm)
#   --      make_employer_risk_features    (requires: fact_warn_events, dim_employer)
#   --      make_visa_demand_metrics       (requires: fact_visa_issuance, fact_visa_applications)
echo "------------------------------------------------------------"
echo "Stage 2d: P3 EXPORT ARTIFACTS (dashboards + visualizations)"
echo "------------------------------------------------------------"
echo "[STEP 1/8] Cutoff trends (fact_cutoff_trends)"
python3 scripts/make_fact_cutoff_trends.py
echo "[STEP 2/8] Employer monthly metrics"
python3 scripts/make_employer_monthly_metrics.py
echo "[STEP 3/8] Category movement metrics (blended velocity)"
python3 scripts/make_category_movement_metrics.py
echo "[STEP 4/8] Worksite geographic metrics"
python3 scripts/make_worksite_geo_metrics.py
echo "[STEP 6/8] SOC demand metrics"
python3 scripts/make_soc_demand_metrics.py
echo "[STEP 7/8] Processing times trends"
python3 scripts/make_processing_times_trends.py
echo "[STEP 8/8] Backlog estimates (blended velocity)"
python3 scripts/make_backlog_estimates.py
echo "[  extra ] Employer risk features"
python3 scripts/make_employer_risk_features.py
echo "[  extra ] Visa demand metrics"
python3 scripts/make_visa_demand_metrics.py
echo ""

# Stage 3: Models
echo "------------------------------------------------------------"
echo "Stage 3: MODEL TRAINING"
echo "------------------------------------------------------------"
python3 -m src.models.run_models --paths "$PATHS_CONFIG"
echo ""

# Stage 4: RAG Export (pre-compute chunks & Q&A for Compass chat)
echo "------------------------------------------------------------"
echo "Stage 4: RAG EXPORT (chunks + Q&A cache for Compass)"
echo "------------------------------------------------------------"
python3 -m src.export.rag_builder
python3 -m src.export.qa_generator
echo ""

echo "============================================================"
echo "PIPELINE COMPLETE"
echo "============================================================"
echo "Artifacts generated in: ./artifacts/"
echo "  - tables/    : Curated data and feature tables"
echo "  - models/    : Trained model artifacts"
echo "  - rag/       : RAG chunks & Q&A cache for Compass chat"
echo ""
echo "Next: Review artifacts and deploy to Compass (P3)"
echo "============================================================"
