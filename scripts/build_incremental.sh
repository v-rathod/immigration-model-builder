#!/bin/bash
# Incremental pipeline runner — only rebuilds what changed in P1.
#
# Usage:
#   bash scripts/build_incremental.sh              # detect + plan (no execute)
#   bash scripts/build_incremental.sh --execute    # detect + rebuild changed
#   bash scripts/build_incremental.sh --full       # full rebuild (ignores changes)
#   bash scripts/build_incremental.sh --init       # initialize manifest (first run)
#
# How it works:
#   1. Scans P1 downloads directory for new/changed/deleted files
#   2. Compares against saved manifest (artifacts/metrics/p1_manifest.json)
#   3. Uses dependency graph to determine which P2 artifacts need rebuilding
#   4. Executes only the affected pipeline stages
#   5. Saves updated manifest on success
#
# First time setup:
#   bash scripts/build_all.sh                       # full build first
#   bash scripts/build_incremental.sh --init        # save baseline manifest

set -e

echo "============================================================"
echo "IMMIGRATION MODEL BUILDER - INCREMENTAL PIPELINE"
echo "============================================================"
echo ""

CONFIGS_DIR="configs"
PATHS_CONFIG="$CONFIGS_DIR/paths.yaml"

if [ ! -f "$PATHS_CONFIG" ]; then
    echo "ERROR: Config file not found: $PATHS_CONFIG"
    exit 1
fi

# Parse arguments
MODE="plan"
EXTRA_FLAGS=""
for arg in "$@"; do
    case "$arg" in
        --execute)  MODE="execute" ;;
        --full)     MODE="full" ;;
        --init)     MODE="init" ;;
        --hash)     EXTRA_FLAGS="$EXTRA_FLAGS --hash" ;;
        --dry-run)  MODE="dry-run" ;;
        *)          echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

case "$MODE" in
    init)
        echo "Initializing manifest from current P1 state..."
        python3 -m src.incremental.change_detector --paths "$PATHS_CONFIG" --init $EXTRA_FLAGS
        echo ""
        echo "✅ Manifest initialized. Future runs will detect changes from this baseline."
        ;;
    
    plan)
        echo "Detecting changes (plan only — no execution)..."
        python3 -m src.incremental.change_detector --paths "$PATHS_CONFIG" $EXTRA_FLAGS
        echo ""
        echo "To execute the rebuild: bash scripts/build_incremental.sh --execute"
        ;;
    
    execute)
        echo "Detecting changes and executing rebuild..."
        python3 -m src.incremental.change_detector --paths "$PATHS_CONFIG" --execute $EXTRA_FLAGS
        EXITCODE=$?
        echo ""
        if [ $EXITCODE -eq 0 ]; then
            echo "------------------------------------------------------------"
            echo "Post-rebuild: Patching dim_employer"
            echo "------------------------------------------------------------"
            python3 scripts/patch_dim_employer_from_fact_perm.py 2>&1 || true
            echo ""
            echo "============================================================"
            echo "INCREMENTAL PIPELINE COMPLETE"
            echo "============================================================"
        else
            echo "⚠️  Some rebuilds failed. Check output above."
            exit 1
        fi
        ;;
    
    dry-run)
        echo "Detecting changes (dry-run — commands shown but not executed)..."
        python3 -m src.incremental.change_detector --paths "$PATHS_CONFIG" --dry-run $EXTRA_FLAGS
        ;;
    
    full)
        echo "Running FULL rebuild (ignoring change detection)..."
        bash scripts/build_all.sh
        echo ""
        echo "Saving manifest for future incremental runs..."
        python3 -m src.incremental.change_detector --paths "$PATHS_CONFIG" --init $EXTRA_FLAGS
        echo "✅ Full rebuild + manifest saved."
        ;;
esac
