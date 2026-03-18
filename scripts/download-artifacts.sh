#!/bin/bash

# Download artifacts from GitHub Releases
# Usage: ./scripts/download-artifacts.sh

set -e

REPO="v-rathod/immigration-model-builder"
RELEASE_TAG="latest"
ARTIFACTS_DIR="artifacts/tables"

echo "📥 Downloading artifacts from GitHub Releases..."
echo "Repository: $REPO"
echo "Release: $RELEASE_TAG"

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI not found. Install it with: brew install gh"
    echo "Or download artifacts manually from: https://github.com/$REPO/releases"
    exit 1
fi

# Check if we have GitHub auth
if ! gh auth status &> /dev/null; then
    echo "❌ Not authenticated with GitHub. Run: gh auth login"
    exit 1
fi

# Create artifacts directory
mkdir -p "$ARTIFACTS_DIR"

# Get the latest release
echo "Getting release info..."
RELEASE_INFO=$(gh release view $RELEASE_TAG --repo $REPO --json tagName,assets 2>/dev/null || echo "{}")

if [ "$RELEASE_INFO" = "{}" ]; then
    echo "❌ Could not find release. Check: https://github.com/$REPO/releases"
    exit 1
fi

# Extract asset information
PARQUET_FILES=$(echo "$RELEASE_INFO" | grep -o '"name":"[^"]*\.parquet"' || true)

if [ -z "$PARQUET_FILES" ]; then
    echo "⚠️  No .parquet files found in latest release"
    echo "📖 Generate artifacts locally: python3 scripts/rebuild_all.py"
    echo "📤 Upload to GitHub: gh release upload v1.0 artifacts/tables/*.parquet"
    exit 0
fi

# Download all .parquet files
echo ""
echo "Downloading files..."
while IFS= read -r file; do
    filename=$(echo "$file" | sed 's/.*"\([^"]*\.parquet\)".*/\1/')
    if [ -n "$filename" ]; then
        echo "  📥 $filename"
        gh release download $RELEASE_TAG --repo $REPO --pattern "$filename" --dir "$ARTIFACTS_DIR" 2>/dev/null || echo "    ⚠️  Failed to download $filename"
    fi
done <<< "$PARQUET_FILES"

# Verify downloads
DOWNLOADED=$(find "$ARTIFACTS_DIR" -name "*.parquet" 2>/dev/null | wc -l)
echo ""
echo "✅ Downloaded $DOWNLOADED .parquet files to $ARTIFACTS_DIR/"
echo ""
echo "Next steps:"
echo "1. Load artifacts in Python: import pandas as pd; df = pd.read_parquet('$ARTIFACTS_DIR/fact_cutoffs.parquet')"
echo "2. Or sync to P3: cd ../immigration-insights-app && python3 scripts/sync_p2_data.py"
