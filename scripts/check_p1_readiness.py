#!/usr/bin/env python3
"""
P1 → P2 Readiness Check

Scans the P1 downloads directory and reports:
  1. All tracked files classified by dataset
  2. Any UNKNOWN files (not mapped to a dataset pattern)
  3. New directories not in DATASET_PATTERNS
  4. Which datasets have P2 builders (with dependency graph entries)
  5. Which datasets are tracked but have no P2 builder yet (future work)
  6. Change summary vs saved manifest
  7. Recommended actions

Usage:
    python3 scripts/check_p1_readiness.py
    python3 scripts/check_p1_readiness.py --verbose
    python3 scripts/check_p1_readiness.py --fix  # auto-run incremental build if changes found

This script should be run:
  - After any P1 fetch/update to detect new data
  - Before running the P2 pipeline to confirm readiness
  - Periodically to check if new P1 data sources appeared
"""

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.incremental.change_detector import (
    DATASET_PATTERNS,
    DEPENDENCY_GRAPH,
    TRACKED_EXTENSIONS,
    ChangeDetector,
    classify_dataset,
)


def main():
    parser = argparse.ArgumentParser(description="P1 → P2 readiness check")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show file-level details")
    parser.add_argument("--fix", action="store_true", help="Auto-run incremental build if changes found")
    args = parser.parse_args()

    # Load config
    config_path = PROJECT_ROOT / "configs" / "paths.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    data_root = Path(config["data_root"])

    print("=" * 70)
    print("P1 → P2 READINESS CHECK")
    print("=" * 70)
    print(f"  P1 data root:  {data_root}")
    print(f"  P2 project:    {PROJECT_ROOT}")

    if not data_root.exists():
        print(f"\n  ❌ ERROR: P1 data_root does not exist: {data_root}")
        print(f"     Run Project 1 (fetch-immigration-data) first.")
        sys.exit(1)

    # ── 1. Scan P1 directories ──────────────────────────────────────────

    print(f"\n{'─' * 70}")
    print("1. P1 DIRECTORY SCAN")
    print(f"{'─' * 70}")

    top_dirs = sorted([d for d in data_root.iterdir() if d.is_dir() and not d.name.startswith(".")])
    all_files = []
    dir_file_counts = {}

    for d in top_dirs:
        count = sum(1 for _ in d.rglob("*") if _.is_file() and not _.name.startswith(".") and not _.name.startswith("_"))
        dir_file_counts[d.name] = count
        for f in d.rglob("*"):
            if f.is_file() and not f.name.startswith(".") and not f.name.startswith("_"):
                ext = f.suffix.lower()
                if ext in TRACKED_EXTENSIONS:
                    all_files.append(str(f.relative_to(data_root)))

    print(f"\n  {'Directory':<35s} {'Files':>6s}  Dataset Classification")
    print(f"  {'─' * 35} {'─' * 6}  {'─' * 30}")
    for d_name, count in sorted(dir_file_counts.items()):
        dataset = classify_dataset(d_name + "/somefile.ext")
        has_builder = "✅ has builder" if dataset in DEPENDENCY_GRAPH and DEPENDENCY_GRAPH[dataset] else "⏳ future"
        if dataset == "UNKNOWN":
            has_builder = "❌ UNKNOWN"
        elif dataset == "DOL_RECORD_LAYOUTS":
            has_builder = "📄 metadata"
        print(f"  {d_name:<35s} {count:>6d}  {dataset:<22s} {has_builder}")

    print(f"\n  Total tracked files: {len(all_files)}")

    # ── 2. Classification check ─────────────────────────────────────────

    print(f"\n{'─' * 70}")
    print("2. FILE CLASSIFICATION")
    print(f"{'─' * 70}")

    dataset_counts = Counter()
    unknowns = []
    for rel_path in all_files:
        ds = classify_dataset(rel_path)
        dataset_counts[ds] += 1
        if ds == "UNKNOWN":
            unknowns.append(rel_path)

    for ds, count in sorted(dataset_counts.items(), key=lambda x: -x[1]):
        has_graph = "✅" if ds in DEPENDENCY_GRAPH and DEPENDENCY_GRAPH[ds] else "⏳"
        if ds == "UNKNOWN":
            has_graph = "❌"
        elif ds == "DOL_RECORD_LAYOUTS":
            has_graph = "📄"
        print(f"  {has_graph} {ds:<25s} {count:>5d} files")

    if unknowns:
        print(f"\n  ⚠️  {len(unknowns)} UNKNOWN files (not mapped to any dataset):")
        for u in unknowns[:10]:
            print(f"     {u}")
        if len(unknowns) > 10:
            print(f"     ... and {len(unknowns) - 10} more")
        print(f"\n  ACTION NEEDED: Add patterns to DATASET_PATTERNS in")
        print(f"    src/incremental/change_detector.py")
    else:
        print(f"\n  ✅ All {len(all_files)} files are classified — zero UNKNOWN")

    # ── 3. Dependency graph coverage ────────────────────────────────────

    print(f"\n{'─' * 70}")
    print("3. DEPENDENCY GRAPH COVERAGE")
    print(f"{'─' * 70}")

    datasets_with_builders = []
    datasets_future = []
    datasets_metadata = []

    for ds in sorted(set(dataset_counts.keys()) - {"UNKNOWN"}):
        if ds in DEPENDENCY_GRAPH:
            artifacts = DEPENDENCY_GRAPH[ds]
            if artifacts:
                datasets_with_builders.append((ds, artifacts))
            elif ds == "DOL_RECORD_LAYOUTS":
                datasets_metadata.append(ds)
            else:
                datasets_future.append(ds)
        else:
            datasets_future.append(ds)

    print(f"\n  Datasets WITH P2 builders ({len(datasets_with_builders)}):")
    for ds, artifacts in datasets_with_builders:
        art_names = [a[0] for a in artifacts]
        print(f"    ✅ {ds:<25s} → {', '.join(art_names[:4])}")
        if len(art_names) > 4:
            print(f"       {'':25s}   + {len(art_names) - 4} more artifacts")

    if datasets_future:
        print(f"\n  Datasets WITHOUT P2 builders yet ({len(datasets_future)}) — FUTURE WORK:")
        for ds in datasets_future:
            count = dataset_counts.get(ds, 0)
            print(f"    ⏳ {ds:<25s} ({count} files tracked, no artifacts built)")
        print(f"\n  💡 To add a builder for a new dataset:")
        print(f"     1. Create curate script: scripts/build_fact_<name>.py")
        print(f"     2. Add dependency entry to DEPENDENCY_GRAPH in")
        print(f"        src/incremental/change_detector.py")
        print(f"     3. Run: python3 scripts/build_fact_<name>.py")
        print(f"     4. Add tests in tests/ directory")
        print(f"     5. Re-init manifest: bash scripts/build_incremental.sh --init")

    if datasets_metadata:
        print(f"\n  Reference/metadata datasets ({len(datasets_metadata)}):")
        for ds in datasets_metadata:
            print(f"    📄 {ds} — no artifacts (parser reference docs)")

    # ── 4. Change detection vs manifest ─────────────────────────────────

    print(f"\n{'─' * 70}")
    print("4. CHANGE DETECTION (vs saved manifest)")
    print(f"{'─' * 70}")

    manifest_path = PROJECT_ROOT / "artifacts" / "metrics" / "p1_manifest.json"
    if not manifest_path.exists():
        print(f"\n  ⚠️  No manifest found at: {manifest_path}")
        print(f"     Run: bash scripts/build_incremental.sh --init")
    else:
        cd = ChangeDetector("configs/paths.yaml", project_root=PROJECT_ROOT)
        changes = cd.detect_changes()

        if changes.has_changes:
            print(f"\n  ⚠️  CHANGES DETECTED: {changes.summary}")

            if changes.new_files:
                new_datasets = Counter(f.dataset for f in changes.new_files)
                print(f"\n    NEW files ({len(changes.new_files)}):")
                for ds, cnt in sorted(new_datasets.items(), key=lambda x: -x[1]):
                    print(f"      {ds}: {cnt} files")
                if args.verbose:
                    for f in changes.new_files[:20]:
                        print(f"        {f.rel_path}")

            if changes.changed_files:
                chg_datasets = Counter(f[1].dataset for f in changes.changed_files)
                print(f"\n    CHANGED files ({len(changes.changed_files)}):")
                for ds, cnt in sorted(chg_datasets.items(), key=lambda x: -x[1]):
                    print(f"      {ds}: {cnt} files")

            if changes.deleted_files:
                del_datasets = Counter(f.dataset for f in changes.deleted_files)
                print(f"\n    DELETED files ({len(changes.deleted_files)}):")
                for ds, cnt in sorted(del_datasets.items(), key=lambda x: -x[1]):
                    print(f"      {ds}: {cnt} files")

            # Plan rebuild
            actions = cd.plan_rebuild(changes)
            if actions:
                print(f"\n  📋 REBUILD PLAN: {len(actions)} commands needed")
                print(f"     Run: bash scripts/build_incremental.sh --execute")

                if args.fix:
                    print(f"\n  🔧 Auto-fixing (--fix flag): running incremental build...")
                    result = subprocess.run(
                        ["bash", "scripts/build_incremental.sh", "--execute"],
                        cwd=str(PROJECT_ROOT),
                        capture_output=False,
                    )
                    if result.returncode == 0:
                        print(f"  ✅ Incremental build succeeded")
                    else:
                        print(f"  ❌ Incremental build failed (exit code {result.returncode})")
                        sys.exit(1)
        else:
            print(f"\n  ✅ No changes — P1 data matches manifest ({changes.unchanged_count} files)")

    # ── 5. New directory detection ──────────────────────────────────────

    print(f"\n{'─' * 70}")
    print("5. NEW DIRECTORY DETECTION")
    print(f"{'─' * 70}")

    # Check for top-level dirs not matching any dataset pattern
    known_prefixes = set(DATASET_PATTERNS.keys())
    new_dirs = []
    for d in top_dirs:
        matched = False
        # Check if any pattern would match files in this directory
        test_path = d.name + "/test.csv"
        if classify_dataset(test_path) != "UNKNOWN":
            matched = True
        if not matched:
            new_dirs.append(d.name)

    if new_dirs:
        print(f"\n  ⚠️  {len(new_dirs)} P1 directories not in DATASET_PATTERNS:")
        for d_name in new_dirs:
            count = dir_file_counts.get(d_name, 0)
            print(f"    ❓ {d_name}/ ({count} files)")
        print(f"\n  ACTION NEEDED: Add patterns to DATASET_PATTERNS in")
        print(f"    src/incremental/change_detector.py")
    else:
        print(f"\n  ✅ All P1 directories are recognized — no new unknown sources")

    # ── 6. Summary ──────────────────────────────────────────────────────

    print(f"\n{'═' * 70}")
    print("SUMMARY")
    print(f"{'═' * 70}")
    print(f"  P1 directories:       {len(top_dirs)}")
    print(f"  Files tracked:        {len(all_files)}")
    print(f"  Datasets classified:  {len(dataset_counts)}")
    print(f"  UNKNOWN files:        {len(unknowns)}")
    print(f"  Datasets w/ builders: {len(datasets_with_builders)}")
    print(f"  Datasets future:      {len(datasets_future)}")

    issues = len(unknowns) + len(new_dirs)
    if issues:
        print(f"\n  ⚠️  {issues} issue(s) found — see details above")
        print(f"  Run 'python3 scripts/check_p1_readiness.py --verbose' for details")
    else:
        print(f"\n  ✅ P1 → P2 pipeline is READY")
        print(f"     To check for changes: bash scripts/build_incremental.sh")
        print(f"     To rebuild changed:   bash scripts/build_incremental.sh --execute")


if __name__ == "__main__":
    main()
