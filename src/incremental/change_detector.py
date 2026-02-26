#!/usr/bin/env python3
"""
Incremental change detection for P1 → P2 pipeline.

Maintains a manifest of all P1 download files (path, size, mtime, sha256).
On each run, compares current state against manifest to detect:
  - NEW files (in P1 but not in manifest)
  - CHANGED files (size or mtime differs)
  - DELETED files (in manifest but gone from P1)

Maps changed files to affected P2 artifacts via a dependency graph,
then returns the minimal set of rebuild actions needed.

Usage:
    from src.incremental.change_detector import ChangeDetector
    cd = ChangeDetector("configs/paths.yaml")
    changes = cd.detect_changes()
    actions = cd.plan_rebuild(changes)
    cd.save_manifest()  # after successful rebuild
"""

import hashlib
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class FileFingerprint:
    """Fingerprint of a single file in the P1 downloads directory."""
    rel_path: str          # relative to data_root
    size: int              # bytes
    mtime: float           # os.stat st_mtime
    sha256: Optional[str]  # hex digest (computed lazily for large files)
    dataset: str           # canonical dataset bucket (PERM, LCA, OEWS, etc.)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FileFingerprint":
        return cls(**d)


@dataclass
class ChangeSet:
    """Result of comparing current P1 state against saved manifest."""
    new_files: List[FileFingerprint] = field(default_factory=list)
    changed_files: List[Tuple[FileFingerprint, FileFingerprint]] = field(default_factory=list)  # (old, new)
    deleted_files: List[FileFingerprint] = field(default_factory=list)
    unchanged_count: int = 0

    @property
    def has_changes(self) -> bool:
        return bool(self.new_files or self.changed_files or self.deleted_files)

    @property
    def summary(self) -> str:
        parts = []
        if self.new_files:
            parts.append(f"{len(self.new_files)} new")
        if self.changed_files:
            parts.append(f"{len(self.changed_files)} changed")
        if self.deleted_files:
            parts.append(f"{len(self.deleted_files)} deleted")
        if not parts:
            return f"No changes ({self.unchanged_count} files unchanged)"
        return f"{', '.join(parts)} ({self.unchanged_count} unchanged)"


@dataclass
class RebuildAction:
    """A single artifact rebuild action."""
    artifact: str       # e.g. "fact_perm/", "employer_features.parquet"
    reason: str         # human-readable reason
    stage: int          # pipeline stage (1=curate, 2=features, 3=models, 4=derived)
    command: str        # shell command or Python callable reference
    triggered_by: List[str] = field(default_factory=list)  # changed files that caused this


# ── Dataset classifier ───────────────────────────────────────────────────────

# Maps directory patterns → canonical dataset names
DATASET_PATTERNS = {
    # ── Core datasets (have P2 builders) ──
    "PERM":                  "PERM",
    "LCA":                   "LCA",
    "OEWS":                  "OEWS",
    "Visa_Bulletin":         "VISA_BULLETIN",
    "Visa_Annual_Reports":   "VISA_ISSUANCE",
    "Visa_Statistics":       "VISA_APPLICATIONS",
    "NIV_Statistics":        "NIV_ISSUANCE",
    "USCIS_IMMIGRATION":     "USCIS",
    "DHS_Yearbook":          "DHS_ADMISSIONS",
    "WARN":                  "WARN",
    "Codebooks":             "CODEBOOKS",
    "Numerical_Limits":      "VISA_CEILING",
    "Waiting_List":          "WAITING_LIST",
    "ACS":                   "ACS",
    "TRAC":                  "TRAC",
    # ── Tracked but no P2 builder yet (future) ──
    "USCIS_H1B_Employer_Hub": "H1B_EMPLOYER_HUB",    # USCIS H-1B employer-level data FY2010-FY2023
    "BLS/":                   "BLS_CES",             # BLS Current Employment Statistics (JSON)
    # ── Reference / metadata (tracked for completeness, no artifacts) ──
    "DOL_Record_Layouts":     "DOL_RECORD_LAYOUTS",  # DOL LCA/PERM record layout PDFs
}


def classify_dataset(rel_path: str) -> str:
    """Classify a file into a canonical dataset bucket based on its path.

    Uses longest-match-wins strategy to avoid collisions when one pattern
    is a substring of another (e.g. 'LCA' vs 'DOL_Record_Layouts/LCA').
    """
    best_match = "UNKNOWN"
    best_len = 0
    for pattern, dataset in DATASET_PATTERNS.items():
        if pattern in rel_path and len(pattern) > best_len:
            best_match = dataset
            best_len = len(pattern)
    return best_match


# ── Dependency graph: dataset → artifacts ────────────────────────────────────

# Maps dataset name → list of (artifact_name, stage, rebuild_command)
DEPENDENCY_GRAPH: Dict[str, List[Tuple[str, int, str]]] = {
    "PERM": [
        ("fact_perm/",                         1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
        ("fact_perm_all.parquet",              1, "python3 scripts/make_presentation_and_snapshot.py"),
        ("fact_perm_unique_case/",             1, "python3 scripts/build_fact_perm_unique_case.py"),
        ("dim_employer.parquet",               1, "python3 scripts/patch_dim_employer_from_fact_perm.py"),
        ("employer_features.parquet",          2, "python3 -m src.features.run_features --paths configs/paths.yaml"),
        ("employer_friendliness_scores.parquet", 3, "python3 -m src.models.run_models --paths configs/paths.yaml"),
        ("employer_friendliness_scores_ml.parquet", 3, "python3 -m src.models.run_models --paths configs/paths.yaml"),
        ("employer_monthly_metrics.parquet",   4, "python3 scripts/make_employer_monthly_metrics.py"),
        ("employer_risk_features.parquet",     4, "python3 scripts/make_employer_risk_features.py"),
        ("soc_demand_metrics.parquet",         4, "python3 scripts/make_soc_demand_metrics.py"),
        ("worksite_geo_metrics.parquet",       4, "python3 scripts/make_worksite_geo_metrics.py"),
    ],
    "LCA": [
        ("fact_lca/",                          1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
        ("soc_demand_metrics.parquet",         4, "python3 scripts/make_soc_demand_metrics.py"),
    ],
    "OEWS": [
        ("fact_oews/",                         1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
        ("fact_oews.parquet",                  1, "python3 scripts/make_presentation_and_snapshot.py"),
        ("salary_benchmarks.parquet",          2, "python3 -m src.features.run_features --paths configs/paths.yaml"),
        ("worksite_geo_metrics.parquet",       4, "python3 scripts/make_worksite_geo_metrics.py"),
    ],
    "VISA_BULLETIN": [
        ("fact_cutoffs/",                      1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
        ("fact_cutoffs_all.parquet",           1, "python3 scripts/make_vb_presentation.py"),
        ("fact_cutoff_trends.parquet",         4, "python3 scripts/make_fact_cutoff_trends.py"),
        ("backlog_estimates.parquet",          4, "python3 scripts/make_backlog_estimates.py"),
        ("category_movement_metrics.parquet",  4, "python3 scripts/make_category_movement_metrics.py"),
        ("pd_forecasts.parquet",               3, "python3 -m src.models.run_models --paths configs/paths.yaml"),
    ],
    "VISA_ISSUANCE": [
        ("fact_visa_issuance.parquet",         1, "python3 scripts/build_fact_visa_issuance.py --downloads {data_root}/Visa_Annual_Reports --out artifacts/tables/fact_visa_issuance.parquet"),
        ("visa_demand_metrics.parquet",        4, "python3 scripts/make_visa_demand_metrics.py"),
    ],
    "VISA_APPLICATIONS": [
        ("fact_visa_applications.parquet",     1, "python3 scripts/build_fact_visa_applications.py"),
    ],
    "NIV_ISSUANCE": [
        ("fact_niv_issuance.parquet",          1, "python3 scripts/build_fact_niv_issuance.py"),
    ],
    "USCIS": [
        ("fact_uscis_approvals.parquet",       1, "python3 scripts/build_fact_uscis_approvals.py --downloads {data_root}/USCIS_IMMIGRATION --out artifacts/tables/fact_uscis_approvals.parquet"),
        ("processing_times_trends.parquet",    4, "python3 scripts/make_processing_times_trends.py"),
    ],
    "DHS_ADMISSIONS": [
        ("fact_dhs_admissions.parquet",        1, "python3 scripts/build_fact_dhs_admissions.py"),
    ],
    "WARN": [
        ("fact_warn_events.parquet",           1, "python3 scripts/build_fact_warn_events.py --downloads {data_root}/WARN --out artifacts/tables/fact_warn_events.parquet"),
    ],
    "VISA_CEILING": [
        ("dim_visa_ceiling.parquet",           1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
    ],
    "WAITING_LIST": [
        ("fact_waiting_list.parquet",          1, "python3 scripts/build_fact_waiting_list.py --downloads {data_root}/DOS_Waiting_List --out artifacts/tables/fact_waiting_list.parquet"),
    ],
    "CODEBOOKS": [
        ("dim_country.parquet",                1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
        ("dim_soc.parquet",                    1, "python3 -m src.curate.run_curate --paths configs/paths.yaml"),
    ],
    # ── New datasets (M17 — 2026-02-25) ──
    "H1B_EMPLOYER_HUB": [
        # USCIS H-1B Employer Hub (discontinued after FY2023)
        # All rows marked is_stale=True, data_weight=0.6
        ("fact_h1b_employer_hub.parquet", 1, "python3 scripts/build_fact_h1b_employer_hub.py"),
    ],
    # USCIS_PROC_TIMES: removed — P1 directory deleted (USCIS page is a Vue.js SPA,
    # no usable data was ever extracted). fact_processing_times.parquet remains as
    # a 0-row stub artifact.
    "BLS_CES": [
        # BLS Current Employment Statistics (nonfarm + private employment)
        ("fact_bls_ces.parquet", 1, "python3 scripts/build_fact_bls_ces.py"),
    ],
    "ACS": [
        # Census ACS — skipped: API returns HTTP 404 (data not yet available)
        # P1 file: acs1_2025_nativity.json contains {"error": "404 Client Error"}
        # No builder until Census publishes 2025 ACS1 data (~Sep 2026)
        ("fact_acs_wages.parquet", 1, "echo 'ACS: skipped — Census API 404'"),
    ],
    "DOL_RECORD_LAYOUTS": [
        # Reference metadata only — record layout PDFs for LCA/PERM parsers
        # No P2 artifacts to rebuild (used manually for parser development)
    ],
}

# File extensions we track
TRACKED_EXTENSIONS = {
    ".xlsx", ".xls", ".csv", ".tsv", ".txt", ".pdf",
    ".zip", ".gz", ".json", ".xml", ".html", ".htm",
    ".parquet", ".dat",
}


# ── Main class ───────────────────────────────────────────────────────────────

class ChangeDetector:
    """Detects changes in P1 downloads and plans incremental P2 rebuilds."""

    MANIFEST_PATH = "artifacts/metrics/p1_manifest.json"

    def __init__(self, paths_config: str = "configs/paths.yaml",
                 project_root: Optional[Path] = None):
        # Resolve project root
        if project_root is None:
            project_root = Path(__file__).resolve().parents[2]
        self.project_root = project_root

        # Load paths config
        config_path = self.project_root / paths_config
        with open(config_path) as f:
            config = yaml.safe_load(f)
        self.data_root = Path(config["data_root"])
        self.artifacts_root = self.project_root / config.get("artifacts_root", "artifacts")

        # Manifest file
        self.manifest_path = self.project_root / self.MANIFEST_PATH
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        # Load existing manifest (or empty)
        self._old_manifest: Dict[str, FileFingerprint] = {}
        self._new_manifest: Dict[str, FileFingerprint] = {}
        if self.manifest_path.exists():
            self._load_manifest()

    # ── Manifest I/O ─────────────────────────────────────────────────────

    def _load_manifest(self):
        """Load the saved manifest from disk."""
        with open(self.manifest_path) as f:
            data = json.load(f)
        self._old_manifest = {
            k: FileFingerprint.from_dict(v) for k, v in data.get("files", {}).items()
        }
        print(f"  Loaded manifest: {len(self._old_manifest)} files "
              f"(saved {data.get('saved_at', '?')})")

    def save_manifest(self):
        """Save the current manifest to disk (call after successful rebuild)."""
        data = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "data_root": str(self.data_root),
            "file_count": len(self._new_manifest),
            "files": {k: v.to_dict() for k, v in self._new_manifest.items()},
        }
        with open(self.manifest_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  Manifest saved: {len(self._new_manifest)} files → {self.manifest_path}")

    # ── Scanning ─────────────────────────────────────────────────────────

    def _scan_downloads(self, compute_hash: bool = False) -> Dict[str, FileFingerprint]:
        """Walk the data_root directory and fingerprint every tracked file."""
        fingerprints: Dict[str, FileFingerprint] = {}
        if not self.data_root.exists():
            print(f"  WARNING: data_root does not exist: {self.data_root}")
            return fingerprints

        for root, dirs, files in os.walk(self.data_root):
            # Skip hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in files:
                if fname.startswith(".") or fname.startswith("_"):
                    continue
                fpath = Path(root) / fname
                ext = fpath.suffix.lower()
                if ext not in TRACKED_EXTENSIONS:
                    continue

                rel = str(fpath.relative_to(self.data_root))
                stat = fpath.stat()
                sha = None
                if compute_hash:
                    sha = self._compute_sha256(fpath)

                fingerprints[rel] = FileFingerprint(
                    rel_path=rel,
                    size=stat.st_size,
                    mtime=stat.st_mtime,
                    sha256=sha,
                    dataset=classify_dataset(rel),
                )
        return fingerprints

    @staticmethod
    def _compute_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
        """Compute SHA-256 hex digest of a file."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    # ── Change detection ─────────────────────────────────────────────────

    def detect_changes(self, compute_hash: bool = False) -> ChangeSet:
        """Compare current P1 downloads against saved manifest.

        Args:
            compute_hash: If True, compute SHA-256 for changed files
                          (slower but catches content-only changes).

        Returns:
            ChangeSet with new, changed, and deleted files.
        """
        print("\n" + "=" * 60)
        print("INCREMENTAL CHANGE DETECTION")
        print("=" * 60)
        print(f"  Data root: {self.data_root}")

        # Scan current state
        t0 = time.time()
        current = self._scan_downloads(compute_hash=False)
        elapsed = time.time() - t0
        print(f"  Scanned {len(current)} files in {elapsed:.1f}s")

        self._new_manifest = current
        changes = ChangeSet()

        old_keys = set(self._old_manifest.keys())
        new_keys = set(current.keys())

        # New files (in current but not in manifest)
        for key in sorted(new_keys - old_keys):
            fp = current[key]
            if compute_hash:
                fp.sha256 = self._compute_sha256(self.data_root / fp.rel_path)
            changes.new_files.append(fp)

        # Deleted files (in manifest but not in current)
        for key in sorted(old_keys - new_keys):
            changes.deleted_files.append(self._old_manifest[key])

        # Changed files (in both but different size or mtime)
        for key in sorted(old_keys & new_keys):
            old_fp = self._old_manifest[key]
            new_fp = current[key]
            if old_fp.size != new_fp.size or abs(old_fp.mtime - new_fp.mtime) > 1.0:
                if compute_hash:
                    new_fp.sha256 = self._compute_sha256(self.data_root / new_fp.rel_path)
                    # If hashes match, it's not really changed (just touched)
                    if old_fp.sha256 and new_fp.sha256 == old_fp.sha256:
                        changes.unchanged_count += 1
                        continue
                changes.changed_files.append((old_fp, new_fp))
            else:
                changes.unchanged_count += 1

        print(f"\n  Result: {changes.summary}")
        if changes.new_files:
            datasets = set(f.dataset for f in changes.new_files)
            print(f"    New files span datasets: {', '.join(sorted(datasets))}")
        if changes.changed_files:
            datasets = set(f[1].dataset for f in changes.changed_files)
            print(f"    Changed files span datasets: {', '.join(sorted(datasets))}")
        if changes.deleted_files:
            datasets = set(f.dataset for f in changes.deleted_files)
            print(f"    Deleted files span datasets: {', '.join(sorted(datasets))}")

        return changes

    # ── Rebuild planning ─────────────────────────────────────────────────

    def plan_rebuild(self, changes: ChangeSet) -> List[RebuildAction]:
        """Given a changeset, determine the minimal set of artifacts to rebuild.

        Returns:
            Ordered list of RebuildActions (sorted by stage, then artifact).
        """
        if not changes.has_changes:
            print("\n  No changes detected — nothing to rebuild.")
            return []

        # Collect affected datasets
        affected_datasets: Dict[str, List[str]] = {}  # dataset → [triggering files]

        for fp in changes.new_files:
            affected_datasets.setdefault(fp.dataset, []).append(f"NEW: {fp.rel_path}")
        for old_fp, new_fp in changes.changed_files:
            affected_datasets.setdefault(new_fp.dataset, []).append(f"CHANGED: {new_fp.rel_path}")
        for fp in changes.deleted_files:
            affected_datasets.setdefault(fp.dataset, []).append(f"DELETED: {fp.rel_path}")

        # Map datasets → artifacts via dependency graph
        seen_artifacts: Set[str] = set()
        actions: List[RebuildAction] = []

        for dataset, trigger_files in sorted(affected_datasets.items()):
            if dataset not in DEPENDENCY_GRAPH:
                print(f"  WARNING: No dependency mapping for dataset '{dataset}' — skipping")
                continue

            for artifact, stage, command in DEPENDENCY_GRAPH[dataset]:
                if artifact not in seen_artifacts:
                    seen_artifacts.add(artifact)
                    # Resolve {data_root} placeholder in command
                    cmd = command.replace("{data_root}", str(self.data_root))
                    actions.append(RebuildAction(
                        artifact=artifact,
                        reason=f"Dataset '{dataset}' has changes",
                        stage=stage,
                        command=cmd,
                        triggered_by=trigger_files[:5],  # limit to 5 examples
                    ))

        # Sort by stage, then artifact name
        actions.sort(key=lambda a: (a.stage, a.artifact))

        # Deduplicate commands (same command may appear for multiple artifacts)
        # Keep unique commands in stage order
        seen_cmds: Set[str] = set()
        deduped: List[RebuildAction] = []
        for action in actions:
            if action.command not in seen_cmds:
                seen_cmds.add(action.command)
                deduped.append(action)
            else:
                # Merge triggered_by into existing action with same command
                for existing in deduped:
                    if existing.command == action.command:
                        existing.triggered_by.extend(action.triggered_by)
                        existing.reason += f"; also rebuilds {action.artifact}"
                        break

        # Print plan
        print(f"\n  Rebuild plan: {len(deduped)} commands affecting {len(seen_artifacts)} artifacts")
        print(f"  Affected datasets: {', '.join(sorted(affected_datasets.keys()))}")
        print()

        stage_names = {1: "CURATE", 2: "FEATURES", 3: "MODELS", 4: "DERIVED"}
        current_stage = 0
        for action in deduped:
            if action.stage != current_stage:
                current_stage = action.stage
                print(f"  --- Stage {current_stage}: {stage_names.get(current_stage, '?')} ---")
            print(f"    [{action.artifact}] {action.reason}")
            print(f"      $ {action.command}")
            if action.triggered_by:
                for tf in action.triggered_by[:3]:
                    print(f"        ← {tf}")

        return deduped

    # ── Execution ────────────────────────────────────────────────────────

    def execute_rebuild(self, actions: List[RebuildAction],
                        dry_run: bool = False) -> Dict[str, bool]:
        """Execute rebuild actions in order.

        Args:
            actions: Ordered list from plan_rebuild()
            dry_run: If True, print commands but don't execute

        Returns:
            Dict mapping command → success (True/False)
        """
        import subprocess

        results: Dict[str, bool] = {}

        print("\n" + "=" * 60)
        print(f"INCREMENTAL REBUILD {'[DRY RUN]' if dry_run else ''}")
        print("=" * 60)

        stage_names = {1: "CURATE", 2: "FEATURES", 3: "MODELS", 4: "DERIVED"}

        for i, action in enumerate(actions, 1):
            stage = stage_names.get(action.stage, "?")
            print(f"\n  [{i}/{len(actions)}] Stage {action.stage} ({stage}): {action.artifact}")
            print(f"    $ {action.command}")

            if dry_run:
                print(f"    → SKIPPED (dry-run)")
                results[action.command] = True
                continue

            try:
                result = subprocess.run(
                    action.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=str(self.project_root),
                    timeout=1800,  # 30 min max per command
                )
                if result.returncode == 0:
                    print(f"    → SUCCESS")
                    results[action.command] = True
                else:
                    print(f"    → FAILED (exit code {result.returncode})")
                    if result.stderr:
                        for line in result.stderr.strip().split("\n")[-5:]:
                            print(f"      {line}")
                    results[action.command] = False
            except subprocess.TimeoutExpired:
                print(f"    → TIMEOUT (30 min)")
                results[action.command] = False
            except Exception as e:
                print(f"    → ERROR: {e}")
                results[action.command] = False

        # Summary
        successes = sum(1 for v in results.values() if v)
        failures = sum(1 for v in results.values() if not v)
        print(f"\n  Rebuild complete: {successes} succeeded, {failures} failed")

        return results


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point for incremental change detection."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Detect P1 changes and plan/execute incremental P2 rebuilds"
    )
    parser.add_argument("--paths", default="configs/paths.yaml",
                        help="Path to paths.yaml config")
    parser.add_argument("--hash", action="store_true",
                        help="Compute SHA-256 for changed files (slower but more accurate)")
    parser.add_argument("--execute", action="store_true",
                        help="Actually execute rebuild commands (default: plan only)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print rebuild commands without executing")
    parser.add_argument("--save-manifest", action="store_true",
                        help="Save manifest after detection (use after successful full build)")
    parser.add_argument("--init", action="store_true",
                        help="Initialize manifest from current P1 state (first run)")
    args = parser.parse_args()

    cd = ChangeDetector(args.paths)

    if args.init:
        print("Initializing manifest from current P1 downloads...")
        cd._new_manifest = cd._scan_downloads(compute_hash=args.hash)
        cd.save_manifest()
        print(f"Done. {len(cd._new_manifest)} files registered.")
        return

    changes = cd.detect_changes(compute_hash=args.hash)

    if not changes.has_changes:
        if args.save_manifest:
            cd.save_manifest()
        return

    actions = cd.plan_rebuild(changes)

    if args.execute or args.dry_run:
        results = cd.execute_rebuild(actions, dry_run=args.dry_run)
        # Save manifest only if all commands succeeded
        if all(results.values()):
            cd.save_manifest()
            print("\n  ✅ All rebuilds succeeded — manifest updated.")
        else:
            print("\n  ⚠️  Some rebuilds failed — manifest NOT updated.")
            print("     Fix failures and re-run, or use --save-manifest to force-update.")
    elif args.save_manifest:
        cd.save_manifest()


if __name__ == "__main__":
    main()
