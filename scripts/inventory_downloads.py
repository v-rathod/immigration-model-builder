#!/usr/bin/env python3
"""
inventory_downloads.py
Walks the downloads root recursively, groups files into canonical dataset buckets,
and writes artifacts/metrics/downloads_inventory.json + downloads_inventory.md.
NON-INTERACTIVE, read-only.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DOWNLOADS_ROOT = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
METRICS_DIR = REPO_ROOT / "artifacts" / "metrics"

OUT_JSON = METRICS_DIR / "downloads_inventory.json"
OUT_MD   = METRICS_DIR / "downloads_inventory.md"

# ---------------------------------------------------------------------------
# Canonical dataset classification rules
# Each entry: (canonical_name, list_of_match_patterns_lower)
# First match wins; patterns are tested against top-level subfolder name (lower).
# ---------------------------------------------------------------------------
DATASET_PATTERNS = [
    ("PERM",                     ["perm"]),
    ("LCA",                      ["lca"]),
    ("BLS_OEWS",                 ["bls_oews", "oews"]),
    ("BLS",                      ["bls"]),
    ("Visa_Bulletin",            ["visa_bulletin", "visa bulletin"]),
    ("Visa_Annual_Reports",      ["visa_annual_reports", "visa_annual"]),
    ("Visa_Statistics",          ["visa_statistics"]),
    ("DOS_Numerical_Limits",     ["dos_numerical_limits", "numerical_limits"]),
    ("DOS_Waiting_List",         ["dos_waiting_list", "waiting_list"]),
    ("USCIS_H1B_Employer_Hub",   ["uscis_h1b_employer_hub", "h1b_employer_hub"]),
    ("USCIS_IMMIGRATION",        ["uscis_immigration"]),
    ("USCIS_Processing_Times",   ["uscis_processing_times", "processing_times"]),
    ("DHS_Yearbook",             ["dhs_yearbook", "dhs"]),
    ("NIV_Statistics",           ["niv_statistics", "niv"]),
    ("TRAC",                     ["trac"]),
    ("WARN",                     ["warn"]),
    ("ACS",                      ["acs"]),
    ("Codebooks",                ["codebooks", "codebook"]),
    ("DOL_Record_Layouts",       ["dol_record_layouts", "record_layouts"]),
]


def classify_folder(folder_name: str) -> str:
    """Map a top-level folder name to a canonical dataset name."""
    lower = folder_name.lower()
    for canonical, patterns in DATASET_PATTERNS:
        for p in patterns:
            if p in lower:
                return canonical
    return f"UNKNOWN:{folder_name}"


def scan_downloads(root: Path) -> list[dict]:
    """Walk root, group top-level dirs into dataset buckets, return inventory list."""
    if not root.exists():
        print(f"WARN: downloads root not found: {root}", file=sys.stderr)
        return []

    # Group top-level directories
    buckets: dict[str, dict] = {}

    top_level_dirs = sorted([d for d in root.iterdir() if d.is_dir()])

    for top_dir in top_level_dirs:
        if top_dir.name.startswith("."):
            continue
        canonical = classify_folder(top_dir.name)

        if canonical not in buckets:
            buckets[canonical] = {
                "dataset":          canonical,
                "folders":          [],
                "files":            0,
                "total_size_bytes": 0,
                "ext_counts":       defaultdict(int),
                "min_mtime":        None,
                "max_mtime":        None,
            }

        bucket = buckets[canonical]
        bucket["folders"].append(str(top_dir.relative_to(root.parent)))

        # Walk subtree
        try:
            for dirpath, dirnames, filenames in os.walk(top_dir):
                # Skip hidden dirs
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                for fname in filenames:
                    if fname.startswith("."):
                        continue
                    fpath = Path(dirpath) / fname
                    try:
                        stat   = fpath.stat()
                        size   = stat.st_size
                        mtime  = stat.st_mtime
                        ext    = fpath.suffix.lower().lstrip(".") or "noext"
                        bucket["files"] += 1
                        bucket["total_size_bytes"] += size
                        bucket["ext_counts"][ext] += 1
                        if bucket["min_mtime"] is None or mtime < bucket["min_mtime"]:
                            bucket["min_mtime"] = mtime
                        if bucket["max_mtime"] is None or mtime > bucket["max_mtime"]:
                            bucket["max_mtime"] = mtime
                    except OSError as e:
                        print(f"WARN: cannot stat {fpath}: {e}", file=sys.stderr)
        except PermissionError as e:
            print(f"WARN: cannot walk {top_dir}: {e}", file=sys.stderr)

    # Serialise defaultdicts and convert timestamps
    result = []
    for bucket in buckets.values():
        bucket["ext_counts"] = dict(bucket["ext_counts"])
        for field in ("min_mtime", "max_mtime"):
            if bucket[field] is not None:
                bucket[field] = datetime.fromtimestamp(
                    bucket[field], tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
        result.append(bucket)

    return sorted(result, key=lambda x: x["dataset"])


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def write_md(inventory: list[dict], path: Path, ts: str) -> None:
    lines = [
        "# Downloads Inventory",
        f"_Generated: {ts}_",
        f"_Downloads root: {DOWNLOADS_ROOT}_",
        "",
        f"**Total datasets detected:** {len(inventory)}  ",
        f"**Total files:** {sum(d['files'] for d in inventory):,}  ",
        f"**Total size:** {fmt_size(sum(d['total_size_bytes'] for d in inventory))}",
        "",
        "| # | Dataset | Folders | Files | Size | Extensions | Newest File |",
        "|---|---------|---------|-------|------|------------|-------------|",
    ]
    for i, d in enumerate(inventory, 1):
        folders_str = ", ".join(Path(f).name for f in d["folders"])
        ext_str = ", ".join(
            f"{k}:{v}" for k, v in sorted(d["ext_counts"].items(), key=lambda x: -x[1])
        )
        newest = d["max_mtime"] or "—"
        lines.append(
            f"| {i} | {d['dataset']} | {folders_str} | {d['files']:,} | "
            f"{fmt_size(d['total_size_bytes'])} | {ext_str} | {newest} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Scanning: {DOWNLOADS_ROOT}")
    inventory = scan_downloads(DOWNLOADS_ROOT)

    print(f"Datasets found: {len(inventory)}")
    for d in inventory:
        print(f"  {d['dataset']:35s}  files={d['files']:4d}  size={fmt_size(d['total_size_bytes'])}")

    OUT_JSON.write_text(
        json.dumps({"generated": ts, "downloads_root": str(DOWNLOADS_ROOT), "datasets": inventory},
                   indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Written: {OUT_JSON}")

    write_md(inventory, OUT_MD, ts)
    print(f"Written: {OUT_MD}")


if __name__ == "__main__":
    main()
