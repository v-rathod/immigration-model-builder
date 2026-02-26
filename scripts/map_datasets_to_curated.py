#!/usr/bin/env python3
"""
map_datasets_to_curated.py
Loads downloads_inventory.json, checks for curated presence in artifacts/tables,
and writes dataset_coverage_matrix.md + dataset_coverage_matrix.json.
NON-INTERACTIVE, read-only (no parquet scans beyond quick metadata).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT   = Path(__file__).resolve().parents[1]
TABLES_DIR  = REPO_ROOT / "artifacts" / "tables"
METRICS_DIR = REPO_ROOT / "artifacts" / "metrics"

IN_INVENTORY = METRICS_DIR / "downloads_inventory.json"
OUT_MATRIX   = METRICS_DIR / "dataset_coverage_matrix.md"
OUT_JSON     = METRICS_DIR / "dataset_coverage_matrix.json"

# ---------------------------------------------------------------------------
# Mapping: canonical dataset → expected curated outputs
# Each entry: (table_name_or_glob, display_name)
# ---------------------------------------------------------------------------
CURATED_MAP: dict[str, list[tuple[str, str]]] = {
    "PERM": [
        ("fact_perm",             "fact_perm (partitioned)"),
        ("fact_perm_unique_case", "fact_perm_unique_case"),
        ("fact_perm_all.parquet", "fact_perm_all"),
    ],
    "LCA": [
        ("fact_lca",              "fact_lca (partitioned)"),
    ],
    "BLS_OEWS": [
        ("fact_oews",             "fact_oews (partitioned)"),
        ("fact_oews.parquet",     "fact_oews (flat)"),
        ("salary_benchmarks.parquet", "salary_benchmarks"),
        ("oews_wages.parquet",    "oews_wages"),
    ],
    "BLS": [
        ("fact_oews",             "fact_oews (via BLS raw)"),
        ("salary_benchmarks.parquet", "salary_benchmarks"),
    ],
    "Visa_Bulletin": [
        ("fact_cutoffs",          "fact_cutoffs (partitioned)"),
        ("fact_cutoffs_all.parquet", "fact_cutoffs_all"),
        ("fact_cutoff_trends.parquet", "fact_cutoff_trends"),
        ("backlog_estimates.parquet",  "backlog_estimates"),
        ("category_movement_metrics.parquet", "category_movement_metrics"),
        ("visa_bulletin.parquet", "visa_bulletin"),
    ],
    "Visa_Annual_Reports":    [],  # gap
    "Visa_Statistics":        [],  # gap
    "DOS_Numerical_Limits":   [],  # gap
    "DOS_Waiting_List":       [],  # gap
    "USCIS_H1B_Employer_Hub": [
        ("employer_features.parquet",              "employer_features"),
        ("employer_scores.parquet",               "employer_scores"),
        ("employer_friendliness_scores.parquet",  "employer_friendliness_scores"),
        ("employer_friendliness_scores_ml.parquet","employer_friendliness_scores_ml"),
        ("employer_monthly_metrics.parquet",       "employer_monthly_metrics"),
        ("dim_employer.parquet",                   "dim_employer"),
    ],
    "USCIS_IMMIGRATION":      [],  # gap
    # USCIS_Processing_Times: P1 source dir deleted (Vue.js SPA, no usable data)
    "DHS_Yearbook":           [],  # gap
    "NIV_Statistics":         [],  # gap
    "TRAC":                   [],  # gap
    "WARN":                   [],  # gap
    "ACS":                    [],  # gap
    "Codebooks":              [],  # reference only
    "DOL_Record_Layouts":     [],  # reference only
}

# Notes for gap/reference datasets
GAP_NOTES: dict[str, str] = {
    "Visa_Annual_Reports":    "Visa issuance totals by country/category — complements DOS_Numerical_Limits",
    "Visa_Statistics":        "DOS visa applications/refusals — NIV demand signal",
    "DOS_Numerical_Limits":   "Annual per-country ceiling data — key for backlog projection",
    "DOS_Waiting_List":       "Priority date waiting-list reports — direct backlog source",
    "USCIS_IMMIGRATION":      "USCIS immigration statistics reports (annual)",
    "DHS_Yearbook":           "DHS Yearbook of Immigration Statistics — historical baselines",
    "NIV_Statistics":         "Non-immigrant visa issuance counts by category",
    "TRAC":                   "TRAC FOIA records — adjudication timelines and denial rates",
    "WARN":                   "WARN Act layoff notices — employer-level workforce signal",
    "ACS":                    "Census ACS occupation/wage data — salary benchmark supplement",
    "Codebooks":              "Reference only — schema codebooks for DOL/OEWS files",
    "DOL_Record_Layouts":     "Reference only — column layout specs for LCA/PERM raw files",
    "BLS":                    "BLS raw feeds partially ingested via BLS_OEWS pipeline",
    # USCIS_Processing_Times: P1 dir deleted (SPA, no data); processing_times_trends built from USCIS_IMMIGRATION
}

# Gap priority + placement notes
GAP_PLAN: dict[str, dict] = {
    "DOS_Numerical_Limits": {
        "priority": 1,
        "placement": "P2.1",
        "rationale": "Annual per-country visa ceilings are foundational for backlog projections; already downloaded (15 folders).",
        "idea": "Parse Excel annual-limits files → `dim_visa_ceiling` table; join to `backlog_estimates`.",
    },
    "DOS_Waiting_List": {
        "priority": 2,
        "placement": "P2.1",
        "rationale": "Waiting-list priority-date data directly extends backlog estimation accuracy.",
        "idea": "Parse DOS waiting-list PDFs/CSVs → `fact_waiting_list` with (country, category, cutoff_date, applicants).",
    },
    "Visa_Annual_Reports": {
        "priority": 3,
        "placement": "P2.1",
        "rationale": "Yearly visa-issuance totals validate fact_cutoffs demand estimates.",
        "idea": "Parse annual-report Excel tables → `fact_visa_issuance`; aggregate by fy/category/country.",
    },
    "Visa_Statistics": {
        "priority": 4,
        "placement": "P2.1",
        "rationale": "Application/refusal stats are a leading indicator of EB demand.",
        "idea": "Parse DOS NIV statistics CSVs → `fact_visa_applications`.",
    },
    "NIV_Statistics": {
        "priority": 5,
        "placement": "P2.1",
        "rationale": "Non-immigrant visa trends inform H-1B/L1 demand signal for employer scoring.",
        "idea": "Parse NIV Excel → `fact_niv_issuance` (visa_class, country, fy, count).",
    },
    "USCIS_IMMIGRATION": {
        "priority": 6,
        "placement": "P2.2",
        "rationale": "Official USCIS approval/denial statistics cross-validate fact_perm and fact_lca.",
        "idea": "Parse USCIS annual immigration data Excel → `fact_uscis_approvals`.",
    },
    "DHS_Yearbook": {
        "priority": 7,
        "placement": "P2.2",
        "rationale": "Long historical series (1820+) enables cohort-level backlog trend analysis.",
        "idea": "Ingest DHS Yearbook csvs → `fact_dhs_admissions` (fy, category, country, n_admitted).",
    },
    "TRAC": {
        "priority": 8,
        "placement": "P3",
        "rationale": "TRAC adjudication timelines enrich employer-level risk scoring.",
        "idea": "FOIA-derived data — parse TRAC CSVs → `fact_trac_adjudications`; link to dim_employer.",
    },
    "WARN": {
        "priority": 9,
        "placement": "P3",
        "rationale": "Layoff notices are a risk signal for employer sponsorship stability.",
        "idea": "Parse WARN state files → `fact_warn_events`; fuzzy-join to dim_employer.",
    },
    "ACS": {
        "priority": 10,
        "placement": "P3",
        "rationale": "ACS occupation wage distributions supplement OEWS benchmarks.",
        "idea": "Parse Census ACS PUMS → `fact_acs_wages`; join on SOC code.",
    },
}


def get_table_meta(table_ref: str) -> dict | None:
    """
    Return lightweight metadata for a curated table reference.
    Tries: parquet file stat, or directory listing for partitioned tables.
    Returns None if not found.
    """
    path = TABLES_DIR / table_ref
    if path.is_file():
        stat = path.stat()
        return {
            "path":         str(path.relative_to(REPO_ROOT)),
            "row_count":    None,  # skip full parquet scan
            "size_bytes":   stat.st_size,
            "last_updated": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    elif path.is_dir():
        parts = list(path.rglob("*.parquet"))
        if parts:
            mtimes = [p.stat().st_mtime for p in parts]
            sizes  = [p.stat().st_size  for p in parts]
            return {
                "path":         str(path.relative_to(REPO_ROOT)),
                "row_count":    None,
                "size_bytes":   sum(sizes),
                "last_updated": datetime.fromtimestamp(max(mtimes), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "partition_files": len(parts),
            }
    return None


def build_coverage(inventory: list[dict]) -> list[dict]:
    """Cross-reference each inventoried dataset with its curated outputs."""
    # Index inventory by canonical name
    inv_by_name = {d["dataset"]: d for d in inventory}

    # Also handle UNKNOWN: datasets
    all_datasets = sorted(
        set(list(CURATED_MAP.keys()) + [d["dataset"] for d in inventory])
    )

    results = []
    for ds in all_datasets:
        inv = inv_by_name.get(ds)
        expected = CURATED_MAP.get(ds, None)

        found_outputs: list[dict] = []
        if expected is not None:
            for table_ref, display in expected:
                meta = get_table_meta(table_ref)
                if meta:
                    found_outputs.append({"table": display, **meta})

        curated_present = len(found_outputs) > 0
        is_gap = (inv is not None) and (not curated_present) and (ds not in ("Codebooks", "DOL_Record_Layouts"))

        results.append({
            "dataset":         ds,
            "downloaded":      inv is not None,
            "download_files":  inv["files"]            if inv else 0,
            "download_size":   inv["total_size_bytes"] if inv else 0,
            "download_folders": [Path(f).name for f in inv["folders"]] if inv else [],
            "curated_present": curated_present,
            "outputs":         found_outputs,
            "gap":             is_gap,
            "note":            GAP_NOTES.get(ds, ""),
        })

    return results


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def write_matrix_md(coverage: list[dict], ts: str, path: Path) -> None:
    n_downloaded = sum(1 for d in coverage if d["downloaded"])
    n_curated    = sum(1 for d in coverage if d["curated_present"])
    n_gaps       = sum(1 for d in coverage if d["gap"])

    lines = [
        "# Dataset Coverage Matrix (P2 vs. Downloads)",
        f"_Last generated: {ts}_",
        "",
        "## Summary",
        f"- **Total datasets tracked:** {len(coverage)}",
        f"- **Datasets with downloads present:** {n_downloaded}",
        f"- **Datasets curated in P2:** {n_curated}",
        f"- **Gap datasets (downloaded, not yet curated):** {n_gaps}",
        "",
        "## Coverage Matrix",
        "",
        "| Dataset | Downloaded? | Files | Size | Curated? | Curated Outputs | Notes |",
        "|---------|-------------|-------|------|----------|-----------------|-------|",
    ]

    for d in coverage:
        dl_flag  = "✅" if d["downloaded"] else "—"
        cur_flag = "✅" if d["curated_present"] else ("⚠️ GAP" if d["gap"] else "— ref")
        files    = f"{d['download_files']:,}" if d["downloaded"] else "—"
        size     = fmt_size(d["download_size"]) if d["downloaded"] else "—"

        if d["outputs"]:
            outputs_str = "<br>".join(
                f"`{o['table']}` ({fmt_size(o['size_bytes'])})" for o in d["outputs"]
            )
        else:
            outputs_str = "—"

        note = d["note"] or "—"
        lines.append(
            f"| {d['dataset']} | {dl_flag} | {files} | {size} | {cur_flag} | {outputs_str} | {note} |"
        )

    lines += [
        "",
        "## Gap Plan (prioritized)",
        "",
        "_Datasets that are downloaded but have no curated P2 output yet._",
        "",
    ]
    for ds, plan in sorted(GAP_PLAN.items(), key=lambda x: x[1]["priority"]):
        # Only emit if actually a gap
        match = next((d for d in coverage if d["dataset"] == ds and d["gap"]), None)
        if not match:
            continue
        inv_note = f"({match['download_files']:,} files, {fmt_size(match['download_size'])})"
        lines += [
            f"### {plan['priority']}. {ds} — {plan['placement']}  {inv_note}",
            f"**Why it matters:** {plan['rationale']}",
            f"**Ingestion idea:** {plan['idea']}",
            "",
        ]

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    if not IN_INVENTORY.exists():
        print(f"ERROR: {IN_INVENTORY} not found — run inventory_downloads.py first", flush=True)
        raise SystemExit(1)

    with IN_INVENTORY.open() as f:
        inv_data = json.load(f)

    inventory = inv_data["datasets"]
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Loaded {len(inventory)} datasets from inventory.")
    coverage = build_coverage(inventory)

    n_curated = sum(1 for d in coverage if d["curated_present"])
    n_gaps    = sum(1 for d in coverage if d["gap"])
    print(f"Curated: {n_curated}/{len(coverage)}   Gaps: {n_gaps}")

    for d in coverage:
        status = "CUR" if d["curated_present"] else ("GAP" if d["gap"] else "REF")
        out_names = ", ".join(o["table"] for o in d["outputs"]) or "—"
        print(f"  [{status}] {d['dataset']:35s}  outputs={out_names}")

    write_matrix_md(coverage, ts, OUT_MATRIX)
    print(f"Written: {OUT_MATRIX}")

    OUT_JSON.write_text(
        json.dumps({"generated": ts, "coverage": coverage}, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Written: {OUT_JSON}")


if __name__ == "__main__":
    main()
