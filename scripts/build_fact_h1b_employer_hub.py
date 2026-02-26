#!/usr/bin/env python3
"""
build_fact_h1b_employer_hub.py
Parse USCIS H-1B Employer Hub CSVs → fact_h1b_employer_hub.parquet

Schema:
  fiscal_year          INT    — FY of the petition data
  employer_name        TEXT   — raw employer name from USCIS
  initial_approvals    INT    — initial H-1B petition approvals
  initial_denials      INT    — initial H-1B petition denials
  continuing_approvals INT    — continuing H-1B petition approvals
  continuing_denials   INT    — continuing H-1B petition denials
  naics_code           TEXT   — 2-digit NAICS sector code
  tax_id               TEXT   — employer Tax ID (masked by USCIS, 4-digit)
  state                TEXT   — employer state (2-letter)
  city                 TEXT   — employer city
  zip_code             TEXT   — employer ZIP code
  total_petitions      INT    — computed: sum of all 4 petition columns
  approval_rate        FLOAT  — computed: (init_app + cont_app) / total
  is_stale             BOOL   — always True (program discontinued after FY2023)
  data_weight          FLOAT  — 0.6 for historical weighting (stale source)
  source_file          TEXT   — filename parsed from
  ingested_at          TEXT   — UTC ISO-8601 timestamp

Data source:
  USCIS H-1B Employer Data Hub (discontinued)
  FY2010–FY2023, 14 CSV files
  ~763K rows total

IMPORTANT: This data source was discontinued by USCIS after FY2023.
  All records are marked is_stale=True and data_weight=0.6 to reduce
  influence in downstream models while preserving historical context.

Primary Key: (fiscal_year, employer_name, state, city, naics_code)
"""
import argparse
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Column aliases across fiscal years — USCIS changed naming slightly
COL_MAP = {
    # fiscal_year
    "fiscal year":            "fiscal_year",
    "fiscal_year":            "fiscal_year",
    # employer
    "employer":               "employer_name",
    # initial approvals (plural vs singular)
    "initial approvals":      "initial_approvals",
    "initial approval":       "initial_approvals",
    # initial denials
    "initial denials":        "initial_denials",
    "initial denial":         "initial_denials",
    # continuing approvals
    "continuing approvals":   "continuing_approvals",
    "continuing approval":    "continuing_approvals",
    # continuing denials
    "continuing denials":     "continuing_denials",
    "continuing denial":      "continuing_denials",
    # naics
    "naics":                  "naics_code",
    # tax id
    "tax id":                 "tax_id",
    # state
    "state":                  "state",
    # city
    "city":                   "city",
    # zip
    "zip":                    "zip_code",
    "zip code":               "zip_code",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names using the alias map."""
    rename = {}
    for col in df.columns:
        key = col.strip().lower().replace("_", " ")
        if key in COL_MAP:
            rename[col] = COL_MAP[key]
        else:
            # Try removing extra whitespace
            key2 = re.sub(r"\s+", " ", key)
            if key2 in COL_MAP:
                rename[col] = COL_MAP[key2]
    df = df.rename(columns=rename)
    return df


def parse_h1b_csv(path: pathlib.Path) -> pd.DataFrame:
    """Parse a single H-1B Employer Hub CSV file."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    # Extract FY from filename: H1B_Employer_Data_FY2023.csv
    fy_match = re.search(r"FY(\d{4})", fname)
    file_fy = int(fy_match.group(1)) if fy_match else None

    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as e:
        log.warning("Error reading %s: %s", fname, e)
        return pd.DataFrame()

    if df.empty:
        log.warning("Empty file: %s", fname)
        return pd.DataFrame()

    df = normalize_columns(df)

    # Ensure fiscal_year column exists
    if "fiscal_year" not in df.columns and file_fy:
        df["fiscal_year"] = str(file_fy)

    # Required columns check
    required = ["fiscal_year", "employer_name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.warning("Missing columns in %s: %s (have: %s)", fname, missing, list(df.columns))
        return pd.DataFrame()

    # Numeric columns
    int_cols = ["initial_approvals", "initial_denials",
                "continuing_approvals", "continuing_denials"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")

    # Clean text columns
    for tc in ["employer_name", "state", "city", "zip_code", "naics_code", "tax_id"]:
        if tc in df.columns:
            df[tc] = df[tc].fillna("").astype(str).str.strip()
        else:
            df[tc] = ""

    # Remove rows with no employer name (USCIS redacted rows)
    df = df[df["employer_name"].str.len() > 0].copy()

    # Computed columns
    df["total_petitions"] = (
        df["initial_approvals"] + df["initial_denials"] +
        df["continuing_approvals"] + df["continuing_denials"]
    )
    df["approval_rate"] = 0.0
    mask = df["total_petitions"] > 0
    df.loc[mask, "approval_rate"] = (
        (df.loc[mask, "initial_approvals"] + df.loc[mask, "continuing_approvals"]) /
        df.loc[mask, "total_petitions"]
    ).round(4)

    # Stale data markers (discontinued after FY2023)
    df["is_stale"] = True
    df["data_weight"] = 0.6

    # Provenance
    df["source_file"] = fname
    df["ingested_at"] = now_ts

    log.info("  %s: %d rows (FY%s)", fname, len(df), file_fy or "?")
    return df


def main():
    ap = argparse.ArgumentParser(description="Build fact_h1b_employer_hub from USCIS H-1B Employer Hub CSVs")
    ap.add_argument("--downloads", default=None,
                    help="Path to USCIS_H1B_Employer_Hub/raw/ directory")
    ap.add_argument("--out", default=None,
                    help="Output parquet path")
    args = ap.parse_args()

    # Resolve paths from configs/paths.yaml if not provided
    if args.downloads is None or args.out is None:
        try:
            import yaml
            cfg = yaml.safe_load(open("configs/paths.yaml"))
            data_root = cfg.get("data_root", "")
            artifacts_root = cfg.get("artifacts_root", "./artifacts")
        except Exception:
            data_root = "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"
            artifacts_root = "./artifacts"

        if args.downloads is None:
            args.downloads = str(pathlib.Path(data_root) / "USCIS_H1B_Employer_Hub" / "raw")
        if args.out is None:
            args.out = str(pathlib.Path(artifacts_root) / "tables" / "fact_h1b_employer_hub.parquet")

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("=== Building fact_h1b_employer_hub ===")
    log.info("Source: %s", droot)
    log.info("Output: %s", out_path)

    # Discover CSV files
    csv_files = sorted(droot.glob("H1B_Employer_Data_FY*.csv"))
    if not csv_files:
        # Try broader search
        csv_files = sorted(droot.rglob("*.csv"))

    log.info("Found %d CSV files", len(csv_files))

    if not csv_files:
        log.warning("No H1B Employer Hub CSV files found — writing empty stub")
        schema_cols = [
            "fiscal_year", "employer_name", "initial_approvals", "initial_denials",
            "continuing_approvals", "continuing_denials", "naics_code", "tax_id",
            "state", "city", "zip_code", "total_petitions", "approval_rate",
            "is_stale", "data_weight", "source_file", "ingested_at",
        ]
        pd.DataFrame(columns=schema_cols).to_parquet(out_path, index=False)
        log.info("Wrote empty stub to %s", out_path)
        return

    # Parse all files
    frames = []
    for f in csv_files:
        df = parse_h1b_csv(f)
        if len(df) > 0:
            frames.append(df)

    if not frames:
        log.error("All files failed to parse — writing empty stub")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    merged = pd.concat(frames, ignore_index=True)
    log.info("Total rows before dedup: %d", len(merged))

    # Define primary key and dedup
    pk = ["fiscal_year", "employer_name", "state", "city", "naics_code"]
    before = len(merged)
    merged = merged.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    deduped = before - len(merged)
    if deduped > 0:
        log.info("Removed %d duplicate rows", deduped)

    # Sort
    merged = merged.sort_values(["fiscal_year", "employer_name", "state"]).reset_index(drop=True)

    # Validate PK uniqueness
    dup_count = merged.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not fully unique: %d remaining duplicates", dup_count)
    else:
        log.info("PK is unique (%d rows)", len(merged))

    # Final column order
    out_cols = [
        "fiscal_year", "employer_name", "initial_approvals", "initial_denials",
        "continuing_approvals", "continuing_denials", "naics_code", "tax_id",
        "state", "city", "zip_code", "total_petitions", "approval_rate",
        "is_stale", "data_weight", "source_file", "ingested_at",
    ]
    for c in out_cols:
        if c not in merged.columns:
            merged[c] = None
    merged = merged[out_cols]

    # Write
    merged.to_parquet(out_path, index=False)

    # Summary stats
    fy_range = f"FY{merged['fiscal_year'].min()}-FY{merged['fiscal_year'].max()}"
    log.info("=== DONE ===")
    log.info("  Rows:       %s", f"{len(merged):,}")
    log.info("  FY range:   %s", fy_range)
    log.info("  Employers:  %s", f"{merged['employer_name'].nunique():,}")
    log.info("  States:     %d", merged["state"].nunique())
    log.info("  Stale:      100%% (data_weight=0.6)")
    log.info("  Output:     %s", out_path)


if __name__ == "__main__":
    main()
