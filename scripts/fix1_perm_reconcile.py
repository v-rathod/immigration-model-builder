#!/usr/bin/env python3
"""
FIX 1: PERM Reconciliation — quarantine, harmonize, dedupe, partition.
Reads data_root/artifacts_root from configs/paths.yaml.
"""
import hashlib
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


def load_config():
    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    with open("configs/schemas.yml") as f:
        schemas = yaml.safe_load(f)
    with open("configs/layouts/employer.yml") as f:
        employer_layout = yaml.safe_load(f)
    return paths, schemas, employer_layout


def normalize_employer(name, layout):
    if pd.isna(name) or not isinstance(name, str):
        return ""
    n = name.lower().strip()
    for c in layout.get("punctuation_to_strip", []):
        n = n.replace(c, " ")
    for s in layout.get("suffixes", []):
        n = re.sub(r'\b' + re.escape(s.lower()) + r'\.?\s*$', '', n, flags=re.IGNORECASE)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def employer_id(normalized):
    if not normalized:
        return hashlib.sha1(b"UNKNOWN").hexdigest()
    return hashlib.sha1(normalized.encode('utf-8')).hexdigest()


def derive_fy(received_date):
    if pd.isna(received_date):
        return 0
    try:
        ts = pd.Timestamp(received_date)
        if pd.isna(ts):
            return 0
        return ts.year + 1 if ts.month >= 10 else ts.year
    except Exception:
        return 0


# Flexible column mapping per FY (handles header drift)
COLUMN_ALIASES = {
    'case_number':   ['CASE_NUMBER', 'CASE_NO', 'case_number'],
    'case_status':   ['CASE_STATUS', 'STATUS', 'case_status', 'APPROVAL_STATUS'],
    'received_date': ['RECEIVED_DATE', 'CASE_RECEIVED_DATE', 'received_date'],
    'decision_date': ['DECISION_DATE', 'CASE_DECISION_DATE', 'decision_date'],
    'employer_name': ['EMPLOYER_NAME', 'EMP_BUSINESS_NAME', 'EMPLOYER_BUSINESS_NAME',
                      'employer_name', 'EMPLOYER_NAME_1'],
    'employer_country': ['EMPLOYER_COUNTRY', 'EMP_COUNTRY', 'COUNTRY_OF_CITIZENSHIP',
                         'employer_country', 'EMPLOYER_COUNTRY_OF_CITZENSHIP'],
    'soc_code':      ['PW_SOC_CODE', 'PWD_SOC_CODE', 'PW_SOC', 'soc_code',
                      'PW_JOB_TITLE_9089', 'SOC_CODE'],
    'job_title':     ['JOB_INFO_JOB_TITLE', 'JOB_TITLE', 'PW_JOB_TITLE_9089', 'job_title'],
    'wage_offer_from': ['WAGE_OFFER_FROM_9089', 'WAGE_OFFERED_FROM', 'PW_AMOUNT_9089',
                        'JOB_OPP_WAGE_FROM', 'wage_offer_from', 'WAGE_RATE_OF_PAY_FROM'],
    'wage_offer_to': ['WAGE_OFFER_TO_9089', 'WAGE_OFFERED_TO', 'JOB_OPP_WAGE_TO',
                      'wage_offer_to', 'WAGE_RATE_OF_PAY_TO'],
    'wage_offer_unit': ['WAGE_OFFER_UNIT_OF_PAY_9089', 'WAGE_UNIT_OF_PAY',
                      'JOB_OPP_WAGE_PER', 'PW_UNIT_OF_PAY', 'wage_unit', 'wage_offer_unit',
                      'WAGE_UNIT_OF_PAY_9089'],
    'worksite_city': ['WORKSITE_CITY', 'PRIMARY_WORKSITE_CITY', 'WORK_CITY',
                      'worksite_city', 'WORKSITE_CITY_1'],
    'worksite_state':['WORKSITE_STATE', 'PRIMARY_WORKSITE_STATE', 'WORK_STATE',
                      'worksite_state', 'WORKSITE_STATE_1'],
    'naics_code':    ['NAICS_CODE', 'NAICS_2007_US_CODE', 'NAIC_CODE',
                      'naics_code', 'NAICS_US_CODE'],
    'is_fulltime':   ['OTHER_REQ_IS_FULLTIME_EMP', 'FULL_TIME_POSITION',
                      'FULL_TIME_POS', 'is_fulltime'],
    'worksite_postal': ['PRIMARY_WORKSITE_POSTAL_CODE', 'WORKSITE_POSTAL_CODE',
                        'worksite_postal'],
    'area_code':     ['PRIMARY_WORKSITE_BLS_AREA', 'worksite_area', 'area_code'],
}


def resolve_col(df_columns, canonical):
    """Find the actual column name for a canonical name."""
    aliases = COLUMN_ALIASES.get(canonical, [canonical])
    cols_lower = {c.lower(): c for c in df_columns}
    for alias in aliases:
        if alias in df_columns:
            return alias
        if alias.lower() in cols_lower:
            return cols_lower[alias.lower()]
    return None


def main():
    print("=" * 70)
    print("FIX 1: PERM RECONCILIATION")
    print("=" * 70)

    paths, schemas, employer_layout = load_config()
    data_root = Path(paths["data_root"])
    artifacts_root = Path(paths["artifacts_root"])
    fact_perm_dir = artifacts_root / "tables" / "fact_perm"
    quarantine_dir = artifacts_root / "_quarantine" / "fact_perm"
    backup_dir = artifacts_root / "_backup" / "fact_perm" / datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_dir = artifacts_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / "fact_perm_reconcile.log"
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    # ── A) Quarantine legacy single-file outputs ─────────────────────
    log("\n[A] Quarantine legacy single-file outputs")
    for legacy in [artifacts_root / "tables" / "fact_perm.parquet",
                   artifacts_root / "tables" / "fact_perm_test.parquet",
                   artifacts_root / "tables" / "fact_perm_single_file_backup.parquet",
                   artifacts_root / "tables" / "perm_cases.parquet"]:
        if legacy.exists():
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            dest = quarantine_dir / legacy.name
            shutil.move(str(legacy), str(dest))
            log(f"  Quarantined: {legacy.name} → {dest}")

    # Also quarantine __HIVE_DEFAULT_PARTITION__ if present
    hive_default = fact_perm_dir / "__HIVE_DEFAULT_PARTITION__"
    if hive_default.exists():
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(hive_default), str(quarantine_dir / "__HIVE_DEFAULT_PARTITION__"))
        log("  Quarantined: __HIVE_DEFAULT_PARTITION__")

    # ── B–E) Read PERM Excel files, harmonize, dedupe, write ────────
    log("\n[B-E] Loading PERM Excel files, harmonize columns, dedupe, write partitions")

    perm_base = data_root / "PERM" / "PERM"
    perm_files = []
    for fy_dir in sorted(perm_base.iterdir()):
        if not fy_dir.is_dir() or not fy_dir.name.startswith("FY"):
            continue
        fy_match = re.search(r'FY(\d{4})', fy_dir.name)
        if not fy_match:
            continue
        fy = int(fy_match.group(1))
        for pattern in ["PERM_Disclosure_Data_*.xlsx", "PERM_FY*.xlsx", "PERM_*.xlsx"]:
            for f in fy_dir.glob(pattern):
                if (fy, f) not in perm_files:
                    perm_files.append((fy, f))
    perm_files.sort(key=lambda x: x[0])
    log(f"  Found {len(perm_files)} PERM file(s) across FY{perm_files[0][0]}-FY{perm_files[-1][0]}")

    ingested_at = datetime.now(timezone.utc)
    all_chunks = []
    fy_stats = {}  # fy → {rows_before, rows_after, dedup_count, pct_employer_null, pct_soc_null}

    for fy, fpath in perm_files:
        log(f"\n  Processing FY{fy}: {fpath.name}")
        try:
            df = pd.read_excel(fpath)
        except Exception as e:
            log(f"    WARN: Cannot read {fpath.name}: {e}")
            continue

        rows_before = len(df)
        log(f"    Raw rows: {rows_before}")

        # Map columns to canonical names
        rename_map = {}
        for canonical in COLUMN_ALIASES:
            actual = resolve_col(df.columns, canonical)
            if actual and actual != canonical:
                rename_map[actual] = canonical
        df = df.rename(columns=rename_map)

        # Ensure canonical columns exist (set missing to null)
        canonical_cols = ['case_number', 'case_status', 'decision_date', 'received_date',
                          'employer_name', 'employer_country', 'soc_code', 'job_title',
                          'wage_offer_from', 'wage_offer_to', 'wage_offer_unit',
                          'worksite_city', 'worksite_state', 'naics_code',
                          'is_fulltime', 'worksite_postal', 'area_code']
        for col in canonical_cols:
            if col not in df.columns:
                df[col] = None

        # Parse dates
        df['decision_date'] = pd.to_datetime(df['decision_date'], errors='coerce')
        df['received_date'] = pd.to_datetime(df['received_date'], errors='coerce')

        # Derive fiscal_year from received_date (fallback to directory FY)
        df['fiscal_year'] = df['received_date'].apply(derive_fy)
        df.loc[df['fiscal_year'] == 0, 'fiscal_year'] = fy

        # Normalize employer / compute employer_id
        df['employer_name_raw'] = df['employer_name']
        df['_norm'] = df['employer_name'].apply(lambda x: normalize_employer(x, employer_layout))
        df['employer_id'] = df['_norm'].apply(employer_id)
        df.drop(columns=['_norm'], inplace=True)

        # Normalize SOC code
        def norm_soc(raw):
            if pd.isna(raw):
                return None
            raw = str(raw).strip().replace(' ', '')
            if re.match(r'^\d{2}-\d{4}$', raw):
                return raw
            cleaned = raw.replace('-', '')
            if re.match(r'^\d{6}$', cleaned):
                return f"{cleaned[:2]}-{cleaned[2:]}"
            return raw  # keep raw for logging
        df['soc_code'] = df['soc_code'].apply(norm_soc)

        # audit_flag (Y/N) — presence of audit-related columns
        df['audit_flag'] = 'N'

        # Rename wage_offer_unit if still named wage_unit
        if 'wage_unit' in df.columns and 'wage_offer_unit' not in df.columns:
            df.rename(columns={'wage_unit': 'wage_offer_unit'}, inplace=True)

        # Numeric wages
        df['wage_offer_from'] = pd.to_numeric(df['wage_offer_from'], errors='coerce')
        df['wage_offer_to'] = pd.to_numeric(df['wage_offer_to'], errors='coerce')

        # Provenance
        rel_source = f"PERM/PERM/FY{fy}/{fpath.name}"
        df['source_file'] = rel_source
        df['ingested_at'] = ingested_at
        df['record_layout_version'] = f"FY{fy}"

        # ── C) Dedupe ──────────────────────────────────────────
        has_case_number = df['case_number'].notna().sum() > len(df) * 0.5
        if has_case_number:
            # Sort by decision_date desc (latest first), then non-null count desc
            df['_nonnull'] = df.notna().sum(axis=1)
            df = df.sort_values(['case_number', 'decision_date', '_nonnull'],
                                ascending=[True, False, False])
            df = df.drop_duplicates(subset=['fiscal_year', 'case_number'], keep='first')
            df.drop(columns=['_nonnull'], inplace=True)
        else:
            # Deterministic hash key
            hash_cols = ['employer_id', 'soc_code', 'decision_date', 'worksite_state',
                         'wage_offer_from', 'wage_offer_unit']
            def make_hash(row):
                parts = [str(row.get(c, '')) for c in hash_cols]
                return hashlib.sha256('|'.join(parts).encode()).hexdigest()
            df['_hashkey'] = df.apply(make_hash, axis=1)
            df['_nonnull'] = df.notna().sum(axis=1)
            df = df.sort_values(['_hashkey', 'decision_date', '_nonnull'],
                                ascending=[True, False, False])
            df = df.drop_duplicates(subset=['fiscal_year', '_hashkey'], keep='first')
            df.drop(columns=['_hashkey', '_nonnull'], inplace=True)

        rows_after = len(df)
        dedup_count = rows_before - rows_after
        pct_eid_null = (df['employer_id'] == hashlib.sha1(b"UNKNOWN").hexdigest()).mean() * 100
        pct_soc_null = df['soc_code'].isna().mean() * 100

        fy_stats[fy] = {
            'rows_before': rows_before,
            'rows_after': rows_after,
            'dedup_count': dedup_count,
            'pct_employer_id_null': round(pct_eid_null, 1),
            'pct_soc_code_null': round(pct_soc_null, 1),
        }
        log(f"    After dedupe: {rows_after} (removed {dedup_count})")

        all_chunks.append(df)

    if not all_chunks:
        log("  ERROR: No PERM data loaded!")
        with open(log_path, 'w') as f:
            f.write('\n'.join(log_lines))
        return

    full_df = pd.concat(all_chunks, ignore_index=True)
    log(f"\n  Total rows after per-FY dedupe: {len(full_df)}")

    # ── C-global) Global dedup on case_number (PK) ──────────────────────
    pre_global = len(full_df)
    has_cn = full_df['case_number'].notna()
    df_with_cn = full_df[has_cn].copy()
    df_no_cn = full_df[~has_cn].copy()
    # Keep latest decision_date per case_number across all FYs
    df_with_cn['_nonnull'] = df_with_cn.notna().sum(axis=1)
    df_with_cn = df_with_cn.sort_values(['case_number', 'decision_date', '_nonnull'],
                                         ascending=[True, False, False])
    df_with_cn = df_with_cn.drop_duplicates(subset=['case_number'], keep='first')
    df_with_cn.drop(columns=['_nonnull'], inplace=True)
    full_df = pd.concat([df_with_cn, df_no_cn], ignore_index=True)
    global_dedup_count = pre_global - len(full_df)
    log(f"  Global dedup on case_number: removed {global_dedup_count:,} cross-FY duplicates")
    log(f"  Final row count: {len(full_df):,}")

    # ── D) Types & Arrow unification: cast all object/category to plain string ──
    log("\n[D] Casting columns to plain types for Arrow unification")
    for col in full_df.columns:
        if full_df[col].dtype.name == 'category':
            full_df[col] = full_df[col].astype(str)
        elif full_df[col].dtype == object:
            full_df[col] = full_df[col].astype(str)
            full_df.loc[full_df[col] == 'None', col] = None
            full_df.loc[full_df[col] == 'nan', col] = None
            full_df.loc[full_df[col] == 'NaT', col] = None

    # Ensure fiscal_year is int
    full_df['fiscal_year'] = pd.to_numeric(full_df['fiscal_year'], errors='coerce').fillna(0).astype(int)

    # ── E) Write partitioned parquet ──────────────────────────
    log("\n[E] Writing partitioned parquet")
    # Backup existing partitions
    if fact_perm_dir.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        for item in fact_perm_dir.iterdir():
            if item.name.startswith('fiscal_year='):
                shutil.copytree(str(item), str(backup_dir / item.name), dirs_exist_ok=True)
        log(f"  Backed up existing partitions to {backup_dir}")
        shutil.rmtree(fact_perm_dir)

    fact_perm_dir.mkdir(parents=True, exist_ok=True)

    # Select canonical output columns (per schemas.yml fact_perm + extras)
    output_cols = [
        'case_number', 'case_status', 'received_date', 'decision_date',
        'employer_id', 'soc_code', 'area_code', 'employer_country', 'job_title',
        'wage_offer_from', 'wage_offer_to', 'wage_offer_unit',
        'worksite_city', 'worksite_state', 'worksite_postal', 'is_fulltime',
        'source_file', 'ingested_at', 'fiscal_year',
        # Extras beyond schema:
        'audit_flag', 'employer_name_raw', 'naics_code', 'record_layout_version',
    ]
    # Keep only columns that exist
    output_cols = [c for c in output_cols if c in full_df.columns]
    write_df = full_df[output_cols].copy()

    for fy, group in write_df.groupby('fiscal_year'):
        part_dir = fact_perm_dir / f"fiscal_year={fy}"
        part_dir.mkdir(parents=True, exist_ok=True)
        # Keep fiscal_year as a physical column inside the parquet file
        group_data = group.copy()

        # Cast all object cols to string for Arrow
        for c in group_data.select_dtypes(include=['object']).columns:
            group_data[c] = group_data[c].astype(str)
            group_data.loc[group_data[c].isin(['None', 'nan', 'NaT']), c] = None

        group_data.to_parquet(part_dir / "part-0.parquet", index=False, engine='pyarrow')

    partitions_written = len(write_df['fiscal_year'].unique())
    log(f"  Written {partitions_written} partitions to {fact_perm_dir}")

    # ── F) Metrics log ────────────────────────────────────────
    log("\n[F] Writing metrics log")
    log_lines.append("\n--- Per-FY Statistics ---")
    log_lines.append(f"{'FY':<6} {'Before':>10} {'After':>10} {'Dedup':>8} {'%EmpNull':>10} {'%SocNull':>10}")
    total_before = total_after = total_dedup = 0
    for fy in sorted(fy_stats):
        s = fy_stats[fy]
        total_before += s['rows_before']
        total_after += s['rows_after']
        total_dedup += s['dedup_count']
        log_lines.append(f"FY{fy:<4} {s['rows_before']:>10,} {s['rows_after']:>10,} {s['dedup_count']:>8,} {s['pct_employer_id_null']:>9.1f}% {s['pct_soc_code_null']:>9.1f}%")
    log_lines.append(f"{'TOTAL':<6} {total_before:>10,} {total_after:>10,} {total_dedup:>8,}")
    log(f"  Total: {total_before:,} → {total_after:,} ({total_dedup:,} deduped)")

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    log(f"  Log: {log_path}")
    log("\n✓ FIX 1 COMPLETE")


if __name__ == "__main__":
    main()
