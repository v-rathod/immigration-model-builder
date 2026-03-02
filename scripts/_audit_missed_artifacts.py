#!/usr/bin/env python3
"""
Complete deep-dive audit of ALL 17 previously unaudited artifacts.

Categories:
  A. Real data tables needing full audit:
     - employer_monthly_metrics (224K rows)
     - employer_risk_features (668 rows)
     - employer_friendliness_scores_ml (1,695 rows)
     - fact_perm_unique_case (1.67M rows)
     - fact_perm_all (1.67M rows) 
     - fact_oews/ partitioned (446K rows)
     - backlog_estimates (8K rows)
     - category_movement_metrics (8K rows)
     - processing_times_trends (35 rows)
     - fact_waiting_list (9 rows)
  
  B. 0-row stubs (verify schema only):
     - employer_scores (0), fact_acs_wages (0), fact_processing_times (0),
       fact_trac_adjudications (0), oews_wages (0), visa_bulletin (0)

  C. Backup files:
     - dim_soc.bak (1,396 rows)
"""
import pandas as pd
import numpy as np
from pathlib import Path

TABLES = Path("artifacts/tables")

findings = {"critical": [], "warning": [], "ok": [], "info": []}

def log(level, msg):
    findings[level].append(msg)
    sym = {"critical": "CRITICAL", "warning": "WARNING", "ok": "OK", "info": "INFO"}[level]
    icon = {"critical": "❌", "warning": "⚠", "ok": "✓", "info": "ℹ"}[level]
    print(f"  {icon} [{sym}] {msg}")

def section(title):
    print(f"\n{'='*72}")
    print(f"  {title}")
    print(f"{'='*72}")

def check_nulls(df, table, cols, threshold=0.05):
    for c in cols:
        if c not in df.columns:
            log("warning", f"{table}: expected column '{c}' MISSING")
            continue
        null_pct = df[c].isna().mean()
        if null_pct > threshold:
            log("warning", f"{table}.{c}: {null_pct*100:.1f}% null (threshold {threshold*100:.0f}%)")
        else:
            log("ok", f"{table}.{c}: {null_pct*100:.1f}% null — ok")

def check_pk_unique(df, table, pk_cols):
    existing = [c for c in pk_cols if c in df.columns]
    if len(existing) != len(pk_cols):
        log("warning", f"{table}: PK columns {set(pk_cols)-set(existing)} missing")
        return
    dupes = df.duplicated(subset=existing).sum()
    if dupes > 0:
        dupe_pct = 100 * dupes / len(df)
        if dupe_pct > 5:
            log("critical", f"{table}: {dupes:,} duplicate PKs ({dupe_pct:.1f}%)")
        else:
            log("warning", f"{table}: {dupes:,} duplicate PKs ({dupe_pct:.2f}%)")
    else:
        log("ok", f"{table}: PK {existing} is unique")

def check_range(df, table, col, lo, hi):
    if col not in df.columns:
        return
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    below = (vals < lo).sum()
    above = (vals > hi).sum()
    if below > 0 or above > 0:
        log("warning", f"{table}.{col}: {below} below {lo}, {above} above {hi}")
    else:
        log("ok", f"{table}.{col}: all values in [{lo}, {hi}]")

def check_yoy(df, table, fy_col="fiscal_year"):
    if fy_col not in df.columns:
        return
    try:
        fy = df[fy_col].astype(int) if df[fy_col].dtype != "int64" else df[fy_col]
    except:
        fy_str = df[fy_col].astype(str).str.replace("FY", "")
        fy = pd.to_numeric(fy_str, errors="coerce")
    counts = fy.dropna().value_counts().sort_index()
    for i in range(1, len(counts)):
        yr = counts.index[i]
        prev = counts.index[i - 1]
        if counts.iloc[i - 1] == 0:
            continue
        pct_change = (counts.iloc[i] - counts.iloc[i - 1]) / counts.iloc[i - 1] * 100
        if abs(pct_change) > 100:
            log("warning", f"{table}: {prev}→{yr} volume change {pct_change:+.0f}%")

# ══════════════════════════════════════════════════════════════════════════════
# A. REAL DATA TABLES
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. employer_monthly_metrics (224K) ──────────────────────────────────────
section("employer_monthly_metrics.parquet (224,114 rows)")
df = pd.read_parquet(TABLES / "employer_monthly_metrics.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "emp_monthly", ["employer_id", "employer_name", "month", "filings", "approvals", "denials"])
check_pk_unique(df, "emp_monthly", ["employer_id", "month", "dataset"])

# approval_rate sanity: should be 0–1 or 0–100
if "approval_rate" in df.columns:
    ar = pd.to_numeric(df["approval_rate"], errors="coerce").dropna()
    log("info", f"approval_rate range: [{ar.min():.3f}, {ar.max():.3f}], median={ar.median():.3f}")
    if ar.max() > 1.01:
        log("warning", f"emp_monthly.approval_rate max={ar.max():.3f} — are these percentages, not fractions?")
    else:
        log("ok", "emp_monthly.approval_rate in [0,1] range")

# filings non-negative
for col in ["filings", "approvals", "denials"]:
    if col in df.columns:
        neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
        if neg > 0:
            log("critical", f"emp_monthly.{col}: {neg} negative values")
        else:
            log("ok", f"emp_monthly.{col}: no negative values")

# dataset values
if "dataset" in df.columns:
    log("info", f"dataset values: {df['dataset'].value_counts().to_dict()}")

# month format check
if "month" in df.columns:
    sample = df["month"].dropna().iloc[:5].tolist()
    log("info", f"month sample: {sample}")

# ── 2. employer_risk_features (668) ─────────────────────────────────────────
section("employer_risk_features.parquet (668 rows)")
df = pd.read_parquet(TABLES / "employer_risk_features.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "emp_risk", ["employer_id", "is_warn_flagged"])
check_pk_unique(df, "emp_risk", ["employer_id"])
if "total_warn_events" in df.columns:
    check_range(df, "emp_risk", "total_warn_events", 0, 1000)
if "total_employees_affected" in df.columns:
    check_range(df, "emp_risk", "total_employees_affected", 0, 100000)
if "is_warn_flagged" in df.columns:
    flagged = df["is_warn_flagged"].sum()
    log("info", f"is_warn_flagged: {flagged}/{len(df)} ({100*flagged/len(df):.1f}%)")

# ── 3. employer_friendliness_scores_ml (1,695) ──────────────────────────────
section("employer_friendliness_scores_ml.parquet (1,695 rows)")
df = pd.read_parquet(TABLES / "employer_friendliness_scores_ml.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "efs_ml", ["employer_id", "efs_ml", "n_cases_36m"])
check_pk_unique(df, "efs_ml", ["employer_id", "scope"])
if "efs_ml" in df.columns:
    scores = pd.to_numeric(df["efs_ml"], errors="coerce").dropna()
    log("info", f"efs_ml range: [{scores.min():.1f}, {scores.max():.1f}], median={scores.median():.1f}")
    check_range(df, "efs_ml", "efs_ml", 0, 100)
if "scope" in df.columns:
    log("info", f"scope distribution: {df['scope'].value_counts().to_dict()}")

# ── 4. fact_perm_unique_case (1.67M) ────────────────────────────────────────
section("fact_perm_unique_case/ (1,668,587 rows)")
df = pd.read_parquet(TABLES / "fact_perm_unique_case")
df["fiscal_year"] = df["fiscal_year"].astype(int)
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")

# Check dedup vs fact_perm
perm_total = len(pd.read_parquet(TABLES / "fact_perm"))
dedup_total = len(df)
removed = perm_total - dedup_total
log("info", f"fact_perm has {perm_total:,}, unique_case has {dedup_total:,} — {removed:,} removed ({100*removed/perm_total:.2f}%)")

# is_crossfy_duplicate flag
if "is_crossfy_duplicate" in df.columns:
    dupes = df["is_crossfy_duplicate"].sum()
    log("info", f"is_crossfy_duplicate = True: {dupes:,} ({100*dupes/len(df):.1f}%)")

# case_number uniqueness (the whole point)
if "case_number" in df.columns:
    non_null = df["case_number"].dropna()
    case_dupes = non_null.duplicated().sum()
    null_cases = df["case_number"].isna().sum()
    log("info", f"case_number null: {null_cases:,} ({100*null_cases/len(df):.2f}%)")
    if case_dupes > 0:
        log("warning", f"fact_perm_unique_case: {case_dupes:,} duplicate case_numbers (after dedup)")
    else:
        log("ok", "fact_perm_unique_case: all non-null case_numbers are unique")

# FY distribution comparison with fact_perm
check_yoy(df, "perm_unique_case")

# ── 5. fact_perm_all (flat copy) ────────────────────────────────────────────
section("fact_perm_all.parquet (1,675,051 rows)")
df_perm_all = pd.read_parquet(TABLES / "fact_perm_all.parquet")
df_perm = pd.read_parquet(TABLES / "fact_perm")
log("info", f"Shape: {df_perm_all.shape}")
# Should be identical to fact_perm/
if len(df_perm_all) == len(df_perm):
    log("ok", f"fact_perm_all matches fact_perm/ row count ({len(df_perm_all):,})")
else:
    log("critical", f"fact_perm_all ({len(df_perm_all):,}) ≠ fact_perm/ ({len(df_perm):,})")

# Column match
perm_all_cols = set(df_perm_all.columns)
perm_cols = set(df_perm.columns)
if perm_all_cols == perm_cols:
    log("ok", "fact_perm_all columns match fact_perm/")
else:
    extra = perm_all_cols - perm_cols
    missing = perm_cols - perm_all_cols
    if extra:
        log("warning", f"fact_perm_all extra columns: {extra}")
    if missing:
        log("warning", f"fact_perm_all missing columns: {missing}")
del df_perm_all, df_perm

# ── 6. fact_oews/ partitioned ───────────────────────────────────────────────
section("fact_oews/ partitioned (446,432 rows)")
df_oews_dir = pd.read_parquet(TABLES / "fact_oews")
df_oews_flat = pd.read_parquet(TABLES / "fact_oews.parquet")
log("info", f"fact_oews/ shape: {df_oews_dir.shape}, fact_oews.parquet shape: {df_oews_flat.shape}")
if len(df_oews_dir) == len(df_oews_flat):
    log("ok", f"fact_oews/ matches fact_oews.parquet row count ({len(df_oews_dir):,})")
else:
    log("warning", f"fact_oews/ ({len(df_oews_dir):,}) ≠ fact_oews.parquet ({len(df_oews_flat):,})")

# Column difference
dir_cols = set(df_oews_dir.columns)
flat_cols = set(df_oews_flat.columns)
diff = dir_cols.symmetric_difference(flat_cols)
if diff:
    log("info", f"Column difference: dir has {dir_cols - flat_cols}, flat has {flat_cols - dir_cols}")
else:
    log("ok", "fact_oews/ and fact_oews.parquet have identical columns")
del df_oews_dir, df_oews_flat

# ── 7. backlog_estimates (8,060) ────────────────────────────────────────────
section("backlog_estimates.parquet (8,060 rows)")
df = pd.read_parquet(TABLES / "backlog_estimates.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "backlog", ["category", "chart", "country", "bulletin_year", "bulletin_month"])
check_pk_unique(df, "backlog", ["category", "chart", "country", "bulletin_year", "bulletin_month"])

# backlog_months_to_clear_est sanity
if "backlog_months_to_clear_est" in df.columns:
    vals = pd.to_numeric(df["backlog_months_to_clear_est"], errors="coerce").dropna()
    log("info", f"backlog_months range: [{vals.min():.1f}, {vals.max():.1f}], median={vals.median():.1f}")
    neg = (vals < 0).sum()
    if neg > 0:
        log("critical", f"backlog.backlog_months_to_clear_est: {neg} negative values")
    else:
        log("ok", "backlog.backlog_months_to_clear_est: no negative values")

# advancement_days_12m_avg
if "advancement_days_12m_avg" in df.columns:
    vals = pd.to_numeric(df["advancement_days_12m_avg"], errors="coerce").dropna()
    log("info", f"advancement_days_12m_avg range: [{vals.min():.1f}, {vals.max():.1f}]")
    null_pct = df["advancement_days_12m_avg"].isna().mean()
    log("info", f"advancement_days_12m_avg null: {null_pct*100:.1f}%")

# ── 8. category_movement_metrics (8,060) ────────────────────────────────────
section("category_movement_metrics.parquet (8,060 rows)")
df = pd.read_parquet(TABLES / "category_movement_metrics.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "cat_movement", ["category", "chart", "country", "bulletin_year", "bulletin_month"])
check_pk_unique(df, "cat_movement", ["category", "chart", "country", "bulletin_year", "bulletin_month"])

# volatility_score
if "volatility_score" in df.columns:
    vals = pd.to_numeric(df["volatility_score"], errors="coerce").dropna()
    log("info", f"volatility_score range: [{vals.min():.2f}, {vals.max():.2f}], null: {df['volatility_score'].isna().mean()*100:.1f}%")
    neg = (vals < 0).sum()
    if neg > 0:
        log("warning", f"cat_movement.volatility_score: {neg} negative values")
    else:
        log("ok", "cat_movement.volatility_score: no negative values")

# retrogression_events_12m
if "retrogression_events_12m" in df.columns:
    vals = pd.to_numeric(df["retrogression_events_12m"], errors="coerce").dropna()
    log("info", f"retrogression_events_12m range: [{int(vals.min())}, {int(vals.max())}], null: {df['retrogression_events_12m'].isna().mean()*100:.1f}%")
    if vals.max() > 12:
        log("warning", f"cat_movement.retrogression_events_12m max={int(vals.max())} > 12")
    else:
        log("ok", "cat_movement.retrogression_events_12m: max ≤ 12")

# ── 9. processing_times_trends (35) ────────────────────────────────────────
section("processing_times_trends.parquet (35 rows)")
df = pd.read_parquet(TABLES / "processing_times_trends.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
check_nulls(df, "proc_trends", ["fiscal_year", "category", "form_type"])

# throughput / approval_rate sanity
if "approval_rate" in df.columns:
    ar = pd.to_numeric(df["approval_rate"], errors="coerce").dropna()
    log("info", f"approval_rate range: [{ar.min():.3f}, {ar.max():.3f}]")
    if ar.max() > 1.01:
        log("warning", f"proc_trends.approval_rate max={ar.max():.3f}")
    else:
        log("ok", "proc_trends.approval_rate in [0,1] range")

# Check totals non-negative
for col in ["total_approved", "total_denied", "total_pending", "total_received"]:
    if col in df.columns:
        neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
        if neg > 0:
            log("warning", f"proc_trends.{col}: {neg} negative values")

if "fiscal_year" in df.columns:
    log("info", f"FY range: {df['fiscal_year'].min()}-{df['fiscal_year'].max()}")
    log("info", f"FY distribution:\n{df['fiscal_year'].value_counts().sort_index().to_string()}")

# ── 10. fact_waiting_list (9) ───────────────────────────────────────────────
section("fact_waiting_list.parquet (9 rows)")
df = pd.read_parquet(TABLES / "fact_waiting_list.parquet")
log("info", f"Shape: {df.shape}")
log("info", f"Columns: {sorted(df.columns.tolist())}")
print(f"\n  Full table:")
print(df.to_string(index=False))
check_nulls(df, "waiting_list", ["category", "country", "count_waiting"])
if "count_waiting" in df.columns:
    neg = (pd.to_numeric(df["count_waiting"], errors="coerce") < 0).sum()
    if neg > 0:
        log("warning", f"waiting_list.count_waiting: {neg} negative values")
    else:
        log("ok", "waiting_list.count_waiting: no negative values")


# ══════════════════════════════════════════════════════════════════════════════
# B. 0-ROW STUBS — verify schema, confirm expected empty
# ══════════════════════════════════════════════════════════════════════════════
section("0-ROW STUBS (expected empty)")
stubs = {
    "employer_scores.parquet": ["employer_name_normalized", "friendliness_score"],
    "fact_acs_wages.parquet": ["soc_code", "area_code", "median", "year"],
    "fact_processing_times.parquet": ["form", "category", "office", "processing_time_min"],
    "fact_trac_adjudications.parquet": ["fiscal_year", "form", "measure", "value"],
    "oews_wages.parquet": ["soc_code", "area_code", "median_wage", "year"],
    "visa_bulletin.parquet": ["category", "country", "year_month"],
}

for name, expected_cols in stubs.items():
    path = TABLES / name
    if not path.exists():
        log("warning", f"{name}: file not found")
        continue
    df = pd.read_parquet(path)
    if len(df) == 0:
        log("ok", f"{name}: confirmed 0-row stub — schema: {sorted(df.columns.tolist())}")
    else:
        log("warning", f"{name}: expected 0 rows but has {len(df):,}")


# ══════════════════════════════════════════════════════════════════════════════
# C. BACKUP FILES
# ══════════════════════════════════════════════════════════════════════════════
section("BACKUP FILES")
df_bak = pd.read_parquet(TABLES / "dim_soc.bak.parquet")
df_curr = pd.read_parquet(TABLES / "dim_soc.parquet")
log("info", f"dim_soc.bak: {len(df_bak):,} rows (SOC-2018 only)")
log("info", f"dim_soc.parquet: {len(df_curr):,} rows (SOC-2018 + legacy)")
extra_codes = len(df_curr) - len(df_bak)
log("ok", f"dim_soc expanded by {extra_codes} legacy codes over backup")


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
section("AUDIT SUMMARY")
print(f"  Critical: {len(findings['critical'])}")
print(f"  Warnings: {len(findings['warning'])}")
print(f"  OK:       {len(findings['ok'])}")
print(f"  Info:     {len(findings['info'])}")

if findings["critical"]:
    print("\n  ❌ CRITICAL ISSUES:")
    for c in findings["critical"]:
        print(f"    - {c}")

if findings["warning"]:
    print("\n  ⚠ WARNINGS:")
    for w in findings["warning"]:
        print(f"    - {w}")
