#!/usr/bin/env python3
"""
Comprehensive Data Sanity Audit for ALL P2 artifacts.

Checks every ingested data source for:
  1. Year-over-year volume swings (>50% change flagged)
  2. Value range plausibility (wages, dates, counts)
  3. Null/missing rates in key columns
  4. Outlier detection (IQR-based for numeric columns)
  5. Temporal continuity (gaps in time series)
  6. Distribution anomalies (single value dominating)
  7. Cross-table referential integrity
  8. Month-over-month cutoff date jumps (visa bulletin specific)

Usage:
    python scripts/data_sanity_audit.py [--source SOURCE] [--verbose]

If --source is omitted, audits ALL sources.
"""

import sys, argparse, warnings
from pathlib import Path
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning)

TABLES = Path("artifacts/tables")

# ── colour helpers ──────────────────────────────────────────────
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

class AuditResult:
    def __init__(self, source: str):
        self.source = source
        self.findings: list[dict] = []

    def add(self, severity: str, check: str, detail: str):
        self.findings.append({"severity": severity, "check": check, "detail": detail})

    def critical(self, check, detail):   self.add("CRITICAL", check, detail)
    def warning(self, check, detail):    self.add("WARNING", check, detail)
    def info(self, check, detail):       self.add("INFO", check, detail)
    def ok(self, check, detail):         self.add("OK", check, detail)

    def print_summary(self):
        crits = [f for f in self.findings if f["severity"] == "CRITICAL"]
        warns = [f for f in self.findings if f["severity"] == "WARNING"]
        oks   = [f for f in self.findings if f["severity"] in ("OK", "INFO")]
        label = f"{BOLD}{CYAN}[{self.source}]{RESET}"
        print(f"\n{'='*80}")
        print(f"{label}  {len(crits)} critical, {len(warns)} warnings, {len(oks)} ok")
        print(f"{'='*80}")
        for f in self.findings:
            sev = f["severity"]
            if sev == "CRITICAL":
                icon = f"{RED}✗ CRITICAL{RESET}"
            elif sev == "WARNING":
                icon = f"{YELLOW}⚠ WARNING{RESET}"
            elif sev == "INFO":
                icon = f"{CYAN}ℹ INFO{RESET}"
            else:
                icon = f"{GREEN}✓ OK{RESET}"
            print(f"  {icon}  [{f['check']}] {f['detail']}")
        return len(crits), len(warns)


# ── helper functions ────────────────────────────────────────────
def load(name, partitioned=False):
    p = TABLES / name
    if not p.exists():
        return None
    return pd.read_parquet(p)

def yoy_volume_check(result: AuditResult, df: pd.DataFrame, year_col: str, label: str, max_swing=0.60):
    """Flag year-over-year volume changes > max_swing."""
    counts = df.groupby(year_col).size().sort_index()
    for i in range(1, len(counts)):
        yr_prev, yr_curr = counts.index[i-1], counts.index[i]
        n_prev, n_curr = counts.iloc[i-1], counts.iloc[i]
        if n_prev == 0:
            continue
        pct = (n_curr - n_prev) / n_prev
        if abs(pct) > max_swing:
            result.warning("yoy_volume",
                f"{label}: {yr_prev}→{yr_curr} volume changed {pct:+.0%} ({n_prev:,}→{n_curr:,})")
        else:
            result.ok("yoy_volume", f"{label}: {yr_prev}→{yr_curr} {pct:+.0%} ok")

def null_rate_check(result: AuditResult, df: pd.DataFrame, key_cols: list, label: str, threshold=0.05):
    """Flag key columns with null rate > threshold."""
    for col in key_cols:
        if col not in df.columns:
            result.warning("null_rate", f"{label}: column '{col}' missing from dataframe")
            continue
        null_rate = df[col].isna().mean()
        if null_rate > threshold:
            result.warning("null_rate",
                f"{label}.{col}: {null_rate:.1%} null (threshold {threshold:.0%})")
        else:
            result.ok("null_rate", f"{label}.{col}: {null_rate:.1%} null — ok")

def range_check(result: AuditResult, df: pd.DataFrame, col: str, lo, hi, label: str):
    """Flag values outside [lo, hi]."""
    if col not in df.columns:
        return
    vals = df[col].dropna()
    below = (vals < lo).sum()
    above = (vals > hi).sum()
    if below > 0:
        result.warning("range", f"{label}.{col}: {below:,} values < {lo}")
    if above > 0:
        result.warning("range", f"{label}.{col}: {above:,} values > {hi}")
    if below == 0 and above == 0:
        result.ok("range", f"{label}.{col}: all values in [{lo}, {hi}]")

def outlier_check_iqr(result: AuditResult, df: pd.DataFrame, col: str, label: str, k=3.0):
    """Flag extreme outliers using IQR method (k=3 → very conservative)."""
    if col not in df.columns:
        return
    vals = df[col].dropna()
    if len(vals) < 10:
        return
    q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return
    lo, hi = q1 - k * iqr, q3 + k * iqr
    outliers = ((vals < lo) | (vals > hi)).sum()
    pct = outliers / len(vals)
    if pct > 0.01:
        result.warning("outlier",
            f"{label}.{col}: {outliers:,} extreme outliers ({pct:.1%}) outside [{lo:,.0f}, {hi:,.0f}]")
    elif outliers > 0:
        result.ok("outlier", f"{label}.{col}: {outliers:,} outliers ({pct:.2%}) — acceptable")

def dominance_check(result: AuditResult, df: pd.DataFrame, col: str, label: str, threshold=0.80):
    """Flag if a single value dominates > threshold of non-null rows."""
    if col not in df.columns:
        return
    vals = df[col].dropna()
    if len(vals) == 0:
        return
    top_val = vals.value_counts().iloc[0]
    pct = top_val / len(vals)
    if pct > threshold:
        top_name = vals.value_counts().index[0]
        result.warning("dominance",
            f"{label}.{col}: '{top_name}' covers {pct:.0%} of rows")
    else:
        result.ok("dominance", f"{label}.{col}: no single value > {threshold:.0%}")

def temporal_gap_check(result: AuditResult, df: pd.DataFrame, year_col: str, label: str,
                        month_col: str = None, expected_start: int = None, expected_end: int = None):
    """Flag gaps in year (or year+month) series."""
    years = sorted(df[year_col].dropna().unique())
    if expected_start and years[0] > expected_start:
        result.warning("temporal_gap", f"{label}: starts at {years[0]}, expected {expected_start}")
    if expected_end and years[-1] < expected_end:
        result.warning("temporal_gap", f"{label}: ends at {years[-1]}, expected {expected_end}")
    for i in range(1, len(years)):
        gap = years[i] - years[i-1]
        if gap > 1:
            result.warning("temporal_gap", f"{label}: gap {years[i-1]}→{years[i]} ({gap} years)")

def wage_plausibility(result: AuditResult, df: pd.DataFrame, wage_col: str, label: str,
                       min_wage=15000, max_wage=1_000_000):
    """Check wage values are plausible (annual)."""
    if wage_col not in df.columns:
        return
    vals = df[wage_col].dropna()
    if len(vals) == 0:
        return
    below = (vals < min_wage).sum()
    above = (vals > max_wage).sum()
    median = vals.median()
    if below > 0:
        result.warning("wage_plausibility",
            f"{label}.{wage_col}: {below:,} values < ${min_wage:,} (possible hourly rates or errors)")
    if above > 0:
        result.warning("wage_plausibility",
            f"{label}.{wage_col}: {above:,} values > ${max_wage:,} (possible errors)")
    result.ok("wage_plausibility",
        f"{label}.{wage_col}: median=${median:,.0f}, range=[{vals.min():,.0f}, {vals.max():,.0f}]")

def mom_jump_check(result: AuditResult, df: pd.DataFrame, 
                    year_col: str, month_col: str, value_col: str, 
                    group_cols: list, label: str, max_jump_days=365*5):
    """Check month-over-month jumps in date columns for time series."""
    if value_col not in df.columns:
        return
    for grp_vals, grp_df in df.groupby(group_cols):
        grp_sorted = grp_df.sort_values([year_col, month_col])
        dates = pd.to_datetime(grp_sorted[value_col], errors="coerce")
        for i in range(1, len(dates)):
            if pd.isna(dates.iloc[i]) or pd.isna(dates.iloc[i-1]):
                continue
            delta = (dates.iloc[i] - dates.iloc[i-1]).days
            if abs(delta) > max_jump_days:
                yr = grp_sorted[year_col].iloc[i]
                mo = grp_sorted[month_col].iloc[i]
                grp_label = grp_vals if isinstance(grp_vals, str) else "/".join(str(v) for v in grp_vals)
                result.warning("mom_jump",
                    f"{label} [{grp_label}] {yr}-{mo:02d}: jump of {delta:,} days "
                    f"({dates.iloc[i-1].strftime('%Y-%m-%d')}→{dates.iloc[i].strftime('%Y-%m-%d')})")


# ═══════════════════════════════════════════════════════════════
#  INDIVIDUAL SOURCE AUDITS
# ═══════════════════════════════════════════════════════════════

def audit_fact_perm():
    """PERM labor certifications (DOL) — 1.67M rows, FY2008–2026."""
    r = AuditResult("fact_perm")
    df = load("fact_perm")
    if df is None:
        r.critical("load", "fact_perm not found"); return r
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = df["fiscal_year"].astype(int)
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols, FY range: {df['fiscal_year'].min()}-{df['fiscal_year'].max()}")

    # 1) YoY volume
    yoy_volume_check(r, df, "fiscal_year", "fact_perm", max_swing=0.50)

    # 2) Key null rates
    null_rate_check(r, df, ["case_number", "employer_name", "job_title", "soc_code",
                             "case_status", "pw_amount", "worksite_state"], "perm")

    # 3) Wage plausibility
    wage_plausibility(r, df, "pw_amount", "perm")

    # 4) case_status distribution
    if "case_status" in df.columns:
        dist = df["case_status"].value_counts(normalize=True)
        r.info("distribution", f"case_status top values: {dict(dist.head(5).round(3))}")
        # Certified should be majority
        cert_pct = dist.get("CERTIFIED", 0) + dist.get("Certified", 0)
        if cert_pct < 0.40:
            r.warning("distribution", f"CERTIFIED only {cert_pct:.0%} — unusually low")

    # 5) Temporal continuity
    temporal_gap_check(r, df, "fiscal_year", "perm", expected_start=2008, expected_end=2025)

    # 6) State distribution — should have all 50+ states
    if "worksite_state" in df.columns:
        n_states = df["worksite_state"].nunique()
        if n_states < 45:
            r.warning("coverage", f"Only {n_states} unique states (expected 50+)")
        else:
            r.ok("coverage", f"{n_states} unique states represented")

    # 7) SOC code format
    if "soc_code" in df.columns:
        valid_soc = df["soc_code"].dropna().str.match(r'^\d{2}-\d{4}(\.\d{2})?$')
        invalid = (~valid_soc).sum()
        if invalid > 0:
            pct = invalid / len(df["soc_code"].dropna())
            if pct > 0.05:
                r.warning("format", f"perm.soc_code: {invalid:,} ({pct:.1%}) don't match XX-XXXX format")
            else:
                r.ok("format", f"perm.soc_code: {invalid:,} invalid ({pct:.2%}) — acceptable")

    # 8) Wage outliers
    outlier_check_iqr(r, df, "pw_amount", "perm", k=4.0)

    # 9) FY2026 partial check
    fy26 = df[df["fiscal_year"] == 2026] if "fiscal_year" in df.columns else pd.DataFrame()
    if len(fy26) > 0:
        r.info("partial_year", f"FY2026 partial: {len(fy26):,} rows")

    return r


def audit_fact_lca():
    """LCA H-1B applications (DOL) — 9.5M+ rows, FY2008–2026."""
    r = AuditResult("fact_lca")
    df = load("fact_lca")
    if df is None:
        r.critical("load", "fact_lca not found"); return r
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = df["fiscal_year"].astype(int)
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols, FY range: {df['fiscal_year'].min()}-{df['fiscal_year'].max()}")

    # 1) YoY volume
    yoy_volume_check(r, df, "fiscal_year", "fact_lca", max_swing=0.40)

    # 2) Key null rates
    null_rate_check(r, df, ["case_number", "employer_name", "job_title", "soc_code",
                             "case_status", "wage_rate_of_pay_from", "worksite_state"], "lca")

    # 3) Wage plausibility
    wage_plausibility(r, df, "wage_rate_of_pay_from", "lca")

    # 4) case_status distribution
    if "case_status" in df.columns:
        dist = df["case_status"].value_counts(normalize=True)
        r.info("distribution", f"case_status top: {dict(dist.head(5).round(3))}")
        cert_pct = sum(v for k, v in dist.items() if "CERTIFIED" in str(k).upper())
        if cert_pct < 0.70:
            r.warning("distribution", f"CERTIFIED variants only {cert_pct:.0%} — unusually low for LCA")

    # 5) Temporal continuity
    temporal_gap_check(r, df, "fiscal_year", "lca", expected_start=2008, expected_end=2025)

    # 6) Visa class distribution (H-1B should dominate)
    if "visa_class" in df.columns:
        dist = df["visa_class"].value_counts(normalize=True)
        h1b_pct = sum(v for k, v in dist.items() if "H-1B" in str(k).upper())
        if h1b_pct < 0.50:
            r.warning("distribution", f"H-1B variants only {h1b_pct:.0%} of LCA filings")
        else:
            r.ok("distribution", f"H-1B variants = {h1b_pct:.0%} of filings")

    # 7) Wage outliers  
    outlier_check_iqr(r, df, "wage_rate_of_pay_from", "lca", k=4.0)

    # 8) State coverage
    if "worksite_state" in df.columns:
        n_states = df["worksite_state"].nunique()
        r.ok("coverage", f"{n_states} unique worksite states")

    return r


def audit_fact_oews():
    """OEWS wage data (BLS) — 446K rows."""
    r = AuditResult("fact_oews")
    df = load("fact_oews.parquet")
    if df is None:
        r.critical("load", "fact_oews not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")

    # 1) Ref year coverage
    if "ref_year" in df.columns:
        years = sorted(df["ref_year"].unique())
        r.info("years", f"ref_years: {years}")

    # 2) Key null rates
    null_rate_check(r, df, ["soc_code", "area_code", "a_median", "tot_emp"], "oews")

    # 3) Wage plausibility (annual median)
    wage_plausibility(r, df, "a_median", "oews", min_wage=18000, max_wage=500000)

    # 4) Employment totals
    if "tot_emp" in df.columns:
        range_check(r, df, "tot_emp", 0, 50_000_000, "oews")
        outlier_check_iqr(r, df, "tot_emp", "oews", k=4.0)

    # 5) SOC code format
    if "soc_code" in df.columns:
        valid = df["soc_code"].dropna().str.match(r'^\d{2}-\d{4}$')
        invalid = (~valid).sum()
        if invalid > 0:
            r.warning("format", f"oews.soc_code: {invalid:,} don't match XX-XXXX")
        else:
            r.ok("format", f"oews.soc_code: all valid format")

    # 6) Wage percentile ordering (10th < 25th < median < 75th < 90th)
    pctl_cols = ["a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90"]
    present = [c for c in pctl_cols if c in df.columns]
    if len(present) >= 3:
        subset = df[present].dropna()
        for i in range(1, len(present)):
            violations = (subset[present[i]] < subset[present[i-1]]).sum()
            if violations > 0:
                r.warning("wage_ordering",
                    f"oews: {violations:,} rows where {present[i]} < {present[i-1]}")
            else:
                r.ok("wage_ordering", f"oews: {present[i]} ≥ {present[i-1]} — all ok")

    return r


def audit_fact_cutoffs():
    """Visa Bulletin cutoffs — 7,190 rows."""
    r = AuditResult("fact_cutoffs_all")
    df = load("fact_cutoffs_all.parquet")
    if df is None:
        r.critical("load", "fact_cutoffs_all not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")

    # 1) Category coverage
    if "category" in df.columns:
        cats = sorted(df["category"].unique())
        r.info("categories", f"Categories: {cats}")
        expected = {"EB1", "EB2", "EB3", "EB4", "EB5"}
        missing = expected - set(cats)
        if missing:
            r.warning("coverage", f"Missing categories: {missing}")

    # 2) Country coverage
    if "country" in df.columns:
        countries = sorted(df["country"].unique())
        r.info("countries", f"Countries: {countries}")
        expected = {"ROW", "CHINA", "INDIA", "MEXICO", "PHILIPPINES"}
        found = set(c.upper() for c in countries)
        # Check for presence, accounting for naming variations
        for exp in expected:
            if not any(exp in f for f in found):
                r.warning("coverage", f"Missing country: {exp}")

    # 3) Bulletin year range
    if "bulletin_year" in df.columns:
        yr_min, yr_max = df["bulletin_year"].min(), df["bulletin_year"].max()
        r.info("year_range", f"Years: {yr_min}–{yr_max}")

    # 4) Chart types (FAD/DFF)
    if "chart" in df.columns:
        charts = df["chart"].unique().tolist()
        r.info("chart_types", f"Charts: {charts}")
        if "FAD" not in charts:
            r.critical("coverage", "FAD chart missing")
        if "DFF" not in charts:
            r.warning("coverage", "DFF chart missing")

    # 5) EB2 India should NEVER be Current for FAD
    if all(c in df.columns for c in ["category", "country", "chart", "status_flag"]):
        eb2_ind_fad = df[(df["category"]=="EB2") & (df["country"]=="INDIA") & (df["chart"]=="FAD")]
        current_count = (eb2_ind_fad["status_flag"] == "C").sum()
        if current_count > 0:
            r.critical("eb2_india_current",
                f"EB2 India FAD has {current_count} 'Current' entries — should be 0")
        else:
            r.ok("eb2_india_current", "EB2 India FAD: 0 Current entries — correct")

    # 6) Month-over-month cutoff jumps (check for wild swings)
    if all(c in df.columns for c in ["category", "country", "chart", "cutoff_date", "bulletin_year", "bulletin_month"]):
        cutoff_dates = df[df["status_flag"] != "C"].copy()
        cutoff_dates["cutoff_dt"] = pd.to_datetime(cutoff_dates["cutoff_date"], errors="coerce")
        for (cat, ctry, chart), grp in cutoff_dates.groupby(["category", "country", "chart"]):
            grp_sorted = grp.sort_values(["bulletin_year", "bulletin_month"])
            dates = grp_sorted["cutoff_dt"].values
            yrs = grp_sorted["bulletin_year"].values
            mos = grp_sorted["bulletin_month"].values
            for i in range(1, len(dates)):
                if pd.isna(dates[i]) or pd.isna(dates[i-1]):
                    continue
                delta = (dates[i] - dates[i-1]) / np.timedelta64(1, 'D')
                # Forward jump > 5 years or backward > 5 years
                if abs(delta) > 365 * 5:
                    r.warning("cutoff_jump",
                        f"{chart}/{cat}/{ctry} {yrs[i]}-{mos[i]:02d}: "
                        f"jump of {delta/365:.1f} years "
                        f"({pd.Timestamp(dates[i-1]).strftime('%Y-%m-%d')}→{pd.Timestamp(dates[i]).strftime('%Y-%m-%d')})")

    return r


def audit_fact_visa_applications():
    """DOS visa applications by foreign state of chargeability — 35K rows."""
    r = AuditResult("fact_visa_applications")
    df = load("fact_visa_applications.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")

    # Columns
    r.info("columns", f"Columns: {list(df.columns)}")

    # 1) Fiscal year coverage
    fy_col = None
    for c in ["fiscal_year", "fy", "year"]:
        if c in df.columns:
            fy_col = c; break
    if fy_col:
        years = sorted(df[fy_col].unique())
        r.info("years", f"Years: {years}")
        yoy_volume_check(r, df, fy_col, "visa_apps", max_swing=0.50)

    # 2) Numeric value checks
    for col in df.select_dtypes(include=[np.number]).columns:
        if col == fy_col:
            continue
        vals = df[col].dropna()
        if len(vals) == 0:
            continue
        neg = (vals < 0).sum()
        if neg > 0:
            r.warning("negative_values", f"visa_apps.{col}: {neg:,} negative values")
        outlier_check_iqr(r, df, col, "visa_apps", k=4.0)

    # 3) Null rates
    null_rate_check(r, df, list(df.columns[:5]), "visa_apps")

    return r


def audit_fact_iv_post():
    """DOS immigrant visa issuances by consular post — 163K rows."""
    r = AuditResult("fact_iv_post")
    df = load("fact_iv_post.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    # 1) Fiscal year coverage
    fy_col = None
    for c in ["fiscal_year", "fy"]:
        if c in df.columns:
            fy_col = c; break
    if fy_col:
        years = sorted(df[fy_col].unique())
        r.info("years", f"Years: {years}")
        yoy_volume_check(r, df, fy_col, "iv_post", max_swing=0.50)

    # 2) Issuance values should be non-negative
    for col in df.select_dtypes(include=[np.number]).columns:
        if col in [fy_col, "month"]:
            continue
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative_values", f"iv_post.{col}: {neg:,} negative values")

    # 3) Post coverage
    if "post" in df.columns:
        n_posts = df["post"].nunique()
        r.info("coverage", f"{n_posts} unique consular posts")

    # 4) Null rates
    null_rate_check(r, df, list(df.columns[:5]), "iv_post")

    return r


def audit_fact_niv_issuance():
    """DOS nonimmigrant visa issuance — 501K rows."""
    r = AuditResult("fact_niv_issuance")
    df = load("fact_niv_issuance.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    # 1) Year coverage
    fy_col = None
    for c in ["fiscal_year", "fy", "year"]:
        if c in df.columns:
            fy_col = c; break
    if fy_col:
        years = sorted(df[fy_col].unique())
        r.info("years", f"Years: {years}")
        yoy_volume_check(r, df, fy_col, "niv_issuance", max_swing=0.50)

    # 2) Issuance count plausibility
    for col in df.select_dtypes(include=[np.number]).columns:
        if col == fy_col:
            continue
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative_values", f"niv_issuance.{col}: {neg:,} negative values")
        outlier_check_iqr(r, df, col, "niv_issuance", k=4.0)

    # 3) Null rates
    null_rate_check(r, df, list(df.columns), "niv_issuance")

    return r


def audit_fact_visa_issuance():
    """DOS visa issuance summary — 28K rows."""
    r = AuditResult("fact_visa_issuance")
    df = load("fact_visa_issuance.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    for col in df.select_dtypes(include=[np.number]).columns:
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative_values", f"visa_issuance.{col}: {neg:,} negative values")
        outlier_check_iqr(r, df, col, "visa_issuance", k=4.0)

    null_rate_check(r, df, list(df.columns), "visa_issuance")
    return r


def audit_fact_h1b_employer_hub():
    """USCIS H-1B Employer Data Hub (discontinued FY2023) — 729K rows."""
    r = AuditResult("fact_h1b_employer_hub")
    df = load("fact_h1b_employer_hub.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")

    # 1) Stale flag — all should be True
    if "is_stale" in df.columns:
        stale_pct = df["is_stale"].mean()
        if stale_pct < 1.0:
            r.warning("stale_flag", f"Only {stale_pct:.0%} rows marked is_stale (expected 100%)")
        else:
            r.ok("stale_flag", "All rows marked is_stale=True — correct")

    # 2) Fiscal year range
    fy_col = None
    for c in ["fiscal_year", "fy"]:
        if c in df.columns:
            fy_col = c; break
    if fy_col:
        years = sorted(df[fy_col].unique())
        r.info("years", f"Years: {years}")
        if max(years) > 2023:
            r.warning("stale_data", f"Data extends to FY{max(years)} but USCIS discontinued after FY2023")
        yoy_volume_check(r, df, fy_col, "h1b_hub", max_swing=0.60)

    # 3) Approval/denial rates should be sane
    for col in ["initial_approvals", "initial_denials", "continuing_approvals", "continuing_denials"]:
        if col in df.columns:
            neg = (df[col].dropna() < 0).sum()
            if neg > 0:
                r.warning("negative", f"h1b_hub.{col}: {neg:,} negative values")
            range_check(r, df, col, 0, 100_000, "h1b_hub")

    return r


def audit_fact_bls_ces():
    """BLS Current Employment Statistics — 26 rows."""
    r = AuditResult("fact_bls_ces")
    df = load("fact_bls_ces.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")
    r.info("sample", f"First 5 rows:\n{df.head().to_string()}")

    # Small table — just check for nulls and negatives
    null_rate_check(r, df, list(df.columns), "bls_ces")
    for col in df.select_dtypes(include=[np.number]).columns:
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative", f"bls_ces.{col}: {neg:,} negative values")
    return r


def audit_fact_dhs_admissions():
    """DHS I-94 admissions — 45 rows."""
    r = AuditResult("fact_dhs_admissions")
    df = load("fact_dhs_admissions.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    null_rate_check(r, df, list(df.columns), "dhs_admissions")
    for col in df.select_dtypes(include=[np.number]).columns:
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative", f"dhs_admissions.{col}: {neg:,} negative values")
        outlier_check_iqr(r, df, col, "dhs_admissions", k=3.0)
    return r


def audit_fact_uscis_approvals():
    """USCIS approval statistics — 146 rows."""
    r = AuditResult("fact_uscis_approvals")
    df = load("fact_uscis_approvals.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    null_rate_check(r, df, list(df.columns), "uscis_approvals")
    for col in df.select_dtypes(include=[np.number]).columns:
        neg = (df[col].dropna() < 0).sum()
        if neg > 0:
            r.warning("negative", f"uscis_approvals.{col}: {neg:,} negative values")
    return r


def audit_fact_warn_events():
    """WARN Act layoff events — 985 rows."""
    r = AuditResult("fact_warn_events")
    df = load("fact_warn_events.parquet")
    if df is None:
        r.critical("load", "not found"); return r
    r.info("shape", f"{len(df):,} rows × {len(df.columns)} cols")
    r.info("columns", f"Columns: {list(df.columns)}")

    null_rate_check(r, df, list(df.columns), "warn_events")

    # Check employee counts
    for col in df.columns:
        if "employee" in col.lower() or "workers" in col.lower() or "affected" in col.lower():
            if pd.api.types.is_numeric_dtype(df[col]):
                range_check(r, df, col, 0, 100_000, "warn_events")
    return r


def audit_dim_tables():
    """All dimension tables."""
    r = AuditResult("dim_tables")

    # dim_employer
    de = load("dim_employer.parquet")
    if de is not None:
        r.info("dim_employer", f"{len(de):,} rows × {len(de.columns)} cols")
        null_rate_check(r, de, ["employer_id", "employer_name"], "dim_employer")
        # Check for duplicate employer_ids
        dupes = de["employer_id"].duplicated().sum()
        if dupes > 0:
            r.critical("pk_unique", f"dim_employer: {dupes:,} duplicate employer_ids")
        else:
            r.ok("pk_unique", "dim_employer: employer_id is unique")

    # dim_soc
    ds = load("dim_soc.parquet")
    if ds is not None:
        r.info("dim_soc", f"{len(ds):,} rows × {len(ds.columns)} cols")
        null_rate_check(r, ds, ["soc_code", "soc_title"], "dim_soc")
        dupes = ds["soc_code"].duplicated().sum()
        if dupes > 0:
            r.critical("pk_unique", f"dim_soc: {dupes:,} duplicate soc_codes")
        else:
            r.ok("pk_unique", f"dim_soc: soc_code is unique ({len(ds):,} codes)")

    # dim_country
    dc = load("dim_country.parquet")
    if dc is not None:
        r.info("dim_country", f"{len(dc):,} rows × {len(dc.columns)} cols")
        null_rate_check(r, dc, ["country_code", "country_name"], "dim_country")

    # dim_area
    da = load("dim_area.parquet")
    if da is not None:
        r.info("dim_area", f"{len(da):,} rows × {len(da.columns)} cols")
        null_rate_check(r, da, ["area_code", "area_title"], "dim_area")

    # dim_visa_ceiling
    dvc = load("dim_visa_ceiling.parquet")
    if dvc is not None:
        r.info("dim_visa_ceiling", f"{len(dvc):,} rows × {len(dvc.columns)} cols")
        if "annual_limit" in dvc.columns:
            range_check(r, dvc, "annual_limit", 0, 1_000_000, "dim_visa_ceiling")

    # dim_visa_class
    dvl = load("dim_visa_class.parquet")
    if dvl is not None:
        r.info("dim_visa_class", f"{len(dvl):,} rows × {len(dvl.columns)} cols")

    return r


def audit_feature_tables():
    """All feature/metric/model output tables."""
    r = AuditResult("feature_tables")

    # employer_features
    ef = load("employer_features.parquet")
    if ef is not None:
        r.info("employer_features", f"{len(ef):,} rows × {len(ef.columns)} cols")
        null_rate_check(r, ef, ["employer_id"], "employer_features")
        # Approval rate should be 0-1
        for col in ["approval_rate", "cert_rate", "perm_approval_rate", "lca_cert_rate"]:
            if col in ef.columns:
                range_check(r, ef, col, 0, 1.01, "employer_features")
        # Wage ratio
        if "wage_ratio" in ef.columns:
            range_check(r, ef, "wage_ratio", 0, 10, "employer_features")
            outlier_check_iqr(r, ef, "wage_ratio", "employer_features", k=4.0)

    # employer_friendliness_scores
    efs = load("employer_friendliness_scores.parquet")
    if efs is not None:
        r.info("efs", f"{len(efs):,} rows × {len(efs.columns)} cols")
        if "efs_score" in efs.columns:
            range_check(r, efs, "efs_score", 0, 100, "efs")
            # Distribution of tiers
            if "tier" in efs.columns:
                tier_dist = efs["tier"].value_counts()
                r.info("efs_tiers", f"Tier distribution: {dict(tier_dist)}")

    # salary_benchmarks
    sb = load("salary_benchmarks.parquet")
    if sb is not None:
        r.info("salary_benchmarks", f"{len(sb):,} rows × {len(sb.columns)} cols")
        for col in ["median_wage", "mean_wage"]:
            if col in sb.columns:
                wage_plausibility(r, sb, col, "salary_benchmarks")

    # soc_demand_metrics
    sdm = load("soc_demand_metrics.parquet")
    if sdm is not None:
        r.info("soc_demand_metrics", f"{len(sdm):,} rows × {len(sdm.columns)} cols")
        for col in sdm.select_dtypes(include=[np.number]).columns:
            neg = (sdm[col].dropna() < 0).sum()
            if neg > 0:
                r.warning("negative", f"soc_demand.{col}: {neg:,} negative values")

    # visa_demand_metrics
    vdm = load("visa_demand_metrics.parquet")
    if vdm is not None:
        r.info("visa_demand_metrics", f"{len(vdm):,} rows × {len(vdm.columns)} cols")
        for col in vdm.select_dtypes(include=[np.number]).columns:
            neg = (vdm[col].dropna() < 0).sum()
            if neg > 0:
                r.warning("negative", f"visa_demand.{col}: {neg:,} negative values")

    # worksite_geo_metrics
    wgm = load("worksite_geo_metrics.parquet")
    if wgm is not None:
        r.info("worksite_geo_metrics", f"{len(wgm):,} rows × {len(wgm.columns)} cols")

    # pd_forecasts
    pf = load("pd_forecasts.parquet")
    if pf is not None:
        r.info("pd_forecasts", f"{len(pf):,} rows × {len(pf.columns)} cols")

    # queue_depth_estimates
    qde = load("queue_depth_estimates.parquet")
    if qde is not None:
        r.info("queue_depth_estimates", f"{len(qde):,} rows × {len(qde.columns)} cols")
        if "est_wait_years" in qde.columns:
            range_check(r, qde, "est_wait_years", 0, 200, "queue_depth")

    return r


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

ALL_AUDITS = {
    "fact_perm":              audit_fact_perm,
    "fact_lca":               audit_fact_lca,
    "fact_oews":              audit_fact_oews,
    "fact_cutoffs":           audit_fact_cutoffs,
    "fact_visa_applications": audit_fact_visa_applications,
    "fact_iv_post":           audit_fact_iv_post,
    "fact_niv_issuance":      audit_fact_niv_issuance,
    "fact_visa_issuance":     audit_fact_visa_issuance,
    "fact_h1b_employer_hub":  audit_fact_h1b_employer_hub,
    "fact_bls_ces":           audit_fact_bls_ces,
    "fact_dhs_admissions":    audit_fact_dhs_admissions,
    "fact_uscis_approvals":   audit_fact_uscis_approvals,
    "fact_warn_events":       audit_fact_warn_events,
    "dim_tables":             audit_dim_tables,
    "feature_tables":         audit_feature_tables,
}


def main():
    parser = argparse.ArgumentParser(description="P2 Data Sanity Audit")
    parser.add_argument("--source", "-s", help="Audit a single source (e.g. fact_perm)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.source:
        if args.source not in ALL_AUDITS:
            print(f"Unknown source: {args.source}")
            print(f"Available: {', '.join(ALL_AUDITS.keys())}")
            sys.exit(1)
        audits = {args.source: ALL_AUDITS[args.source]}
    else:
        audits = ALL_AUDITS

    total_crit, total_warn = 0, 0
    for name, fn in audits.items():
        result = fn()
        c, w = result.print_summary()
        total_crit += c
        total_warn += w

    print(f"\n{'='*80}")
    print(f"{BOLD}GRAND TOTAL: {RED}{total_crit} critical{RESET}, {YELLOW}{total_warn} warnings{RESET}")
    print(f"{'='*80}")

    sys.exit(1 if total_crit > 0 else 0)


if __name__ == "__main__":
    main()
