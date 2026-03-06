#!/usr/bin/env python3
"""
Build employer_salary_profiles.parquet — the core salary artifact for P3 Salary page.

Combines wage data from:
  - fact_lca  (H-1B/H-1B1/E-3 LCA filings, 9.5M+ rows) — visa_type='H-1B'
  - fact_perm (PERM labor certifications, 1.67M rows)    — visa_type='PERM'
  - fact_oews (BLS OEWS prevailing wages)                — market benchmark

Output grain: employer_id × soc_code × visa_type × fiscal_year
Each row contains:
  - Filing counts
  - Salary stats (mean, median, p10, p25, p75, p90)
  - Prevailing wage benchmark (from OEWS)
  - Wage premium vs market

Cross-used by:
  - P3 Salary page (employer pay by role, top payers, trends)
  - P3 Employer Score page (wage competitiveness detail)
  - P2 employer_features (wage_ratio enrichment)

Usage: python scripts/make_employer_salary_profiles.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.normalize.mappings import normalize_employer_name  # noqa: E402

ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
OUT_PATH = TABLES / "employer_salary_profiles.parquet"
LOG_PATH = METRICS / "employer_salary_profiles.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ── Wage annualisation multipliers ──────────────────────────────────────────
WAGE_UNIT_MULT = {
    "year": 1.0, "yr": 1.0,
    "hour": 2080.0, "hr": 2080.0,
    "week": 52.0, "wk": 52.0,
    "month": 12.0, "mo": 12.0,
    "bi-weekly": 26.0, "bi": 26.0,
}

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED", "CERTIFIED - WITHDRAWN", "APPROVED"}
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _read_partitioned(dir_path: Path) -> pd.DataFrame:
    """Read Hive-style partitioned parquet directory."""
    pfiles = [f for f in dir_path.rglob("*.parquet")
              if not _excl(f) and "__HIVE_DEFAULT" not in str(f)]
    dfs = []
    for pf in sorted(pfiles):
        df = pd.read_parquet(pf)
        for part in pf.parts:
            if "=" in part:
                k, v = part.split("=", 1)
                if k not in df.columns:
                    try:
                        df[k] = int(v)
                    except ValueError:
                        df[k] = v
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _annualise(wage: pd.Series, unit: pd.Series) -> pd.Series:
    """Vectorised wage annualisation."""
    mult = unit.str.lower().str.strip().map(WAGE_UNIT_MULT).fillna(1.0)
    return (wage * mult).where(wage.notna() & (wage > 0))


def _load_lca() -> pd.DataFrame:
    """Load H-1B LCA data with annualised wages."""
    log.info("Loading fact_lca ...")
    lca_dir = TABLES / "fact_lca"
    if lca_dir.is_dir():
        df = _read_partitioned(lca_dir)
    else:
        df = pd.read_parquet(TABLES / "fact_lca.parquet")
    log.info(f"  fact_lca raw: {len(df):,} rows")

    # Only certified filings
    df["_status_upper"] = df["case_status"].str.upper().str.strip()
    df = df[df["_status_upper"].isin(APPROVED_STATUS)].copy()
    log.info(f"  After certified filter: {len(df):,}")

    # Annualise wages
    df["wage_rate_from"] = pd.to_numeric(df["wage_rate_from"], errors="coerce")
    df["prevailing_wage"] = pd.to_numeric(df["prevailing_wage"], errors="coerce")
    df["annual_wage"] = _annualise(df["wage_rate_from"], df["wage_unit"])
    df["annual_pw"] = _annualise(df["prevailing_wage"], df["pw_unit"])

    # Normalise SOC to 7-char
    df["soc_code_7"] = df["soc_code"].astype(str).str.strip().str[:7]

    # Fiscal year
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")

    # Determine visa_type from visa_class
    df["visa_type"] = df["visa_class"].str.upper().str.strip().apply(
        lambda v: "H-1B" if v in ("H-1B", "H1B", "R", "H-1B1") else
                  "E-3" if v == "E-3" else "H-1B"
    )

    keep = ["employer_id", "employer_name_raw", "soc_code_7", "soc_title",
            "job_title", "visa_type", "fiscal_year", "annual_wage", "annual_pw",
            "worksite_state", "naics_code", "is_fulltime"]
    return df[[c for c in keep if c in df.columns]].rename(
        columns={"employer_name_raw": "employer_name", "soc_code_7": "soc_code"}
    )


def _load_perm() -> pd.DataFrame:
    """Load PERM data with annualised wages."""
    log.info("Loading fact_perm ...")
    perm_dir = TABLES / "fact_perm"
    if perm_dir.is_dir():
        df = _read_partitioned(perm_dir)
    else:
        df = pd.read_parquet(TABLES / "fact_perm.parquet")
    log.info(f"  fact_perm raw: {len(df):,} rows")

    # Only certified filings
    df["_status_upper"] = df["case_status"].str.upper().str.strip()
    df = df[df["_status_upper"].isin(APPROVED_STATUS)].copy()
    log.info(f"  After certified filter: {len(df):,}")

    # Annualise wages
    df["wage_offer_from"] = pd.to_numeric(df["wage_offer_from"], errors="coerce")
    df["annual_wage"] = _annualise(df["wage_offer_from"], df["wage_offer_unit"])

    # Normalise SOC to 7-char
    if "soc_code" in df.columns:
        df["soc_code_7"] = df["soc_code"].astype(str).str.strip().str[:7]
    else:
        df["soc_code_7"] = None

    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
    df["visa_type"] = "PERM"
    df["annual_pw"] = np.nan  # PERM doesn't have per-row prevailing wage in our schema

    keep = ["employer_id", "employer_name", "soc_code_7", "job_title",
            "visa_type", "fiscal_year", "annual_wage", "annual_pw",
            "worksite_state", "naics_code", "is_fulltime"]
    return df[[c for c in keep if c in df.columns]].rename(
        columns={"soc_code_7": "soc_code"}
    )


def _load_oews_benchmarks() -> pd.DataFrame:
    """Load OEWS wage benchmarks for market comparison.
    Returns SOC-level national medians (latest year).
    """
    log.info("Loading OEWS benchmarks ...")
    oews_dir = TABLES / "fact_oews"
    if oews_dir.is_dir():
        df = _read_partitioned(oews_dir)
    else:
        df = pd.read_parquet(TABLES / "fact_oews.parquet")

    if "ref_year" in df.columns:
        df["ref_year"] = pd.to_numeric(df["ref_year"], errors="coerce")
        latest = df["ref_year"].max()
        df = df[df["ref_year"] == latest]
    log.info(f"  OEWS rows (latest year): {len(df):,}")

    # National-level medians (area_code containing '99' or NATIONAL type)
    dim_area_path = TABLES / "dim_area.parquet"
    nat_code = "99"
    if dim_area_path.exists():
        da = pd.read_parquet(dim_area_path)
        nat = da[da["area_type"] == "NATIONAL"]
        if len(nat) > 0:
            nat_code = str(nat.iloc[0]["area_code"])

    # Keep both area-level and national-level
    df["a_median"] = pd.to_numeric(df.get("a_median"), errors="coerce")
    nat_df = df[df["area_code"] == nat_code][["soc_code", "a_median"]].copy()
    nat_df = nat_df.rename(columns={"a_median": "oews_national_median"})
    nat_df = nat_df.dropna(subset=["oews_national_median"])
    log.info(f"  National SOC benchmarks: {len(nat_df):,}")
    return nat_df


def _build_profiles(combined: pd.DataFrame, oews: pd.DataFrame, log_lines: list) -> pd.DataFrame:
    """Aggregate combined LCA+PERM data into salary profiles."""
    log.info("Building salary profiles ...")

    # Drop rows without valid salary or employer
    combined = combined.dropna(subset=["annual_wage", "employer_id"]).copy()
    combined = combined[combined["annual_wage"] > 5000]  # filter out extreme low (data errors)
    combined = combined[combined["annual_wage"] < 1_000_000]  # filter extreme high
    log.info(f"  Valid wage rows: {len(combined):,}")

    # Group by employer × SOC × visa_type × fiscal_year
    grp_cols = ["employer_id", "soc_code", "visa_type", "fiscal_year"]
    grp = combined.groupby(grp_cols, observed=True, dropna=False)

    # Step 1: fast built-in aggregations
    log.info("  Step 1/3: core salary stats ...")
    agg = grp.agg(
        n_filings=("annual_wage", "count"),
        mean_salary=("annual_wage", "mean"),
        median_salary=("annual_wage", "median"),
        min_salary=("annual_wage", "min"),
        max_salary=("annual_wage", "max"),
        prevailing_wage_median=("annual_pw", "median"),
        employer_name=("employer_name", "first"),
        job_title_top=("job_title", "first"),
        worksite_state_top=("worksite_state", "first"),
    ).reset_index()

    # Step 2: quantiles via describe (much faster than lambda)
    log.info("  Step 2/3: percentile stats ...")
    pcts = grp["annual_wage"].quantile([0.10, 0.25, 0.75, 0.90]).unstack(level=-1)
    pcts.columns = ["p10_salary", "p25_salary", "p75_salary", "p90_salary"]
    pcts = pcts.reset_index()
    agg = agg.merge(pcts, on=grp_cols, how="left")

    log.info(f"  Profile rows (employer×soc×visa×fy): {len(agg):,}")
    log_lines.append(f"profile_rows_detailed: {len(agg):,}")

    # Step 3: round and join OEWS
    log.info("  Step 3/3: OEWS join + wage premium ...")

    # Round salary columns
    for col in ["mean_salary", "median_salary", "p10_salary", "p25_salary",
                "p75_salary", "p90_salary", "min_salary", "max_salary",
                "prevailing_wage_median"]:
        agg[col] = agg[col].round(0)

    # Join OEWS national benchmark
    agg["soc_code_join"] = agg["soc_code"].astype(str).str[:7]
    oews["soc_code_join"] = oews["soc_code"].astype(str).str[:7]
    agg = agg.merge(
        oews[["soc_code_join", "oews_national_median"]],
        on="soc_code_join", how="left"
    )
    agg.drop(columns=["soc_code_join"], inplace=True)

    # Compute wage premium vs OEWS
    agg["wage_premium_pct"] = np.where(
        agg["oews_national_median"].notna() & (agg["oews_national_median"] > 0),
        ((agg["median_salary"] - agg["oews_national_median"]) /
         agg["oews_national_median"] * 100).round(1),
        np.nan
    )

    # Compute wage premium vs LCA prevailing wage (where available)
    agg["wage_vs_pw_pct"] = np.where(
        agg["prevailing_wage_median"].notna() & (agg["prevailing_wage_median"] > 0),
        ((agg["median_salary"] - agg["prevailing_wage_median"]) /
         agg["prevailing_wage_median"] * 100).round(1),
        np.nan
    )

    # Sort for readability
    agg.sort_values(["employer_id", "soc_code", "visa_type", "fiscal_year"], inplace=True)
    agg.reset_index(drop=True, inplace=True)

    return agg


def _canonical_employer_names(df: pd.DataFrame, dim_emp: pd.DataFrame) -> pd.DataFrame:
    """Replace raw employer_name values with canonical names from dim_employer.

    Two-pass strategy:
    1. Primary: join on employer_id → canonical Title Case name from dim_employer.
    2. Fallback: for employer_ids NOT in dim_employer (LCA-only employers),
       apply normalize_employer_name() + title_case to clean up the raw name.

    Args:
        df:       DataFrame that has both ``employer_id`` and ``employer_name``.
        dim_emp:  dim_employer DataFrame with ``employer_id`` and
                  ``employer_name`` (canonical, Title Case).

    Returns:
        df with ``employer_name`` replaced by the canonical value.
    """
    if "employer_id" not in df.columns or "employer_name" not in df.columns:
        return df
    if dim_emp is None or dim_emp.empty:
        return df

    from src.normalize.mappings import normalize_employer_name, title_case_employer_name

    id_to_name = dim_emp.set_index("employer_id")["employer_name"].to_dict()
    df = df.copy()

    # Pass 1: canonical names from dim_employer
    df["_canonical"] = df["employer_id"].map(id_to_name)

    # Pass 2: fallback normalization for LCA-only employers
    missing_mask = df["_canonical"].isna()
    if missing_mask.any():
        df.loc[missing_mask, "_canonical"] = df.loc[missing_mask, "employer_name"].apply(
            lambda n: title_case_employer_name(normalize_employer_name(str(n))) or str(n)
        )

    df["employer_name"] = df["_canonical"].fillna(df["employer_name"])
    df.drop(columns=["_canonical"], inplace=True)
    return df


def _build_employer_yearly_summary(combined: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw records → employer × visa_type × fiscal_year summary.
    This powers the P3 'employer salary trend over time' view.

    Uses the raw combined DataFrame (before SOC aggregation) to compute a
    TRUE flat median — avoids the median-of-SOC-medians bias that appears
    when smaller employers have a skewed SOC distribution.  The profiles
    DataFrame is still used to carry n_soc_codes for informational purposes.
    """
    log.info("Building employer yearly summary (from raw records for accurate medians) ...")

    # ── True flat median from raw records ────────────────────────────────
    df = combined.dropna(subset=["annual_wage", "employer_id"]).copy()
    df = df[(df["annual_wage"] > 5000) & (df["annual_wage"] < 1_000_000)]

    grp_cols = ["employer_id", "visa_type", "fiscal_year"]
    agg = df.groupby(grp_cols, observed=True, dropna=False).agg(
        total_filings=("annual_wage", "count"),
        mean_salary=("annual_wage", "mean"),
        median_salary=("annual_wage", "median"),
        employer_name=("employer_name", "first"),
    ).reset_index()

    agg["mean_salary"] = agg["mean_salary"].round(0)
    agg["median_salary"] = agg["median_salary"].round(0)

    # ── Attach n_soc_codes from profiles (informational) ─────────────────
    soc_counts = (
        profiles[profiles["n_filings"] >= 1]
        .groupby(grp_cols, observed=True)["soc_code"]
        .nunique()
        .reset_index()
        .rename(columns={"soc_code": "n_soc_codes"})
    )
    agg = agg.merge(soc_counts, on=grp_cols, how="left")
    agg["n_soc_codes"] = agg["n_soc_codes"].fillna(0).astype(int)

    log.info(f"  Employer yearly summary rows: {len(agg):,}")
    return agg


def _build_soc_market_summary(profiles: pd.DataFrame) -> pd.DataFrame:
    """Aggregate across employers → SOC × visa_type × fiscal_year summary.
    This powers the P3 'which roles pay the most' view.
    """
    log.info("Building SOC market summary ...")
    df = profiles[profiles["n_filings"] >= 1].copy()

    # Weighted aggregation via pre-multiplication
    df["_w_mean"] = df["mean_salary"] * df["n_filings"]
    df["_w_med"] = df["median_salary"] * df["n_filings"]
    df["_w_p10"] = df["p10_salary"] * df["n_filings"]
    df["_w_p25"] = df["p25_salary"] * df["n_filings"]
    df["_w_p75"] = df["p75_salary"] * df["n_filings"]
    df["_w_p90"] = df["p90_salary"] * df["n_filings"]

    agg = df.groupby(["soc_code", "visa_type", "fiscal_year"], observed=True).agg(
        total_filings=("n_filings", "sum"),
        n_employers=("employer_id", "nunique"),
        _w_mean_sum=("_w_mean", "sum"),
        _w_med_sum=("_w_med", "sum"),
        _w_p10_sum=("_w_p10", "sum"),
        _w_p25_sum=("_w_p25", "sum"),
        _w_p75_sum=("_w_p75", "sum"),
        _w_p90_sum=("_w_p90", "sum"),
    ).reset_index()

    agg["market_mean"] = (agg["_w_mean_sum"] / agg["total_filings"]).round(0)
    agg["market_median"] = (agg["_w_med_sum"] / agg["total_filings"]).round(0)
    agg["market_p10"] = (agg["_w_p10_sum"] / agg["total_filings"]).round(0)
    agg["market_p25"] = (agg["_w_p25_sum"] / agg["total_filings"]).round(0)
    agg["market_p75"] = (agg["_w_p75_sum"] / agg["total_filings"]).round(0)
    agg["market_p90"] = (agg["_w_p90_sum"] / agg["total_filings"]).round(0)
    agg.drop(columns=["_w_mean_sum", "_w_med_sum", "_w_p10_sum", "_w_p25_sum", "_w_p75_sum", "_w_p90_sum"], inplace=True)

    log.info(f"  SOC market summary rows: {len(agg):,}")
    return agg


def main() -> None:
    log_lines: list[str] = []
    METRICS.mkdir(parents=True, exist_ok=True)
    TABLES.mkdir(parents=True, exist_ok=True)

    log.info("=" * 70)
    log.info("EMPLOYER SALARY PROFILES BUILDER")
    log.info("=" * 70)
    started = datetime.now(timezone.utc)

    # ── Load sources ────────────────────────────────────────────────────
    lca = _load_lca()
    log_lines.append(f"lca_certified: {len(lca):,}")

    perm = _load_perm()
    log_lines.append(f"perm_certified: {len(perm):,}")

    oews = _load_oews_benchmarks()

    # ── Combine ─────────────────────────────────────────────────────────
    log.info("\nCombining LCA + PERM ...")
    # Ensure matching columns
    common_cols = ["employer_id", "employer_name", "soc_code", "job_title",
                   "visa_type", "fiscal_year", "annual_wage", "annual_pw",
                   "worksite_state"]
    for col in common_cols:
        if col not in lca.columns:
            lca[col] = None
        if col not in perm.columns:
            perm[col] = None

    combined = pd.concat([lca[common_cols], perm[common_cols]], ignore_index=True)
    log.info(f"  Combined rows: {len(combined):,}")
    log_lines.append(f"combined_total: {len(combined):,}")

    # ── Build detailed profiles ─────────────────────────────────────────
    profiles = _build_profiles(combined, oews, log_lines)

    # ── Build summaries ─────────────────────────────────────────────────
    emp_yearly = _build_employer_yearly_summary(combined, profiles)
    soc_market = _build_soc_market_summary(profiles)

    # ── Canonicalize employer names ──────────────────────────────────────
    # Load dim_employer (always available at this point) and replace raw
    # employer_name values (e.g. "GOOGLE INC.") with canonical Title Case
    # names (e.g. "Google") in every output table that has employer_id.
    log.info("\nReplacing raw employer names with canonical names from dim_employer...")
    try:
        dim_emp = pd.read_parquet(TABLES / "dim_employer.parquet",
                                  columns=["employer_id", "employer_name"])
        pre_profile_variants = profiles["employer_name"].nunique()
        pre_yearly_variants = emp_yearly["employer_name"].nunique()

        profiles = _canonical_employer_names(profiles, dim_emp)
        emp_yearly = _canonical_employer_names(emp_yearly, dim_emp)

        post_profile_variants = profiles["employer_name"].nunique()
        post_yearly_variants = emp_yearly["employer_name"].nunique()

        log.info(f"  employer_salary_profiles: {pre_profile_variants:,} → {post_profile_variants:,} unique names")
        log.info(f"  employer_salary_yearly:   {pre_yearly_variants:,} → {post_yearly_variants:,} unique names")
        log_lines.append(f"canonical_name_dedup_profiles: {pre_profile_variants} -> {post_profile_variants}")
        log_lines.append(f"canonical_name_dedup_yearly: {pre_yearly_variants} -> {post_yearly_variants}")
    except FileNotFoundError:
        log.warning("  WARNING: dim_employer.parquet not found; skipping canonical name replacement")

    # ── Write outputs ───────────────────────────────────────────────────
    log.info("\nWriting outputs ...")

    # Main detailed profiles
    profiles.to_parquet(OUT_PATH, index=False, engine="pyarrow")
    log.info(f"  {OUT_PATH.name}: {len(profiles):,} rows")
    log_lines.append(f"employer_salary_profiles: {len(profiles):,} rows")

    # Employer yearly summary
    ey_path = TABLES / "employer_salary_yearly.parquet"
    emp_yearly.to_parquet(ey_path, index=False, engine="pyarrow")
    log.info(f"  {ey_path.name}: {len(emp_yearly):,} rows")
    log_lines.append(f"employer_salary_yearly: {len(emp_yearly):,} rows")

    # SOC market summary
    sm_path = TABLES / "soc_salary_market.parquet"
    soc_market.to_parquet(sm_path, index=False, engine="pyarrow")
    log.info(f"  {sm_path.name}: {len(soc_market):,} rows")
    log_lines.append(f"soc_salary_market: {len(soc_market):,} rows")

    # ── QA checks ───────────────────────────────────────────────────────
    log.info("\nQA checks ...")
    ok = True

    # Check salary ranges
    bad_salary = (profiles["median_salary"] < 5000) | (profiles["median_salary"] > 1_000_000)
    if bad_salary.any():
        n_bad = bad_salary.sum()
        log.warning(f"  WARN: {n_bad} profiles with median salary outside [5K, 1M]")
        log_lines.append(f"WARN: {n_bad} extreme salary profiles")
    else:
        log.info("  PASS: all median salaries in [5K, 1M]")
        log_lines.append("PASS: salary ranges OK")

    # Check wage premium distribution
    wp = profiles["wage_premium_pct"].dropna()
    if len(wp) > 0:
        wp_median = wp.median()
        log.info(f"  Wage premium vs OEWS: median={wp_median:.1f}%, "
                 f"range=[{wp.min():.0f}%, {wp.max():.0f}%]")
        log_lines.append(f"wage_premium_median: {wp_median:.1f}%")

    # Coverage stats
    h1b_rows = profiles[profiles["visa_type"] == "H-1B"]
    perm_rows = profiles[profiles["visa_type"] == "PERM"]
    log.info(f"\n  H-1B profiles: {len(h1b_rows):,}")
    log.info(f"  PERM profiles: {len(perm_rows):,}")
    log.info(f"  Unique employers: {profiles['employer_id'].nunique():,}")
    log.info(f"  Unique SOC codes: {profiles['soc_code'].nunique():,}")
    log.info(f"  Fiscal year range: {profiles['fiscal_year'].min()} - {profiles['fiscal_year'].max()}")

    log_lines.append(f"h1b_profiles: {len(h1b_rows):,}")
    log_lines.append(f"perm_profiles: {len(perm_rows):,}")
    log_lines.append(f"unique_employers: {profiles['employer_id'].nunique():,}")
    log_lines.append(f"unique_soc: {profiles['soc_code'].nunique():,}")

    # Top 10 employers by total filings (for log)
    top_emp = profiles.groupby("employer_name")["n_filings"].sum().nlargest(10)
    log.info("\n  Top 10 employers by filings:")
    for name, n in top_emp.items():
        log.info(f"    {name}: {n:,}")

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    log.info(f"\nCompleted in {elapsed:.1f}s")
    log_lines.append(f"elapsed_seconds: {elapsed:.1f}")

    # Write log
    with open(LOG_PATH, "w") as f:
        f.write("\n".join(log_lines))
    log.info(f"Log: {LOG_PATH}")
    log.info("\n✓ EMPLOYER SALARY PROFILES COMPLETE")


if __name__ == "__main__":
    main()
