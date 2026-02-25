"""Queue depth estimation feature builder.

Estimates how many applicants are ahead in each EB category × country
queue, based on PERM filing data, visa bulletin cutoffs, PD forecasts,
and annual visa allocation limits.

Produces: artifacts/tables/queue_depth_estimates.parquet

Inputs:
  - fact_perm/           (partitioned: certified filings with received_date & employer_country)
  - fact_cutoffs_all.parquet  (latest visa bulletin cutoff dates)
  - pd_forecasts.parquet      (cutoff advancement velocity)
  - dim_visa_ceiling.parquet  (annual visa allocation limits)

Key design decision:
  PERM does NOT record EB category (EB1/EB2/EB3).  We apply published
  distribution ratios to split all-EB PERM counts into category estimates.
  These ratios come from USCIS Annual Reports and visa usage statistics.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── EB category distribution ratios ──────────────────────────
# Estimated share of PERM filings that map to each EB category,
# by chargeability country.  Based on USCIS Annual Reports (FY2019-2023).
# Note: EB4/EB5 mostly don't use PERM, so we only split EB1/EB2/EB3.
EB_CATEGORY_RATIOS = {
    "IND": {"EB1": 0.08, "EB2": 0.57, "EB3": 0.35},
    "CHN": {"EB1": 0.15, "EB2": 0.50, "EB3": 0.35},
    "PHL": {"EB1": 0.10, "EB2": 0.35, "EB3": 0.55},
    "MEX": {"EB1": 0.10, "EB2": 0.35, "EB3": 0.55},
    "ROW": {"EB1": 0.15, "EB2": 0.40, "EB3": 0.45},
}

# Avg family members per principal applicant (consumes extra visa numbers)
FAMILY_MULTIPLIER = 1.8

# Fraction of certified PERMs that actually proceed to I-140 → I-485
CONVERSION_RATE = 0.75

# Country mapping: employer_country in fact_perm → chargeability in cutoffs
VISA_BULLETIN_COUNTRIES = {"IND", "CHN", "MEX", "PHL"}


def _map_to_chargeability(country_code: str) -> str:
    """Map PERM employer_country (beneficiary birth country) to visa bulletin chargeability."""
    if country_code in VISA_BULLETIN_COUNTRIES:
        return country_code
    return "ROW"


def _read_partitioned(table_dir: Path) -> pd.DataFrame:
    """Read a partitioned parquet directory (Hive-style) into a single DF."""
    pfiles = sorted(table_dir.rglob("*.parquet"))
    if not pfiles:
        return pd.DataFrame()
    dfs = []
    for pf in pfiles:
        pdf = pd.read_parquet(pf)
        for part in pf.parts:
            if "=" in part:
                col, val = part.split("=", 1)
                if col not in pdf.columns:
                    pdf[col] = val
        dfs.append(pdf)
    return pd.concat(dfs, ignore_index=True)


def _load_perm_filings(tables_dir: Path) -> pd.DataFrame:
    """Load certified PERM filings with PD and chargeability country."""
    perm_dir = tables_dir / "fact_perm"
    if not perm_dir.exists():
        raise FileNotFoundError(f"fact_perm not found at {perm_dir}")

    perm = _read_partitioned(perm_dir)

    # Filter to certified filings
    perm["case_status_upper"] = perm["case_status"].str.upper()
    perm = perm[perm["case_status_upper"].isin(["CERTIFIED", "CERTIFIED-EXPIRED"])].copy()

    # Parse received_date as priority date proxy
    perm["received_date"] = pd.to_datetime(perm["received_date"], errors="coerce")
    perm = perm[perm["received_date"].notna()].copy()

    # Map to chargeability country
    perm["chargeability"] = perm["employer_country"].apply(_map_to_chargeability)

    # Floor to monthly bucket
    perm["pd_month"] = perm["received_date"].dt.to_period("M").dt.to_timestamp()

    return perm[["chargeability", "pd_month"]].copy()


def _load_latest_cutoffs(tables_dir: Path) -> pd.DataFrame:
    """Load latest DFF cutoff for each EB category × country."""
    cutoffs_path = tables_dir / "fact_cutoffs_all.parquet"
    if not cutoffs_path.exists():
        raise FileNotFoundError(f"fact_cutoffs_all not found at {cutoffs_path}")

    fc = pd.read_parquet(cutoffs_path)
    fc["bulletin_date"] = pd.to_datetime(
        fc["bulletin_year"].astype(str) + "-" + fc["bulletin_month"].astype(str) + "-01",
        errors="coerce",
    )

    # Keep only EB1/EB2/EB3 + DFF chart + date-based (not Current/Unavailable)
    fc = fc[
        (fc["category"].isin(["EB1", "EB2", "EB3"]))
        & (fc["chart"] == "DFF")
        & (fc["status_flag"] == "D")
    ].copy()
    fc["cutoff_date"] = pd.to_datetime(fc["cutoff_date"], errors="coerce")

    # Latest bulletin per category × country
    latest = (
        fc.sort_values("bulletin_date", ascending=False)
        .drop_duplicates(["category", "country"])
        .set_index(["category", "country"])["cutoff_date"]
    )
    return latest


def _load_visa_ceiling(tables_dir: Path) -> dict:
    """Load per-country EB visa allocation limit."""
    vc_path = tables_dir / "dim_visa_ceiling.parquet"
    if not vc_path.exists():
        return {}

    vc = pd.read_parquet(vc_path)
    # EB_PER_COUNTRY row gives the per-country cap
    per_country_row = vc[vc["category"] == "EB_PER_COUNTRY"]
    per_country_cap = int(per_country_row["ceiling"].iloc[0]) if len(per_country_row) > 0 else 9800

    # Per-category worldwide limits
    cat_ceilings = {}
    for _, row in vc[vc["category"].isin(["EB1", "EB2", "EB3"])].iterrows():
        cat_ceilings[row["category"]] = int(row["ceiling"])

    return {"per_country_cap": per_country_cap, "category_ceilings": cat_ceilings}


def _load_forecast_velocity(tables_dir: Path) -> pd.DataFrame:
    """Load avg forecast velocity per category × country from pd_forecasts."""
    fc_path = tables_dir / "pd_forecasts.parquet"
    if not fc_path.exists():
        return pd.DataFrame()

    pf = pd.read_parquet(fc_path)
    # Average velocity across forecast horizon
    vel = (
        pf[pf["chart"] == "DFF"]
        .groupby(["category", "country"])["velocity_days_per_month"]
        .mean()
    )
    return vel


def build_queue_depth_estimates(
    tables_dir: Path,
    output_path: Path,
) -> pd.DataFrame:
    """Build queue depth estimation feature table.

    Parameters
    ----------
    tables_dir : Path
        Directory containing curated tables (fact_perm/, fact_cutoffs_all.parquet, etc.)
    output_path : Path
        Where to write the output parquet file.

    Returns
    -------
    pd.DataFrame with one row per (category × country × pd_month).
    """
    print("  Loading PERM filings...")
    perm = _load_perm_filings(tables_dir)
    print(f"  {len(perm):,} certified PERM filings with valid PD and country")

    print("  Loading latest cutoffs...")
    cutoffs = _load_latest_cutoffs(tables_dir)

    print("  Loading visa ceiling...")
    ceiling_info = _load_visa_ceiling(tables_dir)
    per_country_cap = ceiling_info.get("per_country_cap", 9800)
    cat_ceilings = ceiling_info.get("category_ceilings", {})

    print("  Loading forecast velocity...")
    velocities = _load_forecast_velocity(tables_dir)

    # Count PERM filings per chargeability × pd_month
    counts = (
        perm.groupby(["chargeability", "pd_month"])
        .size()
        .reset_index(name="perm_filings_certified")
    )

    # Build output: explode across EB categories
    rows = []
    categories = ["EB1", "EB2", "EB3"]
    countries = sorted(counts["chargeability"].unique())

    for category in categories:
        for country in countries:
            country_counts = counts[counts["chargeability"] == country].copy()
            if country_counts.empty:
                continue

            # EB category ratio for this country
            ratios = EB_CATEGORY_RATIOS.get(country, EB_CATEGORY_RATIOS["ROW"])
            cat_ratio = ratios.get(category, 0.33)

            # Current cutoff for this category × country
            cutoff_date = cutoffs.get((category, country), pd.NaT)

            # Forecast velocity (days/month)
            vel_key = (category, country)
            velocity = velocities.get(vel_key, np.nan) if len(velocities) > 0 else np.nan

            # Annual visa allocation for this category × country
            cat_worldwide = cat_ceilings.get(category, 40040)
            # Per-country cap is ~7% of total EB (140K) = 9,800
            # But each category's per-country share is ~7% of that category
            annual_allocation = min(int(cat_worldwide * 0.07), per_country_cap)

            # For oversubscribed countries (IND, CHN), actual allocation is the per-country share
            # For ROW, allocation is effectively unlimited (current for most categories)
            if country == "ROW":
                annual_allocation = cat_worldwide  # ROW gets remainder

            # Compute cumulative ahead from cutoff
            # Sort by pd_month ascending
            country_counts = country_counts.sort_values("pd_month")

            for _, row in country_counts.iterrows():
                pd_month = row["pd_month"]
                raw_filings = row["perm_filings_certified"]

                # Estimated filings for this category
                est_cat_filings = int(raw_filings * cat_ratio)

                # Is this PD month ahead of current cutoff?
                if pd.notna(cutoff_date) and pd_month > cutoff_date:
                    is_ahead = True
                else:
                    is_ahead = False

                rows.append({
                    "category": category,
                    "country": country,
                    "pd_month": pd_month,
                    "perm_filings_certified": int(raw_filings),
                    "eb_category_ratio": round(cat_ratio, 3),
                    "est_category_filings": est_cat_filings,
                    "est_applicants_with_dependents": int(est_cat_filings * CONVERSION_RATE * FAMILY_MULTIPLIER),
                    "current_cutoff_date": cutoff_date,
                    "is_ahead_of_cutoff": is_ahead,
                    "annual_visa_allocation": annual_allocation,
                    "velocity_days_per_month": round(velocity, 1) if pd.notna(velocity) else None,
                })

    result = pd.DataFrame(rows)

    if result.empty:
        print("  WARNING: No queue depth estimates generated")
        result.to_parquet(output_path)
        return result

    # Compute cumulative applicants ahead of cutoff for each category × country
    # (only counting pd_months that are ahead of the cutoff)
    result["cumulative_ahead"] = 0
    for (cat, cty), grp in result.groupby(["category", "country"]):
        ahead_mask = grp["is_ahead_of_cutoff"]
        cum = grp.loc[ahead_mask, "est_applicants_with_dependents"].cumsum()
        result.loc[cum.index, "cumulative_ahead"] = cum.astype(int)

    # Estimated wait (years) = cumulative_ahead / annual_visa_allocation
    result["est_wait_years"] = np.where(
        (result["cumulative_ahead"] > 0) & (result["annual_visa_allocation"] > 0),
        np.round(result["cumulative_ahead"] / result["annual_visa_allocation"], 1),
        0,
    )

    # Estimated months until cutoff reaches this PD month
    result["est_months_to_current"] = np.nan
    for idx, row in result.iterrows():
        if row["is_ahead_of_cutoff"] and pd.notna(row["current_cutoff_date"]) and pd.notna(row["velocity_days_per_month"]) and row["velocity_days_per_month"] > 0:
            gap_days = (row["pd_month"] - row["current_cutoff_date"]).days
            if gap_days > 0:
                result.at[idx, "est_months_to_current"] = round(gap_days / row["velocity_days_per_month"], 0)

    # Confidence band based on data quality
    def _confidence(row):
        if row["country"] in ("IND", "CHN") and row["category"] in ("EB2", "EB3"):
            return "medium"  # Best data coverage for oversubscribed categories
        elif row["country"] == "ROW":
            return "low"  # ROW is typically current, estimates less meaningful
        else:
            return "medium-low"
    result["confidence"] = result.apply(_confidence, axis=1)

    # Add metadata
    result["generated_at"] = datetime.now(timezone.utc).isoformat()

    # Sort output
    result = result.sort_values(["category", "country", "pd_month"]).reset_index(drop=True)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    print(f"  Written {len(result):,} rows to {output_path}")
    print(f"  Categories: {sorted(result['category'].unique())}")
    print(f"  Countries: {sorted(result['country'].unique())}")

    # Summary stats
    for (cat, cty), grp in result.groupby(["category", "country"]):
        ahead = grp[grp["is_ahead_of_cutoff"]]
        if len(ahead) > 0:
            total_ahead = ahead["cumulative_ahead"].max()
            max_wait = ahead["est_wait_years"].max()
            print(f"    {cat} {cty}: {total_ahead:,} est. applicants ahead, ~{max_wait:.0f}yr wait")

    return result
