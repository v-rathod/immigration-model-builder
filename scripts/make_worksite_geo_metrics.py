#!/usr/bin/env python3
"""
STEP 4 — Build worksite_geo_metrics.parquet
Geo aggregates for heatmaps (state/area/soc_area).

Inputs: fact_perm, fact_lca, dim_area, fact_oews
Output: artifacts/tables/worksite_geo_metrics.parquet
Log:    artifacts/metrics/worksite_geo_metrics.log
"""
import re
import os, sys, logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")
OUT_PATH = TABLES / "worksite_geo_metrics.parquet"
LOG_PATH = METRICS / "worksite_geo_metrics.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED"}

WAGE_MULTIPLIERS = {
    "Hour": 2080,
    "hr": 2080,
    "Bi-Weekly": 26,
    "bi-weekly": 26,
    "Week": 52,
    "weekly": 52,
    "Month": 12,
    "monthly": 12,
    "Year": 1,
    "annual": 1,
    "yearly": 1,
}


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _read_partitioned_cols(dir_path: Path, cols: list) -> pd.DataFrame:
    pfiles = [f for f in dir_path.rglob("*.parquet")
              if not _excl(f) and "__HIVE_DEFAULT" not in str(f)]
    dfs = []
    for pf in sorted(pfiles):
        try:
            avail = pd.read_parquet(pf, columns=None).columns.tolist()
            read_cols = [c for c in cols if c in avail]
            df = pd.read_parquet(pf, columns=read_cols)
            for part in pf.parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k not in df.columns and k in cols:
                        try:
                            df[k] = int(v)
                        except ValueError:
                            df[k] = v
            dfs.append(df)
        except Exception as e:
            log.warning(f"Error reading {pf}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def annualize_wage(wage_series: pd.Series, unit_series: pd.Series) -> pd.Series:
    mult = unit_series.map(WAGE_MULTIPLIERS).fillna(1.0)
    return wage_series * mult


def build_geo_metrics(log_lines: list) -> pd.DataFrame:
    # Load dim_area for state mapping
    dim_area = pd.read_parquet(TABLES / "dim_area.parquet")[
        ["area_code", "state_abbr", "area_title"]
    ]
    state_to_area = dim_area[dim_area["area_code"].notna()].copy()

    # Load fact_oews for wage benchmarks (national + area)
    oews_dir = TABLES / "fact_oews"
    oews_files = [f for f in oews_dir.rglob("*.parquet") if not _excl(f)]
    oews_dfs = []
    for pf in sorted(oews_files):
        df_o = pd.read_parquet(pf, columns=["area_code", "soc_code", "a_median", "h_median"])
        for part in pf.parts:
            if "=" in part:
                k, v = part.split("=", 1)
                if k not in df_o.columns:
                    df_o[k] = v
        oews_dfs.append(df_o)
    df_oews = pd.concat(oews_dfs, ignore_index=True) if oews_dfs else pd.DataFrame()

    # OEWS: convert h_median to annual where a_median missing
    if len(df_oews):
        df_oews["oews_median"] = df_oews["a_median"].where(
            df_oews["a_median"].notna(),
            df_oews["h_median"] * 2080,
        )
        # National fallback (area_code null or national code)
        national_mask = df_oews["area_code"].astype(str).isin(["0", "N", "nan", ""]) | df_oews["area_code"].isna()
        oews_national = df_oews[national_mask][["soc_code", "oews_median"]].rename(
            columns={"oews_median": "oews_median_nat"}
        ).groupby("soc_code", as_index=False)["oews_median_nat"].median()
        oews_area = df_oews[~national_mask][["area_code", "soc_code", "oews_median"]].copy()
    else:
        oews_national = pd.DataFrame(columns=["soc_code", "oews_median_nat"])
        oews_area = pd.DataFrame(columns=["area_code", "soc_code", "oews_median"])

    log_lines.append(f"oews_area rows: {len(oews_area):,}  oews_national soc codes: {len(oews_national)}")

    all_grains = []

    for dataset, fp_dir, wage_col, unit_col in [
        ("PERM", TABLES / "fact_perm", "wage_offer_from", "wage_offer_unit"),
        ("LCA", TABLES / "fact_lca", "wage_rate_from", "wage_unit"),
    ]:
        if not fp_dir.exists():
            log_lines.append(f"WARN: {fp_dir} not found, skipping {dataset}")
            continue

        needed_cols = [
            "case_number", "case_status", "employer_id", "soc_code",
            "worksite_state", "worksite_city",
            wage_col, unit_col, "decision_date",
        ]
        log.info(f"Loading {dataset}...")
        df = _read_partitioned_cols(fp_dir, needed_cols)
        if df.empty:
            continue

        log_lines.append(f"{dataset}: {len(df):,} rows loaded")
        # Case-insensitive status match (PERM has mixed-case 'Certified'/'CERTIFIED')
        df["is_approved"] = df["case_status"].str.upper().isin(APPROVED_STATUS).astype(int)
        df["annualized_wage"] = np.nan
        if wage_col in df.columns and unit_col in df.columns:
            df[wage_col] = pd.to_numeric(df[wage_col], errors="coerce")
            df["annualized_wage"] = annualize_wage(df[wage_col], df.get(unit_col, pd.Series(["Year"] * len(df))))

        # Map state → area_code via dim_area (pick first match)
        state_map = state_to_area.groupby("state_abbr")["area_code"].first().to_dict()
        df["area_code"] = df["worksite_state"].map(state_map)

        # Normalize soc_code: null out malformed entries
        # Valid format is XX-XXXX (e.g., '15-1131').  Blank, numeric-only, and
        # text/address strings are set to NaN so they don’t pollute SOC coverage.
        _SOC_FMT = re.compile(r"^\d{2}-\d{4}$")
        if "soc_code" in df.columns:
            raw_soc = df["soc_code"].astype(str).str.strip()
            is_valid = raw_soc.str.match(_SOC_FMT)
            n_malformed = int((~is_valid).sum())
            if n_malformed:
                log_lines.append(f"{dataset}: nulling {n_malformed:,} malformed soc_code values")
            df["soc_code"] = df["soc_code"].where(is_valid)

        def _agg(g_df, keys, gname):
            agg = g_df.groupby(keys, observed=True).agg(
                filings_count=("case_number", "count"),
                approvals_count=("is_approved", "sum"),
                offered_median=("annualized_wage", "median"),
                distinct_employers=("employer_id", "nunique"),
            ).reset_index()
            agg["dataset"] = dataset
            agg["grain"] = gname
            return agg

        # State grain
        if "worksite_state" in df.columns:
            st_agg = _agg(df.dropna(subset=["worksite_state"]), ["worksite_state"], "state")
            st_agg.rename(columns={"worksite_state": "state"}, inplace=True)
            all_grains.append(st_agg)

        # Area grain
        if "area_code" in df.columns:
            ar_agg = _agg(df.dropna(subset=["area_code"]), ["area_code"], "area")
            ar_agg["state"] = ar_agg["area_code"].map(
                state_to_area.set_index("area_code")["state_abbr"].to_dict()
            )
            all_grains.append(ar_agg)

        # SOC × area grain
        if "soc_code" in df.columns and "area_code" in df.columns:
            sub = df.dropna(subset=["area_code", "soc_code"])
            sa_agg = _agg(sub, ["area_code", "soc_code"], "soc_area")
            sa_agg.rename(columns={
                "filings_count": "filings_count_soc_area",
                "offered_median": "offered_median_soc_area",
            }, inplace=True)
            all_grains.append(sa_agg)

        # City grain (worksite_city × state)
        if "worksite_city" in df.columns and "worksite_state" in df.columns:
            city_sub = df.dropna(subset=["worksite_city", "worksite_state"])
            if len(city_sub):
                city_agg = _agg(city_sub, ["worksite_state", "worksite_city"], "city")
                city_agg.rename(columns={"worksite_state": "state", "worksite_city": "city"}, inplace=True)
                all_grains.append(city_agg)
                log_lines.append(f"{dataset}: city grain rows: {len(city_agg):,}")

    if not all_grains:
        log_lines.append("WARN: no grain data produced")
        return pd.DataFrame(columns=[
            "grain", "state", "city", "area_code", "soc_code", "filings_count", "approvals_count",
            "offered_median", "competitiveness_ratio", "distinct_employers", "dataset"
        ])

    df_out = pd.concat(all_grains, ignore_index=True)

    # ---- Competitiveness ratio (per-grain approach) ----
    #
    # For each grain, pick the best available OEWS wage benchmark:
    #   soc_area:  OEWS(area, soc) → fallback OEWS_national(soc)
    #   area:      OEWS median across all SOCs in that area
    #   state:     OEWS median across all areas & SOCs in that state
    #   city:      same as state (no city-level OEWS exists)
    #
    # Build helper lookups from OEWS data + dim_area:

    df_out["competitiveness_ratio"] = np.nan

    if len(oews_area) and len(state_to_area):
        # Area → state mapping
        area_state = state_to_area.set_index("area_code")["state_abbr"].to_dict()

        # OEWS area-soc level (finest grain)
        oews_area_soc = oews_area.groupby(["area_code", "soc_code"])["oews_median"].median().reset_index()

        # OEWS area level (all SOCs in area)
        oews_area_all = oews_area.groupby("area_code")["oews_median"].median().reset_index()
        oews_area_all.rename(columns={"oews_median": "oews_area_median"}, inplace=True)

        # OEWS state level: map area→state, then aggregate
        oews_area_copy = oews_area.copy()
        oews_area_copy["state"] = oews_area_copy["area_code"].map(area_state)
        oews_state_all = oews_area_copy.groupby("state")["oews_median"].median().reset_index()
        oews_state_all.rename(columns={"oews_median": "oews_state_median"}, inplace=True)

        # Process each grain separately
        for grain_name in df_out["grain"].unique():
            mask = df_out["grain"] == grain_name

            if grain_name == "soc_area":
                # Best: exact (area_code, soc_code) match, fallback to national SOC
                sub = df_out.loc[mask].copy()
                sub = sub.merge(oews_area_soc, on=["area_code", "soc_code"], how="left")
                sub = sub.merge(oews_national, on="soc_code", how="left")
                ref = sub["oews_median"].where(sub["oews_median"].notna(), sub["oews_median_nat"])
                wage_col = "offered_median_soc_area" if "offered_median_soc_area" in sub.columns else "offered_median"
                if wage_col in sub.columns:
                    df_out.loc[mask, "competitiveness_ratio"] = (
                        sub[wage_col].values / ref.replace(0, np.nan).values
                    )

            elif grain_name == "area":
                # Use area-level overall OEWS median, fallback to state
                sub = df_out.loc[mask].copy()
                sub = sub.merge(oews_area_all, on="area_code", how="left")
                if "state" in sub.columns:
                    sub = sub.merge(oews_state_all, on="state", how="left")
                    ref = sub["oews_area_median"].where(
                        sub["oews_area_median"].notna(),
                        sub.get("oews_state_median", np.nan),
                    )
                else:
                    ref = sub["oews_area_median"]
                if "offered_median" in sub.columns:
                    df_out.loc[mask, "competitiveness_ratio"] = (
                        sub["offered_median"].values / ref.replace(0, np.nan).values
                    )

            elif grain_name in ("state", "city"):
                # Use state-level OEWS median
                sub = df_out.loc[mask].copy()
                sub = sub.merge(oews_state_all, on="state", how="left")
                if "offered_median" in sub.columns:
                    df_out.loc[mask, "competitiveness_ratio"] = (
                        sub["offered_median"].values / sub["oews_state_median"].replace(0, np.nan).values
                    )

        filled = df_out["competitiveness_ratio"].notna().sum()
        total = len(df_out)
        log_lines.append(f"competitiveness_ratio filled: {filled:,}/{total:,} ({100*filled/max(total,1):.1f}%)")

        cr_warn = df_out["competitiveness_ratio"].gt(2.5).sum()
        if cr_warn:
            log_lines.append(f"WARN: {cr_warn} rows with competitiveness_ratio > 2.5")

    log_lines.append(f"Output rows: {len(df_out):,}")
    return df_out.reset_index(drop=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== worksite_geo_metrics build {datetime.now(timezone.utc).isoformat()} ==="]

    df_out = build_geo_metrics(log_lines)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_worksite_geo_metrics.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log_lines.append(f"Written: {OUT_PATH}")
        log.info(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"worksite_geo_metrics: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
