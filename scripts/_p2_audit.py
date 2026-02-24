"""Quick diagnostic audit before hardening pass."""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"

def rc(name):
    p = TABLES / f"{name}.parquet"
    d = TABLES / name
    if p.exists(): return pq.read_metadata(p).num_rows
    if d.exists(): return sum(pq.read_metadata(f).num_rows for f in d.rglob("*.parquet"))
    return -1

def load(name):
    p = TABLES / f"{name}.parquet"
    d = TABLES / name
    if p.exists(): return pd.read_parquet(p)
    if d.exists():
        files = sorted(d.rglob("*.parquet"))
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()
    return None

print("\n=== AUDIT RESULTS ===\n")

# --- OEWS ref_years ---
df_oews = load("fact_oews")
if df_oews is not None:
    print(f"fact_oews rows: {len(df_oews):,}")
    if "ref_year" in df_oews.columns:
        print(f"  ref_years: {sorted(df_oews['ref_year'].unique())}")
    print(f"  cols: {list(df_oews.columns)[:8]}")
else:
    print("fact_oews: MISSING")

# --- fact_cutoff_trends PK dups ---
df_trends = load("fact_cutoff_trends")
if df_trends is not None:
    pk4 = ["bulletin_year","bulletin_month","category","country"]
    pk5 = ["bulletin_year","bulletin_month","chart","category","country"]
    pk4_avail = [c for c in pk4 if c in df_trends.columns]
    pk5_avail = [c for c in pk5 if c in df_trends.columns]
    dups4 = df_trends.duplicated(subset=pk4_avail).sum()
    dups5 = df_trends.duplicated(subset=pk5_avail).sum()
    print(f"\nfact_cutoff_trends: {len(df_trends):,} rows")
    print(f"  PK(4-col) dups: {dups4:,}")
    print(f"  PK(5-col with chart) dups: {dups5:,}")
    print(f"  charts: {sorted(df_trends['chart'].unique()) if 'chart' in df_trends.columns else 'N/A'}")
    # Count distinct bulletin_year/month pairs
    bym = df_trends.groupby(pk4_avail).size().reset_index(name="cnt")
    print(f"  Unique (year,month,cat,country) groups: {len(bym):,}")
    multi = bym[bym["cnt"] > 1]
    print(f"  Groups with >1 row: {len(multi):,}")
    if len(multi) > 0:
        print(f"  Sample multi-row group:\n{multi.head(2)}")

# --- salary_benchmarks null pcts ---
sb = load("salary_benchmarks")
if sb is not None:
    print(f"\nsalary_benchmarks: {len(sb):,} rows")
    for col in ["p10","p25","median","p75","p90"]:
        if col in sb.columns:
            pct = sb[col].isna().mean()
            print(f"  {col} null: {pct*100:.2f}%")
    rows_any_null = sb[["p10","p25","median","p75","p90"]].isna().any(axis=1).sum()
    print(f"  rows_with_any_null_percentile: {rows_any_null:,} ({rows_any_null/len(sb)*100:.2f}%)")
    if "soc_code" in sb.columns and "area_code" in sb.columns:
        print(f"  unique soc_codes: {sb['soc_code'].nunique():,}")
        print(f"  unique area_codes: {sb['area_code'].nunique():,}")
        print(f"  cols: {list(sb.columns)}")

# --- worksite_geo SOC coverage ---
wg = load("worksite_geo_metrics")
dim_soc = load("dim_soc")
if wg is not None and dim_soc is not None:
    print(f"\nworksite_geo_metrics: {len(wg):,} rows")
    print(f"  cols: {list(wg.columns)}")
    if "soc_code" in wg.columns and "soc_code" in dim_soc.columns:
        soc_set = set(dim_soc["soc_code"].dropna())
        total = wg["soc_code"].notna().sum()
        mapped = wg["soc_code"].isin(soc_set).sum()
        print(f"  soc exact coverage: {mapped/total*100:.2f}% ({mapped:,}/{total:,})")
        # Show sample unmapped SOCs
        if total > mapped:
            unmapped = wg[wg["soc_code"].notna() & ~wg["soc_code"].isin(soc_set)]["soc_code"].unique()[:10]
            print(f"  sample unmapped SOCs: {list(unmapped)}")
    if "filings_count" in wg.columns:
        print(f"  total filings_count: {wg['filings_count'].sum():,.0f}")

# --- employer_monthly distribution ---
emm = load("employer_monthly_metrics")
if emm is not None:
    print(f"\nemployer_monthly_metrics: {len(emm):,} rows")
    print(f"  cols: {list(emm.columns)}")
    if "approval_rate" in emm.columns:
        outside = emm[(emm["approval_rate"] < 0.4) | (emm["approval_rate"] > 1.0)]
        print(f"  approval_rate outside [0.4,1.0]: {len(outside):,} ({len(outside)/len(emm)*100:.2f}%)")

# --- employer_features wage_ratio ---
ef = load("employer_features")
if ef is not None:
    print(f"\nemployer_features: {len(ef):,} rows")
    print(f"  cols: {list(ef.columns)[:15]}")
    if "wage_ratio_med" in ef.columns:
        wr_pct = ef["wage_ratio_med"].notna().mean()
        print(f"  wage_ratio_med coverage: {wr_pct*100:.2f}%")
    else:
        print("  wage_ratio_med: NOT PRESENT")

# --- backlog clamping ---
bg = load("backlog_estimates")
if bg is not None:
    print(f"\nbacklog_estimates: {len(bg):,} rows")
    print(f"  cols: {list(bg.columns)}")
    if "backlog_months_to_clear_est" in bg.columns:
        vals = bg["backlog_months_to_clear_est"].dropna()
        outside = ((vals < 0) | (vals > 600)).sum()
        print(f"  out_of_range [0,600]: {outside:,} ({outside/len(vals)*100:.2f}%)")
        print(f"  max: {vals.max():.1f}, min: {vals.min():.1f}")

# --- dim_soc sample ---
if dim_soc is not None:
    print(f"\ndim_soc: {len(dim_soc):,} rows")
    print(f"  sample soc_codes: {list(dim_soc['soc_code'].head(5))}")

print("\n=== END AUDIT ===\n")
