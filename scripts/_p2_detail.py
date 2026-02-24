"""Detailed diagnostics for specific issues."""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"

def load(name):
    p = TABLES / f"{name}.parquet"
    d = TABLES / name
    if p.exists(): return pd.read_parquet(p)
    if d.exists():
        files = sorted(d.rglob("*.parquet"))
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()
    return None

# 1. employer_monthly_metrics - approval_rate distribution
print("\n=== employer_monthly_metrics ===")
emm = load("employer_monthly_metrics")
print(f"approval_rate sample: {emm['approval_rate'].describe()}")
print(f"approval_rate range: [{emm['approval_rate'].min():.4f}, {emm['approval_rate'].max():.4f}]")
print(f"approval_rate <0: {(emm['approval_rate']<0).sum()}")
print(f"approval_rate >1: {(emm['approval_rate']>1).sum()}")
print(f"approval_rate sample head: {list(emm['approval_rate'].head(10))}")
print(f"filings sample: {list(emm['filings'].head(5))}")
print(f"approvals sample: {list(emm['approvals'].head(5))}")

# 2. backlog_estimates - details
print("\n=== backlog_estimates ===")
bg = load("backlog_estimates")
print(f"backlog_months_to_clear_est unique values: {sorted(bg['backlog_months_to_clear_est'].unique()[:20])}")
print(f"advancement_days_12m_avg sample: {list(bg['advancement_days_12m_avg'].head(5))}")
print(f"inflow sample: {list(bg['inflow_estimate_12m'].head(5))}")
not999 = bg[bg['backlog_months_to_clear_est'] != 999]
print(f"Rows with backlog != 999: {len(not999)}")

# 3. fact_oews - check if ref_year exists somewhere
print("\n=== fact_oews columns detail ===")
oews = load("fact_oews")
print(f"oews full cols: {list(oews.columns)}")
print(f"oews sample:\n{oews.head(3)}")
# Check for ref_year in partitioned version
oews_dir = TABLES / "fact_oews"
if oews_dir.exists():
    for f in list(oews_dir.rglob("*.parquet"))[:3]:
        print(f"  partition: {f.relative_to(TABLES)}")

# 4. worksite_geo SOC normalization
print("\n=== worksite_geo unmapped SOCs ===")
wg = load("worksite_geo_metrics")
dim_soc = load("dim_soc")
soc_set = set(dim_soc["soc_code"].dropna())
unmapped = wg[wg["soc_code"].notna() & ~wg["soc_code"].isin(soc_set)]["soc_code"].value_counts().head(20)
print("Top unmapped SOC codes:")
print(unmapped.to_string())
# Show sample mapped SOCs from dim_soc
print(f"\nSample dim_soc codes: {list(dim_soc['soc_code'].head(10))}")

# 5. fact_cutoff_trends - what makes DFF vs FAD different
print("\n=== fact_cutoff_trends DFF vs FAD ===")
t = load("fact_cutoff_trends")
dff = t[t["chart"]=="DFF"]
fad = t[t["chart"]=="FAD"]
print(f"DFF rows: {len(dff)}, FAD rows: {len(fad)}")
# Compare queue_position_days for same key
pk = ["bulletin_year","bulletin_month","category","country"]
both = t[t.duplicated(subset=pk, keep=False)]
sample_key = (2020, 1, "EB2", "CHN")
sample = t[(t["bulletin_year"]==sample_key[0]) & (t["bulletin_month"]==sample_key[1]) & 
           (t["category"]==sample_key[2]) & (t["country"]==sample_key[3])]
if len(sample) == 0:
    sample = both.head(4)
print(f"Sample dual-chart rows:\n{sample[['bulletin_year','bulletin_month','category','country','chart','queue_position_days','monthly_advancement_days']].head(6)}")

# 6. salary_benchmarks - do we have state/national aggregates?
print("\n=== salary_benchmarks source levels ===")
sb = load("salary_benchmarks")
if "source_level" in sb.columns:
    print(f"source_level distribution:\n{sb['source_level'].value_counts()}")
else:
    print("No source_level column")
# Check if area_code has a null/national aggregate
na_rows = sb[sb["area_code"].isna()]
print(f"Rows with null area_code (national): {len(na_rows)}")
# Check for area_code patterns
print(f"Sample area_codes: {list(sb['area_code'].dropna().unique()[:5])}")

print("\n=== END DETAIL ===")
