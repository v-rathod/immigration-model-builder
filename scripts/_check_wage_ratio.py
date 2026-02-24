#!/usr/bin/env python3
"""Diagnose why wage_ratio_med is 0% coverage in employer_features."""
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

perm_files = sorted(Path("artifacts/tables/fact_perm").rglob("*.parquet"))[:3]
sample_perms = pd.concat(
    [pd.read_parquet(f, columns=["employer_id", "soc_code", "area_code", "wage_offer_from", "wage_offer_unit"])
     for f in perm_files], ignore_index=True
)
print("PERM area_code sample:", sample_perms["area_code"].dropna().unique()[:8])
print("PERM area_code null%:", round(100 * sample_perms["area_code"].isna().mean(), 1))
print("PERM wage_offer_from null%:", round(100 * sample_perms["wage_offer_from"].isna().mean(), 1))
print("PERM soc_code sample:", sample_perms["soc_code"].dropna().str[:7].unique()[:5])
print()

oews = pd.read_parquet(TABLES / "fact_oews/ref_year=2023/data.parquet", columns=["area_code", "soc_code", "a_median"])
oews_area = oews.dropna(subset=["a_median"])
perm_areas = set(sample_perms["area_code"].dropna().astype(str).unique())
oews_areas = set(oews_area["area_code"].dropna().astype(str).unique())
intersect = perm_areas & oews_areas
print(f"PERM unique area_codes: {len(perm_areas)}")
print(f"OEWS area_level area_codes: {len(oews_areas)}")
print(f"Intersection: {len(intersect)}")
print("PERM sample areas:", sorted(perm_areas)[:5])
print("OEWS sample areas:", sorted(oews_areas)[:5])
print()

# Check if existing employer_features already has wage_ratio
ef = pd.read_parquet(TABLES / "employer_features.parquet")
overall = ef[ef["scope"] == "overall"]
wr_null = overall["wage_ratio_med"].isna().mean()
print(f"employer_features overall rows: {len(overall):,}")
print(f"wage_ratio_med null%: {100*wr_null:.1f}%")
print(f"wage_ratio_med non-null sample: {overall['wage_ratio_med'].dropna().head(5).values}")
