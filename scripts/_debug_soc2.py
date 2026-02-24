"""Debug SOC coverage issues in worksite_geo."""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"

wg = pd.read_parquet(TABLES / "worksite_geo_metrics.parquet")
dim_soc = pd.read_parquet(TABLES / "dim_soc.parquet")

# Load OEWS soc codes for comparison
oews_dir = TABLES / "fact_oews"
oews_dfs = [pd.read_parquet(f, columns=["soc_code"]) for f in oews_dir.rglob("*.parquet")]
oews = pd.concat(oews_dfs)
oews_soc_set = set(oews["soc_code"].dropna().unique())
dim_soc_set = set(dim_soc["soc_code"].dropna().unique())

print(f"dim_soc unique codes: {len(dim_soc_set)}")
print(f"oews unique soc codes: {len(oews_soc_set)}")

# Worksite SOC analysis
wg_soc = wg["soc_code"].dropna()
wg_unique = set(wg_soc.unique())
print(f"\nworksite_geo unique SOC codes: {len(wg_unique)}")
print(f"worksite_geo non-null rows: {len(wg_soc):,}")

# Coverage against different reference sets
in_dim_soc = wg_soc.isin(dim_soc_set).sum()
in_oews = wg_soc.isin(oews_soc_set).sum()
print(f"\nCoverage vs dim_soc: {in_dim_soc/len(wg_soc)*100:.2f}% ({in_dim_soc:,}/{len(wg_soc):,})")
print(f"Coverage vs oews SOCs: {in_oews/len(wg_soc)*100:.2f}% ({in_oews:,}/{len(wg_soc):,})")

# Unmapped SOC analysis
unmapped_soc = wg_soc[~wg_soc.isin(dim_soc_set)]
unmapped_unique = unmapped_soc.unique()
print(f"\nUnmapped unique SOC codes: {len(unmapped_unique)}")

# Classify them
blank_codes = [c for c in unmapped_unique if str(c).strip() == ""]
numeric_only = [c for c in unmapped_unique if str(c).strip().replace(".","").isnumeric()]
formatted = [c for c in unmapped_unique if "-" in str(c)]
other = [c for c in unmapped_unique if c not in blank_codes and c not in numeric_only and c not in formatted]

print(f"  blank: {len(blank_codes)}")
print(f"  numeric only (malformed): {len(numeric_only)} - sample: {numeric_only[:10]}")
print(f"  XX-XXXX formatted (old SOC versions?): {len(formatted)} - sample: {formatted[:10]}")
print(f"  other: {len(other)} - sample: {other[:10]}")

# How many rows would be covered if we also include OEWS codes?
union_ref = dim_soc_set | oews_soc_set
in_union = wg_soc.isin(union_ref).sum()
print(f"\nCoverage vs dim_soc + oews union: {in_union/len(wg_soc)*100:.2f}%")

# How many rows are blank/malformed?
blank_rows = unmapped_soc.isin(blank_codes).sum() if blank_codes else 0
malformed_rows = unmapped_soc.isin(numeric_only).sum() if numeric_only else 0
blank_or_malformed = blank_rows + malformed_rows

# Actually the right check: just count non-null, non-blank, non-numeric-only rows
import re
valid_format = wg_soc.apply(lambda x: bool(re.match(r'^\d{2}-\d{4}$', str(x).strip())))
print(f"\nRows with valid XX-XXXX format SOC: {valid_format.sum():,} ({valid_format.mean()*100:.2f}%)")
valid_and_mapped = wg_soc[valid_format].isin(dim_soc_set)
print(f"Of valid format, in dim_soc: {valid_and_mapped.sum():,}/{valid_format.sum():,} = {valid_and_mapped.mean()*100:.2f}%")
valid_and_in_oews = wg_soc[valid_format].isin(oews_soc_set)
print(f"Of valid format, in oews: {valid_and_in_oews.sum():,}/{valid_format.sum():,} = {valid_and_in_oews.mean()*100:.2f}%")

# Check if the old XX-XXXX codes that are in OEWS but not dim_soc would fill the gap
formatted_unmapped = [c for c in formatted if c not in dim_soc_set]
in_oews_not_dim = [c for c in formatted_unmapped if c in oews_soc_set]
print(f"\nFormatted unmapped codes in OEWS but not dim_soc: {len(in_oews_not_dim)} - sample: {in_oews_not_dim[:10]}")
formatted_in_neither = [c for c in formatted_unmapped if c not in oews_soc_set]
print(f"Formatted codes in neither dim_soc nor OEWS: {len(formatted_in_neither)} - sample: {formatted_in_neither[:10]}")
