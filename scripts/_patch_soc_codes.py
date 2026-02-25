"""Patch fact_perm soc_code in-place using expanded dim_soc + soc_code_raw."""
import re
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

# Load updated dim_soc
dim_soc = pd.read_parquet(TABLES / "dim_soc.parquet")
soc_valid = set(dim_soc["soc_code"].values)
print(f"dim_soc: {len(soc_valid)} codes")

# Process each fact_perm partition
fact_perm_dir = TABLES / "fact_perm"
total_patched = 0
total_rows = 0

for fy_dir in sorted(fact_perm_dir.iterdir()):
    if not fy_dir.is_dir():
        continue
    pf = fy_dir / "part-0.parquet"
    if not pf.exists():
        continue
    
    df = pd.read_parquet(pf)
    total_rows += len(df)
    
    if "soc_code_raw" not in df.columns:
        print(f"  {fy_dir.name}: no soc_code_raw column, skipping")
        continue
    
    # Find rows where soc_code is null/None/"None" but soc_code_raw has a value
    null_soc = (df["soc_code"].isna()) | (df["soc_code"].astype(str).isin(["None", "nan", ""]))
    has_raw = df["soc_code_raw"].notna() & (~df["soc_code_raw"].astype(str).isin(["None", "nan", ""]))
    patchable = null_soc & has_raw
    
    if patchable.sum() == 0:
        print(f"  {fy_dir.name}: {len(df):>7,} rows, 0 patchable")
        continue
    
    # Map soc_code_raw to dim_soc
    def map_soc(raw):
        if pd.isna(raw) or not isinstance(raw, str):
            return None
        s = str(raw).strip()
        s = re.sub(r'\.\d+$', '', s)
        if s in soc_valid:
            return s
        if '-' not in s and len(s) >= 6:
            norm = f"{s[:2]}-{s[2:]}"
            if norm in soc_valid:
                return norm
        return None
    
    new_soc = df.loc[patchable, "soc_code_raw"].apply(map_soc)
    patched = new_soc.notna().sum()
    
    if patched > 0:
        df.loc[patchable & new_soc.notna(), "soc_code"] = new_soc[new_soc.notna()]
        df.to_parquet(pf, index=False, engine="pyarrow")
        total_patched += patched
    
    print(f"  {fy_dir.name}: {len(df):>7,} rows, {patchable.sum():>6,} null soc_code, {patched:>6,} patched")

print(f"\nTotal: {total_rows:,} rows, {total_patched:,} soc_code patched")

# Verify final coverage
all_dfs = []
for fy_dir in sorted(fact_perm_dir.iterdir()):
    if not fy_dir.is_dir():
        continue
    pf = fy_dir / "part-0.parquet"
    if pf.exists():
        df = pd.read_parquet(pf, columns=["soc_code", "soc_code_raw"])
        all_dfs.append(df)

total = pd.concat(all_dfs, ignore_index=True)
soc_notna = total["soc_code"].notna() & (~total["soc_code"].astype(str).isin(["None", "nan", ""]))
raw_notna = total["soc_code_raw"].notna() & (~total["soc_code_raw"].astype(str).isin(["None", "nan", ""]))
print(f"\nFinal coverage:")
print(f"  soc_code:     {soc_notna.sum():>10,} / {len(total):,} ({soc_notna.mean()*100:.1f}%)")
print(f"  soc_code_raw: {raw_notna.sum():>10,} / {len(total):,} ({raw_notna.mean()*100:.1f}%)")
