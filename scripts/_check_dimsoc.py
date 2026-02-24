"""Check dim_soc and crosswalk files."""
import pandas as pd
from pathlib import Path
import os

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"

dim_soc = pd.read_parquet(TABLES / "dim_soc.parquet")
print("dim_soc cols:", list(dim_soc.columns))
print("dim_soc sample:\n", dim_soc.head(3).to_string())
print(f"\ndim_soc rows: {len(dim_soc)}")

# Check if crosswalk exists
import yaml
try:
    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    data_root = Path(paths["data_root"])
    crosswalk = data_root / "Codebooks" / "soc_crosswalk_2010_to_2018.csv"
    print(f"\nCrosswalk path: {crosswalk}")
    print(f"Exists: {crosswalk.exists()}")
    if crosswalk.exists():
        df_xw = pd.read_csv(crosswalk)
        print(f"Crosswalk cols: {list(df_xw.columns)}")
        print(f"Crosswalk rows: {len(df_xw)}")
        print(df_xw.head(3).to_string())
    
    # Check what else is in the Codebooks dir
    codebooks = data_root / "Codebooks"
    if codebooks.exists():
        print(f"\nCodebooks files: {[f.name for f in codebooks.iterdir()][:20]}")
    
    # Check BLS OEWS data
    oews_dir = data_root / "BLS_OEWS"
    if oews_dir.exists():
        print(f"\nOEWS years: {[d.name for d in oews_dir.iterdir() if d.is_dir()]}")
except Exception as e:
    print(f"Error: {e}")
