#!/usr/bin/env python3
"""Quick inventory of all artifacts/tables/ with row counts and columns."""
import pandas as pd
import pathlib

TABLES = pathlib.Path("artifacts/tables")
inventory = {}

for f in sorted(TABLES.glob("*.parquet")):
    try:
        df = pd.read_parquet(f)
        inventory[f.name] = {"rows": len(df), "cols": list(df.columns)}
    except Exception as e:
        inventory[f.name] = {"rows": -1, "error": str(e)[:80]}

for d in sorted(TABLES.iterdir()):
    if d.is_dir():
        parts = list(d.rglob("*.parquet"))
        if parts:
            try:
                cols = list(pd.read_parquet(parts[0]).columns)
                total = sum(len(pd.read_parquet(p)) for p in parts)
                inventory[d.name + "/"] = {"rows": total, "cols": cols, "partitions": len(parts)}
            except Exception as e:
                inventory[d.name + "/"] = {"rows": -1, "error": str(e)[:80], "partitions": len(parts)}

print(f"{'Name':45s} {'Rows':>10s}  {'Cols':>4s}  Extra")
print("-" * 80)
for name, info in sorted(inventory.items()):
    r = info.get("rows", "?")
    c = len(info.get("cols", [])) if "cols" in info else "?"
    err = info.get("error", "")
    p = info.get("partitions", "")
    extra = f"ERR: {err}" if err else (f"{p} parts" if p else "")
    print(f"{name:45s} {r:>10}  {c:>4}  {extra}")
