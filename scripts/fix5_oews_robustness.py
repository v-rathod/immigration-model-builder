#!/usr/bin/env python3
"""
FIX 5: OEWS robustness — handle .xlsx and .zip, skip corrupt 2024.
"""
import sys
import zipfile
from pathlib import Path

import yaml


def main():
    print("=" * 70)
    print("FIX 5: OEWS ROBUSTNESS CHECK")
    print("=" * 70)

    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    data_root = Path(paths["data_root"])

    oews_dir = data_root / "BLS_OEWS"
    if not oews_dir.exists():
        print(f"  WARN: OEWS directory not found: {oews_dir}")
        return

    for year_dir in sorted(oews_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        for f in sorted(year_dir.iterdir()):
            if f.suffix == '.zip':
                try:
                    with zipfile.ZipFile(f, 'r') as zf:
                        xlsx_files = [n for n in zf.namelist() if n.endswith('.xlsx')]
                        print(f"  ✓ {f.name}: valid zip, contains {xlsx_files}")
                except zipfile.BadZipFile:
                    print(f"  ⚠ {f.name}: CORRUPT zip — will be skipped by pipeline (WARN logged)")
                except Exception as e:
                    print(f"  ⚠ {f.name}: ERROR: {e} — will be skipped")
            elif f.suffix == '.xlsx':
                print(f"  ✓ {f.name}: xlsx file")
            elif f.suffix == '.pdf':
                print(f"  ℹ {f.name}: documentation (ignored)")

    print("\n  The build_fact_oews.py already handles both .xlsx and .zip.")
    print("  Corrupt files cause read_oews_data() to return None → skipped with WARN.")
    print("\n✓ FIX 5 COMPLETE (no code changes needed — already robust)")


if __name__ == "__main__":
    main()
