#!/usr/bin/env python3
"""
FIX 2: dim_soc Expansion — full SOC-2018 from OEWS + crosswalk.
"""
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


def main():
    print("=" * 70)
    print("FIX 2: dim_soc EXPANSION (Full SOC-2018)")
    print("=" * 70)

    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    data_root = Path(paths["data_root"])
    artifacts_root = Path(paths["artifacts_root"])
    output_path = artifacts_root / "tables" / "dim_soc.parquet"
    metrics_dir = artifacts_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / "dim_soc_build.log"
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    ingested_at = datetime.now(timezone.utc)
    records = {}  # soc_code → record dict (dedup on code)

    # ── A.1) Extract from OEWS 2023 zip ─────────────────────────
    log("\n[A.1] Extracting SOC codes from OEWS 2023")
    oews_zip = data_root / "BLS_OEWS" / "2023" / "oews_all_data_2023.zip"
    if oews_zip.exists():
        with zipfile.ZipFile(oews_zip) as zf:
            xlsx = [n for n in zf.namelist() if n.endswith('.xlsx')]
            if xlsx:
                df_oews = pd.read_excel(zf.open(xlsx[0]))
                # Filter to detailed occupation codes (XX-XXXX format)
                mask = df_oews['OCC_CODE'].astype(str).str.match(r'^\d{2}-\d{4}$', na=False)
                df_detail = df_oews[mask][['OCC_CODE', 'OCC_TITLE']].drop_duplicates(subset=['OCC_CODE'])
                log(f"  OEWS 2023: {len(df_detail)} distinct detailed SOC codes")

                for _, row in df_detail.iterrows():
                    code = str(row['OCC_CODE']).strip()
                    title = str(row['OCC_TITLE']).strip()
                    records[code] = {
                        'soc_code': code,
                        'soc_title': title,
                        'soc_version': '2018',
                        'from_version': None,
                        'from_code': None,
                        'mapping_confidence': 'deterministic',
                        'is_aggregated': False,
                        'source_file': 'BLS_OEWS/2023/oews_all_data_2023.zip',
                    }
    else:
        log(f"  WARN: OEWS 2023 not found at {oews_zip}")

    # ── A.2) Cross-check/augment with crosswalk ─────────────────
    log("\n[A.2] Loading crosswalk soc_crosswalk_2010_to_2018.csv")
    crosswalk_path = data_root / "Codebooks" / "soc_crosswalk_2010_to_2018.csv"
    if crosswalk_path.exists():
        df_xw = pd.read_csv(crosswalk_path)
        log(f"  Crosswalk: {len(df_xw)} rows")
        for _, row in df_xw.iterrows():
            code_2018 = str(row.get('soc_2018_code', '')).strip()
            if not re.match(r'^\d{2}-\d{4}$', code_2018):
                continue
            title_2018 = str(row.get('soc_2018_title', '')).strip()
            code_2010 = str(row.get('soc_2010_code', '')).strip() if pd.notna(row.get('soc_2010_code')) else None
            notes = str(row.get('notes', '')).lower()

            # Determine confidence
            confidence = 'deterministic'
            is_agg = False
            if 'merged' in notes or 'consolidat' in notes:
                confidence = 'many-to-one'
                is_agg = True
            elif 'split' in notes:
                confidence = 'one-to-many'
            elif 'review' in notes or 'manual' in notes:
                confidence = 'manual-review'

            if code_2018 in records:
                # Augment with crosswalk provenance
                records[code_2018]['from_version'] = '2010'
                records[code_2018]['from_code'] = code_2010
                if confidence != 'deterministic':
                    records[code_2018]['mapping_confidence'] = confidence
                records[code_2018]['is_aggregated'] = is_agg
            else:
                records[code_2018] = {
                    'soc_code': code_2018,
                    'soc_title': title_2018 if title_2018 and title_2018 != 'nan' else f"SOC {code_2018} (Title Unknown)",
                    'soc_version': '2018',
                    'from_version': '2010',
                    'from_code': code_2010,
                    'mapping_confidence': confidence,
                    'is_aggregated': is_agg,
                    'source_file': 'Codebooks/soc_crosswalk_2010_to_2018.csv',
                }
    else:
        log(f"  WARN: Crosswalk not found at {crosswalk_path}")

    # ── B) Derive hierarchy ─────────────────────────────────────
    log("\n[B] Deriving hierarchy (major/minor/broad)")
    result = pd.DataFrame(records.values())
    result['soc_major_group'] = result['soc_code'].str[:2]
    result['soc_minor_group'] = result['soc_code'].str[:5]
    result['soc_broad_group'] = result['soc_code'].str[:6]
    result['ingested_at'] = ingested_at

    # Reorder per schema
    col_order = [
        'soc_code', 'soc_title', 'soc_version',
        'soc_major_group', 'soc_minor_group', 'soc_broad_group',
        'from_version', 'from_code', 'mapping_confidence',
        'is_aggregated', 'source_file', 'ingested_at',
    ]
    result = result[col_order].sort_values('soc_code').reset_index(drop=True)

    # ── C) Write & log ──────────────────────────────────────────
    log(f"\n[C] Writing dim_soc.parquet: {len(result)} rows")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False, engine='pyarrow')

    # Log major group distribution
    log("\n  Major group distribution:")
    for grp, cnt in result['soc_major_group'].value_counts().sort_index().items():
        log(f"    {grp}: {cnt}")
    log(f"\n  Total unique SOC-2018 codes: {len(result)}")

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    log(f"  Log: {log_path}")
    log("\n✓ FIX 2 COMPLETE")


if __name__ == "__main__":
    main()
