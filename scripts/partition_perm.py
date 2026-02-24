#!/usr/bin/env python3
"""Convert single fact_perm.parquet to partitioned format by fiscal year."""

import pandas as pd
from pathlib import Path

def main():
    # Read the single fact_perm file
    print("Reading fact_perm.parquet...")
    df = pd.read_parquet('artifacts/tables/fact_perm.parquet')
    print(f"Total rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")

    # Check if fy column exists
    if 'fy' not in df.columns:
        print("ERROR: 'fy' column not found in dataframe")
        return 1

    print(f"\nFiscal years: {sorted(df['fy'].dropna().unique())}")
    
    # Rename fy to fiscal_year for clarity
    df = df.rename(columns={'fy': 'fiscal_year'})
    
    # Create partitioned output directory
    output_dir = Path('artifacts/tables/fact_perm_partitioned')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write partitioned by fiscal_year
    print(f"\nWriting partitioned data to {output_dir}/...")
    df.to_parquet(
        output_dir,
        partition_cols=['fiscal_year'],
        engine='pyarrow',
        index=False
    )
    
    print("✅ Partitioned PERM data written successfully")
    
    # Verify partitions
    partitions = sorted([d.name for d in output_dir.iterdir() if d.is_dir()])
    print(f"\nCreated {len(partitions)} partitions")
    print(f"Partitions: {partitions}")
    
    # Move old file and rename new directory
    import shutil
    old_file = Path('artifacts/tables/fact_perm.parquet')
    backup_file = Path('artifacts/tables/fact_perm_single_file_backup.parquet')
    
    print(f"\nBacking up old file to {backup_file}")
    shutil.move(str(old_file), str(backup_file))
    
    print(f"Renaming {output_dir} to artifacts/tables/fact_perm")
    final_dir = Path('artifacts/tables/fact_perm')
    if final_dir.exists():
        shutil.rmtree(final_dir)
    shutil.move(str(output_dir), str(final_dir))
    
    print("✅ Migration complete!")
    print(f"   - Old file backed up: {backup_file}")
    print(f"   - New partitioned directory: {final_dir}")
    
    return 0

if __name__ == '__main__':
    exit(main())
