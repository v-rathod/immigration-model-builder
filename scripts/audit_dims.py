#!/usr/bin/env python3
"""
Quick audit of dimension tables to verify data quality.
"""

import sys
from pathlib import Path
import pandas as pd


def audit_dimensions():
    """Audit dim_country, dim_soc, and dim_area tables."""
    
    artifacts_dir = Path("artifacts/tables")
    
    # Define tables to audit
    tables = {
        "dim_country": artifacts_dir / "dim_country.parquet",
        "dim_soc": artifacts_dir / "dim_soc.parquet",
        "dim_area": artifacts_dir / "dim_area.parquet",
        "dim_visa_class": artifacts_dir / "dim_visa_class.parquet"
    }
    
    print("="*60)
    print("DIMENSION TABLE AUDIT")
    print("="*60)
    
    all_exist = True
    all_non_empty = True
    
    # Check existence
    for table_name, table_path in tables.items():
        if not table_path.exists():
            print(f"✗ MISSING: {table_name} at {table_path}")
            all_exist = False
        else:
            print(f"✓ Found: {table_name}")
    
    if not all_exist:
        print("\n❌ AUDIT FAILED: Some tables are missing")
        return 1
    
    print()
    
    # Audit each table
    issues = []
    
    # --- dim_country ---
    print("-" * 60)
    print("dim_country")
    print("-" * 60)
    df_country = pd.read_parquet(tables["dim_country"])
    print(f"Rows: {len(df_country)}")
    
    if len(df_country) == 0:
        issues.append("dim_country has zero rows")
        all_non_empty = False
    else:
        print(f"\nTop 5 rows:")
        print(df_country.head(5).to_string())
        
        # Check ISO codes uppercase
        if 'iso3' in df_country.columns:
            non_upper = df_country[df_country['iso3'] != df_country['iso3'].str.upper()]
            if len(non_upper) > 0:
                issues.append(f"dim_country: {len(non_upper)} iso3 codes not uppercase")
                print(f"\n⚠ WARNING: {len(non_upper)} iso3 codes not uppercase")
            
            # Check unique iso3
            if not df_country['iso3'].is_unique:
                dupes = df_country['iso3'].duplicated().sum()
                issues.append(f"dim_country: {dupes} duplicate iso3 codes")
                print(f"\n⚠ WARNING: {dupes} duplicate iso3 codes")
    
    print()
    
    # --- dim_soc ---
    print("-" * 60)
    print("dim_soc")
    print("-" * 60)
    df_soc = pd.read_parquet(tables["dim_soc"])
    print(f"Rows: {len(df_soc)}")
    
    if len(df_soc) == 0:
        issues.append("dim_soc has zero rows")
        all_non_empty = False
    else:
        print(f"\nTop 5 rows:")
        print(df_soc[['soc_code', 'soc_title', 'soc_major_group']].head(5).to_string())
        
        # Check soc_code format
        if 'soc_code' in df_soc.columns:
            import re
            pattern = re.compile(r'^\d{2}-\d{4}$')
            invalid_codes = df_soc[~df_soc['soc_code'].str.match(pattern, na=False)]
            if len(invalid_codes) > 0:
                issues.append(f"dim_soc: {len(invalid_codes)} soc_codes don't match pattern ^^\\d{{2}}-\\d{{4}}$")
                print(f"\n⚠ WARNING: {len(invalid_codes)} invalid soc_code formats")
                print(f"Examples: {invalid_codes['soc_code'].head(3).tolist()}")
            
            # Check unique PK
            if not df_soc['soc_code'].is_unique:
                dupes = df_soc['soc_code'].duplicated().sum()
                issues.append(f"dim_soc: {dupes} duplicate soc_codes")
                print(f"\n⚠ WARNING: {dupes} duplicate soc_codes")
        
        # Check titles present
        if 'soc_title' in df_soc.columns:
            missing_titles = df_soc['soc_title'].isna().sum()
            if missing_titles > 0:
                issues.append(f"dim_soc: {missing_titles} missing soc_titles")
                print(f"\n⚠ WARNING: {missing_titles} missing soc_titles")
        
        # Value counts for soc_major_group
        if 'soc_major_group' in df_soc.columns:
            print(f"\nsoc_major_group (top 10):")
            major_counts = df_soc['soc_major_group'].value_counts().head(10)
            for group, count in major_counts.items():
                print(f"  {group}: {count}")
    
    print()
    
    # --- dim_area ---
    print("-" * 60)
    print("dim_area")
    print("-" * 60)
    df_area = pd.read_parquet(tables["dim_area"])
    print(f"Rows: {len(df_area)}")
    
    if len(df_area) == 0:
        issues.append("dim_area has zero rows")
        all_non_empty = False
    else:
        print(f"\nTop 5 rows:")
        print(df_area[['area_code', 'area_title', 'area_type', 'metro_status']].head(5).to_string())
        
        # Check area_type enum
        if 'area_type' in df_area.columns:
            valid_types = {'NATIONAL', 'STATE', 'MSA', 'NONMSA', 'TERRITORY'}
            invalid_types = set(df_area['area_type'].dropna().unique()) - valid_types
            if invalid_types:
                issues.append(f"dim_area: Invalid area_types found: {invalid_types}")
                print(f"\n⚠ WARNING: Invalid area_types: {invalid_types}")
            
            # Value counts for area_type
            print(f"\narea_type (all):")
            type_counts = df_area['area_type'].value_counts()
            for area_type, count in type_counts.items():
                print(f"  {area_type}: {count}")
    
    print()
    
    # --- dim_visa_class ---
    print("-" * 60)
    print("dim_visa_class")
    print("-" * 60)
    df_visa_class = pd.read_parquet(tables["dim_visa_class"])
    print(f"Rows: {len(df_visa_class)}")
    
    if len(df_visa_class) == 0:
        issues.append("dim_visa_class has zero rows")
        all_non_empty = False
    else:
        print(f"\nTop 5 rows:")
        print(df_visa_class[['family_code', 'family_name', 'sub_code', 'sub_name']].head(5).to_string())
        
        # Check family_code enum
        if 'family_code' in df_visa_class.columns:
            valid_families = {'EB1', 'EB2', 'EB3', 'EB4', 'EB5'}
            invalid_families = set(df_visa_class['family_code'].dropna().unique()) - valid_families
            if invalid_families:
                issues.append(f"dim_visa_class: Invalid family_codes found: {invalid_families}")
                print(f"\n⚠ WARNING: Invalid family_codes: {invalid_families}")
            
            # Value counts for family_code
            print(f"\nfamily_code (all):")
            family_counts = df_visa_class['family_code'].value_counts()
            for family, count in family_counts.items():
                print(f"  {family}: {count}")
        
        # Check PK uniqueness
        df_visa_class['_pk_check'] = df_visa_class['family_code'] + '||' + df_visa_class['sub_code'].fillna('')
        if not df_visa_class['_pk_check'].is_unique:
            dupes = df_visa_class['_pk_check'].duplicated().sum()
            issues.append(f"dim_visa_class: {dupes} duplicate (family_code, sub_code) pairs")
            print(f"\n⚠ WARNING: {dupes} duplicate PKs")
    
    print()
    print("="*60)
    
    # Summary
    if issues:
        print("❌ AUDIT COMPLETED WITH ISSUES")
        print(f"\nFound {len(issues)} issue(s):")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
        return 1
    elif not all_non_empty:
        print("❌ AUDIT FAILED: Some tables are empty")
        return 1
    else:
        print("✅ AUDIT PASSED")
        print(f"\nAll dimension tables validated:")
        print(f"  - dim_country: {len(df_country)} rows")
        print(f"  - dim_soc: {len(df_soc)} rows")
        print(f"  - dim_area: {len(df_area)} rows")
        print(f"  - dim_visa_class: {len(df_visa_class)} rows")
        return 0


if __name__ == "__main__":
    sys.exit(audit_dimensions())
