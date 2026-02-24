#!/usr/bin/env python3
"""
Quick audit of all dimension tables to confirm correctness.
"""

import sys
import re
from pathlib import Path
import pandas as pd


def audit_dims():
    """Audit all dimension tables and validate invariants."""
    
    artifacts_dir = Path("artifacts/tables")
    
    print("=" * 70)
    print("DIMENSION AUDIT - QUICK CHECK")
    print("=" * 70)
    print()
    
    all_pass = True
    issues = []
    
    # --- dim_country ---
    print("-" * 70)
    print("dim_country")
    print("-" * 70)
    
    country_path = artifacts_dir / "dim_country.parquet"
    if not country_path.exists():
        print("❌ File not found")
        issues.append("dim_country: file missing")
        all_pass = False
    else:
        df = pd.read_parquet(country_path)
        print(f"Rows: {len(df)}")
        print(f"\nTop 3:")
        print(df[['country_name', 'iso2', 'iso3']].head(3).to_string())
        
        # Validate unique iso3
        if not df['iso3'].is_unique:
            dupes = df['iso3'].duplicated().sum()
            issues.append(f"dim_country: {dupes} duplicate iso3 values")
            print(f"\n❌ FAIL: {dupes} duplicate iso3 values")
            all_pass = False
        
        # Validate uppercase iso2/iso3
        non_upper_iso2 = df[df['iso2'] != df['iso2'].str.upper()]
        non_upper_iso3 = df[df['iso3'] != df['iso3'].str.upper()]
        if len(non_upper_iso2) > 0 or len(non_upper_iso3) > 0:
            issues.append(f"dim_country: {len(non_upper_iso2)} non-uppercase iso2, {len(non_upper_iso3)} non-uppercase iso3")
            print(f"\n❌ FAIL: ISO codes not uppercase")
            all_pass = False
        
        if not issues or 'dim_country' not in str(issues):
            print("\n✓ PASS")
    
    print()
    
    # --- dim_soc ---
    print("-" * 70)
    print("dim_soc")
    print("-" * 70)
    
    soc_path = artifacts_dir / "dim_soc.parquet"
    if not soc_path.exists():
        print("❌ File not found")
        issues.append("dim_soc: file missing")
        all_pass = False
    else:
        df = pd.read_parquet(soc_path)
        print(f"Rows: {len(df)}")
        print(f"\nTop 3:")
        print(df[['soc_code', 'soc_title']].head(3).to_string())
        
        # Validate soc_code format
        pattern = re.compile(r'^\d{2}-\d{4}$')
        invalid = df[~df['soc_code'].str.match(pattern, na=False)]
        if len(invalid) > 0:
            issues.append(f"dim_soc: {len(invalid)} invalid soc_code formats")
            print(f"\n❌ FAIL: {len(invalid)} soc_codes don't match pattern")
            print(f"Examples: {invalid['soc_code'].head(3).tolist()}")
            all_pass = False
        
        # Validate unique soc_code
        if not df['soc_code'].is_unique:
            dupes = df['soc_code'].duplicated().sum()
            issues.append(f"dim_soc: {dupes} duplicate soc_codes")
            print(f"\n❌ FAIL: {dupes} duplicate soc_codes")
            all_pass = False
        
        if len(invalid) == 0 and df['soc_code'].is_unique:
            print("\n✓ PASS")
    
    print()
    
    # --- dim_area ---
    print("-" * 70)
    print("dim_area")
    print("-" * 70)
    
    area_path = artifacts_dir / "dim_area.parquet"
    if not area_path.exists():
        print("❌ File not found")
        issues.append("dim_area: file missing")
        all_pass = False
    else:
        df = pd.read_parquet(area_path)
        print(f"Rows: {len(df)}")
        print(f"\nTop 3:")
        print(df[['area_code', 'area_title', 'area_type']].head(3).to_string())
        
        # Validate area_type enum
        valid_types = {'NATIONAL', 'STATE', 'MSA', 'NONMSA', 'TERRITORY'}
        invalid_types = set(df['area_type'].dropna().unique()) - valid_types
        if invalid_types:
            issues.append(f"dim_area: invalid area_types {invalid_types}")
            print(f"\n❌ FAIL: Invalid area_types: {invalid_types}")
            all_pass = False
        else:
            print(f"\narea_type distribution: {dict(df['area_type'].value_counts())}")
            print("\n✓ PASS")
    
    print()
    
    # --- dim_visa_class ---
    print("-" * 70)
    print("dim_visa_class")
    print("-" * 70)
    
    visa_path = artifacts_dir / "dim_visa_class.parquet"
    if not visa_path.exists():
        print("❌ File not found")
        issues.append("dim_visa_class: file missing")
        all_pass = False
    else:
        df = pd.read_parquet(visa_path)
        print(f"Rows: {len(df)}")
        print(f"\nTop 3:")
        print(df[['family_code', 'family_name', 'sub_code']].head(3).to_string())
        
        # Validate family_code enum
        valid_families = {'EB1', 'EB2', 'EB3', 'EB4', 'EB5'}
        invalid_families = set(df['family_code'].dropna().unique()) - valid_families
        if invalid_families:
            issues.append(f"dim_visa_class: invalid family_codes {invalid_families}")
            print(f"\n❌ FAIL: Invalid family_codes: {invalid_families}")
            all_pass = False
        else:
            print(f"\nfamily_code distribution: {dict(df['family_code'].value_counts())}")
            print("\n✓ PASS")
    
    print()
    print("=" * 70)
    
    # Summary
    if all_pass and not issues:
        print("✅ ALL DIMENSION TABLES VALIDATED")
        print("\nSummary:")
        print(f"  ✓ dim_country: iso3 unique, ISO codes uppercase")
        print(f"  ✓ dim_soc: soc_code format correct, unique")
        print(f"  ✓ dim_area: area_type enums valid")
        print(f"  ✓ dim_visa_class: family_code enums valid")
        return 0
    else:
        print(f"❌ AUDIT FAILED - {len(issues)} issue(s)")
        print("\nIssues:")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
        return 1


if __name__ == "__main__":
    sys.exit(audit_dims())
