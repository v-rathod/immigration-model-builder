#!/usr/bin/env python3
"""
Build approval_denial_trends.parquet - Comprehensive approval/denial metrics
across PERM, H-1B visa applications, and USCIS forms.

This artifact consolidates worldwide approval/denial data for the P3 wage/approval
dashboard, showing 10-year trends and approval rate percentages.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path("artifacts/tables")
OUTPUT_FILE = ARTIFACTS_DIR / "approval_denial_trends.parquet"


def build_perm_trends():
    """Aggregate PERM case outcomes by fiscal year."""
    logger.info("Loading PERM data...")
    try:
        perm = pd.read_parquet(ARTIFACTS_DIR / "fact_perm_all.parquet")
    except FileNotFoundError:
        perm = pd.read_parquet(ARTIFACTS_DIR / "fact_perm")
    
    # Normalize case_status to approved/denied
    # Note: WITHDRAWN cases are not approved/denied, but counted separately
    approved_statuses = {'CERTIFIED', 'CERTIFIED-EXPIRED', 'CERTIFIED - EXPIRED'}
    denied_statuses = {'DENIED'}
    
    perm['outcome'] = perm['case_status'].apply(
        lambda x: 'APPROVED' if x in approved_statuses
        else ('DENIED' if x in denied_statuses else 'OTHER')
    )
    
    # Aggregate by fiscal year and outcome
    perm_agg = perm.groupby(['fiscal_year', 'outcome']).size().reset_index(name='count')
    
    # Reshape to get approved/denied columns  
    pivot = perm_agg.pivot(index='fiscal_year', columns='outcome', values='count').fillna(0).astype(int)
    
    # Calculate totals (including OTHER/WITHDRAWN)
    pivot['total_cases'] = pivot.sum(axis=1)
    
    # For approval rate, calculate as APPROVED / (APPROVED + DENIED) to exclude WITHDRAWN
    if 'APPROVED' in pivot.columns and 'DENIED' in pivot.columns:
        decided_cases = pivot['APPROVED'] + pivot['DENIED']
        pivot['approval_rate_pct'] = (pivot['APPROVED'] / decided_cases * 100).round(2)
        pivot['denial_rate_pct'] = (pivot['DENIED'] / decided_cases * 100).round(2)
    else:
        pivot['approval_rate_pct'] = 0.0
        pivot['denial_rate_pct'] = 0.0
    
    result = pivot.reset_index()
    result['data_source'] = 'PERM_Labor_Certification'
    result['visa_category'] = 'Employment_Based_EB'
    
    logger.info(f"PERM: {len(result)} fiscal years, {result['total_cases'].sum():,} total cases (includes withdrawn)")
    logger.info(f"      Note: total_cases = APPROVED + DENIED + OTHER (WITHDRAWN). Approval/denial rates calculated on decided cases only.")
    return result


def build_visa_applications_trends():
    """Aggregate visa application refusals by fiscal year and visa class."""
    logger.info("Loading visa applications data...")
    va = pd.read_parquet(ARTIFACTS_DIR / "fact_visa_applications.parquet")
    
    # Clean fiscal year format (remove 'FY' prefix) — drop sentinel/unknown values
    va = va[va['fiscal_year'].notna() & ~va['fiscal_year'].astype(str).str.startswith('_')].copy()
    va['fy'] = va['fiscal_year'].str.replace('FY', '').astype(int)
    
    # Aggregate by fiscal year and visa class
    va_agg = va.groupby(['fy', 'visa_class']).agg({
        'applications': 'sum',
        'refusals': 'sum'
    }).reset_index()
    
    va_agg.rename(columns={'fy': 'fiscal_year'}, inplace=True)
    va_agg['issued'] = va_agg['applications'] - va_agg['refusals']
    va_agg['approval_rate_pct'] = (va_agg['issued'] / va_agg['applications'] * 100).round(2)
    va_agg['denial_rate_pct'] = (va_agg['refusals'] / va_agg['applications'] * 100).round(2)
    va_agg['data_source'] = 'Visa_Applications'
    va_agg.rename(columns={'visa_class': 'category'}, inplace=True)
    
    # Worldwide totals
    va_world = va.groupby('fy').agg({
        'applications': 'sum',
        'refusals': 'sum'
    }).reset_index()
    va_world.rename(columns={'fy': 'fiscal_year'}, inplace=True)
    va_world['issued'] = va_world['applications'] - va_world['refusals']
    va_world['APPROVED'] = va_world['issued']
    va_world['DENIED'] = va_world['refusals']
    va_world['total_cases'] = va_world['applications']
    va_world['approval_rate_pct'] = (va_world['issued'] / va_world['applications'] * 100).round(2)
    va_world['denial_rate_pct'] = (va_world['refusals'] / va_world['applications'] * 100).round(2)
    va_world['data_source'] = 'Visa_Applications'
    va_world['visa_category'] = 'Non_Immigrant_Visa'
    
    logger.info(f"Visa Applications: {len(va_world)} fiscal years, {va_world['total_cases'].sum():,} applications")
    return va_world[['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 'approval_rate_pct', 
                     'denial_rate_pct', 'data_source', 'visa_category']]


def build_uscis_approvals_trends():
    """Aggregate USCIS form approvals/denials by fiscal year."""
    logger.info("Loading USCIS approvals data...")
    ua = pd.read_parquet(ARTIFACTS_DIR / "fact_uscis_approvals.parquet")
    
    # Clean fiscal year — drop rows with sentinel/unknown values before conversion
    ua = ua[ua['fiscal_year'].notna()].copy()
    ua['fiscal_year'] = ua['fiscal_year'].astype(str)
    ua = ua[~ua['fiscal_year'].str.startswith('_')].copy()
    ua['fy'] = ua['fiscal_year'].str.replace('FY', '').apply(lambda x: int(x) if x.isdigit() else None)
    ua = ua[ua['fy'].notna()].copy()
    ua['fy'] = ua['fy'].astype(int)
    
    # Aggregate by fiscal year
    ua_agg = ua.groupby('fy').agg({
        'approvals': 'sum',
        'denials': 'sum'
    }).reset_index()
    
    ua_agg.rename(columns={'fy': 'fiscal_year'}, inplace=True)
    ua_agg['APPROVED'] = ua_agg['approvals']
    ua_agg['DENIED'] = ua_agg['denials']
    ua_agg['total_cases'] = ua_agg['approvals'] + ua_agg['denials']
    ua_agg['approval_rate_pct'] = (ua_agg['approvals'] / ua_agg['total_cases'] * 100).round(2)
    ua_agg['denial_rate_pct'] = (ua_agg['denials'] / ua_agg['total_cases'] * 100).round(2)
    ua_agg['data_source'] = 'USCIS_Forms'
    ua_agg['visa_category'] = 'USCIS_Adjustment'
    
    logger.info(f"USCIS: {len(ua_agg)} fiscal years, {ua_agg['total_cases'].sum():,} decisions")
    return ua_agg[['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 'approval_rate_pct',
                   'denial_rate_pct', 'data_source', 'visa_category']]


def build_niv_issuance_trends():
    """Extract visa issuance data to complement approval trends."""
    logger.info("Loading NIV issuance data...")
    try:
        niv = pd.read_parquet(ARTIFACTS_DIR / "fact_niv_issuance.parquet")
        
        # Clean fiscal year (remove 'FY' prefix) — drop sentinel/unknown values
        niv = niv[niv['fiscal_year'].notna() & ~niv['fiscal_year'].astype(str).str.startswith('_')].copy()
        niv['fy'] = niv['fiscal_year'].str.replace('FY', '').astype(int)
        
        # Aggregate by fiscal year
        niv_agg = niv.groupby('fy').agg({
            'issued': 'sum'
        }).reset_index()
        
        niv_agg.rename(columns={'fy': 'fiscal_year'}, inplace=True)
        niv_agg['APPROVED'] = niv_agg['issued']
        niv_agg['DENIED'] = 0  # Not available in this dataset
        niv_agg['total_cases'] = niv_agg['issued']
        niv_agg['approval_rate_pct'] = 100.0  # All issued (denials not tracked)
        niv_agg['denial_rate_pct'] = 0.0
        niv_agg['data_source'] = 'NIV_Issuance'
        niv_agg['visa_category'] = 'Non_Immigrant_Visa'
        
        logger.info(f"NIV Issuance: {len(niv_agg)} fiscal years, {niv_agg['total_cases'].sum():,} visas issued")
        return niv_agg[['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 'approval_rate_pct',
                       'denial_rate_pct', 'data_source', 'visa_category']]
    except FileNotFoundError:
        logger.warning("NIV issuance file not found, skipping")
        return pd.DataFrame()


def merge_and_summarize(perm_trends, visa_trends, uscis_trends, niv_trends):
    """Merge all trends and create summary statistics."""
    
    # Ensure fiscal_year is numeric for all dataframes
    for df in [perm_trends, visa_trends, uscis_trends, niv_trends]:
        if len(df) > 0:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce').astype('Int64')
    
    # Filter last 10 fiscal years from PERM (most comprehensive)
    if len(perm_trends) > 0:
        max_year = perm_trends['fiscal_year'].max()
        recent_years = max_year - 9
        perm_recent = perm_trends[perm_trends['fiscal_year'] >= recent_years].copy()
    else:
        perm_recent = perm_trends.copy()
    
    logger.info(f"\n{'='*70}")
    logger.info("APPROVAL/DENIAL TRENDS SUMMARY (Last 10 Fiscal Years)")
    logger.info(f"{'='*70}")
    
    # PERM Summary
    if len(perm_recent) > 0:
        print("\n📊 PERM Labor Certification:")
        for _, row in perm_recent.iterrows():
            fy = int(row['fiscal_year'])
            approved = int(row.get('APPROVED', 0))
            denied = int(row.get('DENIED', 0))
            total = int(row['total_cases'])
            rate = row.get('approval_rate_pct', 0)
            print(f"  FY{fy}: {approved:,} approved, {denied:,} denied (Total: {total:,}) - {rate:.1f}% approval")
    
    # Visa Applications Summary
    if len(visa_trends) > 0:
        print("\n📇 Visa Applications (Worldwide):")
        for _, row in visa_trends.iterrows():
            fy = int(row['fiscal_year'])
            issued = int(row.get('APPROVED', 0))
            refused = int(row.get('DENIED', 0))
            total = int(row['total_cases'])
            rate = row.get('approval_rate_pct', 0)
            print(f"  FY{fy}: {issued:,} issued, {refused:,} refused (Total: {total:,}) - {rate:.1f}% approval")
    
    # USCIS Summary
    if len(uscis_trends) > 0:
        print("\n✅ USCIS Form Decisions:")
        for _, row in uscis_trends.iterrows():
            fy = int(row['fiscal_year'])
            approved = int(row.get('APPROVED', 0))
            denied = int(row.get('DENIED', 0))
            total = int(row['total_cases'])
            rate = row.get('approval_rate_pct', 0)
            print(f"  FY{fy}: {approved:,} approved, {denied:,} denied (Total: {total:,}) - {rate:.1f}% approval")
    
    # Combine all sources
    all_trends = pd.concat([perm_trends, visa_trends, uscis_trends], 
                           ignore_index=True, sort=False)
    
    logger.info(f"\n✓ Combined {len(all_trends)} rows from all sources")
    
    return all_trends


def main():
    logger.info("Building approval_denial_trends.parquet...")
    
    # Build trends from each source
    perm_trends = build_perm_trends()
    visa_trends = build_visa_applications_trends()
    uscis_trends = build_uscis_approvals_trends()
    niv_trends = build_niv_issuance_trends()
    
    # Merge and summarize
    combined = merge_and_summarize(perm_trends, visa_trends, uscis_trends, niv_trends)
    
    # Select relevant columns
    output_cols = ['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 
                   'approval_rate_pct', 'denial_rate_pct', 'data_source', 'visa_category']
    combined = combined[[c for c in output_cols if c in combined.columns]].copy()
    
    # Sort by fiscal year and data source
    combined = combined.sort_values(['fiscal_year', 'data_source']).reset_index(drop=True)
    
    # Write output
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    logger.info(f"\n✓ Wrote {len(combined)} rows to {OUTPUT_FILE}")
    logger.info(f"  Columns: {list(combined.columns)}")
    logger.info(f"  Fiscal years covered: {combined['fiscal_year'].min()} to {combined['fiscal_year'].max()}")
    logger.info(f"  Data sources: {combined['data_source'].unique().tolist()}")


if __name__ == "__main__":
    main()
