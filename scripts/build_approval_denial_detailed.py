#!/usr/bin/env python3
"""
Build approval_denial_detailed.parquet - Approval/denial breakdown by 
visa class and country for P3 drill-down analysis.

Provides granular data for P3 dashboard charts showing approval rates by:
- Visa class (H-1B, EB-1, EB-2, etc.)
- Country (top 20 countries)
- Fiscal year and category
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path("artifacts/tables")
OUTPUT_FILE = ARTIFACTS_DIR / "approval_denial_detailed.parquet"


def build_perm_by_country():
    """Build PERM approval/denial breakdown by country."""
    logger.info("Building PERM by country...")
    
    try:
        perm = pd.read_parquet(ARTIFACTS_DIR / "fact_perm_all.parquet")
    except FileNotFoundError:
        perm = pd.read_parquet(ARTIFACTS_DIR / "fact_perm")
    
    # Normalize case_status
    approved_statuses = {'CERTIFIED', 'CERTIFIED-EXPIRED', 'CERTIFIED - EXPIRED'}
    perm['is_approved'] = perm['case_status'].isin(approved_statuses).astype(int)
    
    # Filter to valid countries
    perm = perm[perm['employer_country'].notna()].copy()
    
    # Aggregate by fiscal year and country
    by_country = perm.groupby(['fiscal_year', 'employer_country']).agg({
        'is_approved': 'sum',
        'case_number': 'count'
    }).reset_index()
    
    by_country.rename(columns={
        'is_approved': 'approved_count',
        'case_number': 'total_count'
    }, inplace=True)
    
    by_country['denied_count'] = by_country['total_count'] - by_country['approved_count']
    by_country['approval_rate_pct'] = (by_country['approved_count'] / by_country['total_count'] * 100).round(2)
    by_country['denial_rate_pct'] = (by_country['denied_count'] / by_country['total_count'] * 100).round(2)
    by_country['data_source'] = 'PERM_Labor_Certification'
    by_country['category'] = 'Employment_Based'
    
    logger.info(f"  Countries: {by_country['employer_country'].nunique()}")
    logger.info(f"  Total rows: {len(by_country):,}")
    
    return by_country


def build_lca_by_visa_class():
    """Build H1B/LCA approval breakdown by visa class."""
    logger.info("Building LCA/H1B by visa class...")
    
    try:
        lca = pd.read_parquet(ARTIFACTS_DIR / "fact_lca.parquet")
    except FileNotFoundError:
        logger.warning("  fact_lca not found, skipping")
        return pd.DataFrame()
    
    # Normalize case_status
    approved_statuses = {'APPROVED', 'CERTIFIED', 'CERTIFIED-EXPIRED'}
    lca['is_approved'] = lca['case_status'].isin(approved_statuses).astype(int)
    
    # Filter to valid visa classes
    lca = lca[lca['visa_class'].notna()].copy()
    
    # Aggregate by fiscal year and visa class
    by_visa = lca.groupby(['fiscal_year', 'visa_class']).agg({
        'is_approved': 'sum',
        'case_number': 'count'
    }).reset_index()
    
    by_visa.rename(columns={
        'is_approved': 'approved_count',
        'case_number': 'total_count'
    }, inplace=True)
    
    by_visa['denied_count'] = by_visa['total_count'] - by_visa['approved_count']
    by_visa['approval_rate_pct'] = (by_visa['approved_count'] / by_visa['total_count'] * 100).round(2)
    by_visa['denial_rate_pct'] = (by_visa['denied_count'] / by_visa['total_count'] * 100).round(2)
    by_visa['data_source'] = 'H1B_LCA'
    by_visa['category'] = 'Non_Immigrant'
    by_visa.rename(columns={'visa_class': 'employer_country'}, inplace=True)
    
    logger.info(f"  Visa classes: {by_visa['employer_country'].nunique()}")
    logger.info(f"  Total rows: {len(by_visa):,}")
    
    return by_visa


def build_visa_apps_by_class():
    """Build visa application approval by visa class."""
    logger.info("Building visa applications by class...")
    
    try:
        va = pd.read_parquet(ARTIFACTS_DIR / "fact_visa_applications.parquet")
    except FileNotFoundError:
        logger.warning("  fact_visa_applications not found, skipping")
        return pd.DataFrame()
    
    # Clean fiscal year
    va['fy'] = va['fiscal_year'].str.replace('FY', '').astype(int)
    
    # Group by visa class
    by_class = va.groupby(['fy', 'visa_class']).agg({
        'applications': 'sum',
        'refusals': 'sum'
    }).reset_index()
    
    by_class.rename(columns={'fy': 'fiscal_year'}, inplace=True)
    by_class['approved_count'] = by_class['applications'] - by_class['refusals']
    by_class['denied_count'] = by_class['refusals']
    by_class['total_count'] = by_class['applications']
    by_class['approval_rate_pct'] = (by_class['approved_count'] / by_class['total_count'] * 100).round(2)
    by_class['denial_rate_pct'] = (by_class['denied_count'] / by_class['total_count'] * 100).round(2)
    by_class['data_source'] = 'Visa_Applications'
    by_class['category'] = 'Non_Immigrant_Visa'
    by_class.rename(columns={'visa_class': 'employer_country'}, inplace=True)
    
    # Select relevant columns
    by_class = by_class[['fiscal_year', 'employer_country', 'approved_count', 'denied_count', 
                         'total_count', 'approval_rate_pct', 'denial_rate_pct', 'data_source', 'category']]
    
    logger.info(f"  Visa classes: {by_class['employer_country'].nunique()}")
    logger.info(f"  Total rows: {len(by_class):,}")
    
    return by_class


def build_uscis_by_form_type():
    """Build USCIS approval by form type."""
    logger.info("Building USCIS by form type...")
    
    try:
        ua = pd.read_parquet(ARTIFACTS_DIR / "fact_uscis_approvals.parquet")
    except FileNotFoundError:
        logger.warning("  fact_uscis_approvals not found, skipping")
        return pd.DataFrame()
    
    # Clean fiscal year
    ua['fy'] = ua['fiscal_year'].str.replace('FY', '').astype(int)
    
    # Aggregate by form and fiscal year
    by_form = ua.groupby(['fy', 'form']).agg({
        'approvals': 'sum',
        'denials': 'sum'
    }).reset_index()
    
    by_form.rename(columns={'fy': 'fiscal_year'}, inplace=True)
    by_form['approved_count'] = by_form['approvals']
    by_form['denied_count'] = by_form['denials']
    by_form['total_count'] = by_form['approvals'] + by_form['denials']
    by_form['approval_rate_pct'] = (by_form['approved_count'] / by_form['total_count'] * 100).round(2)
    by_form['denial_rate_pct'] = (by_form['denied_count'] / by_form['total_count'] * 100).round(2)
    by_form['data_source'] = 'USCIS_Forms'
    by_form['category'] = 'USCIS_Adjustment'
    by_form.rename(columns={'form': 'employer_country'}, inplace=True)
    
    # Select relevant columns
    by_form = by_form[['fiscal_year', 'employer_country', 'approved_count', 'denied_count',
                       'total_count', 'approval_rate_pct', 'denial_rate_pct', 'data_source', 'category']]
    
    logger.info(f"  Form types: {by_form['employer_country'].nunique()}")
    logger.info(f"  Total rows: {len(by_form):,}")
    
    return by_form


def main():
    logger.info("Building approval_denial_detailed.parquet...")
    
    # Build all breakdowns
    perm_country = build_perm_by_country()
    lca_visa = build_lca_by_visa_class()
    visa_apps = build_visa_apps_by_class()
    uscis_form = build_uscis_by_form_type()
    
    # Combine all
    combined = pd.concat([perm_country, lca_visa, visa_apps, uscis_form], 
                         ignore_index=True, sort=False)
    
    # Ensure fiscal_year is numeric
    combined['fiscal_year'] = pd.to_numeric(combined['fiscal_year'], errors='coerce').astype('Int64')
    
    # Sort
    combined = combined.sort_values(['data_source', 'fiscal_year', 'employer_country']).reset_index(drop=True)
    
    # Write output
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    logger.info(f"\n✅ Wrote {len(combined):,} rows to {OUTPUT_FILE}")
    logger.info(f"   Data sources: {combined['data_source'].unique().tolist()}")
    logger.info(f"   Categories: {combined['category'].unique().tolist()}")
    logger.info(f"   Fiscal years: {combined['fiscal_year'].min()} to {combined['fiscal_year'].max()}")
    logger.info(f"   Columns: {list(combined.columns)}")


if __name__ == "__main__":
    main()
