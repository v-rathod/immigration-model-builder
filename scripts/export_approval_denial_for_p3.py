#!/usr/bin/env python3
"""
Export approval_denial_trends and related artifacts to JSON for P3 Compass dashboard.

Converts parquet artifacts to optimized JSON slices for web consumption.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

ARTIFACTS_DIR = Path("artifacts/tables")
RAG_DIR = Path("artifacts/rag")
OUTPUT_DIR = RAG_DIR / "approval_denial_export"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def export_approval_denial_trends():
    """Export approval_denial_trends as JSON."""
    print("📊 Exporting approval_denial_trends...")
    
    df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
    
    # Convert to records format
    records = df.to_dict('records')
    
    # Convert integers and floats properly
    for record in records:
        if pd.notna(record.get('fiscal_year')):
            record['fiscal_year'] = int(record['fiscal_year'])
        for col in ['APPROVED', 'DENIED', 'total_cases']:
            if col in record and pd.notna(record[col]):
                record[col] = int(record[col])
        for col in ['approval_rate_pct', 'denial_rate_pct']:
            if col in record and pd.notna(record[col]):
                record[col] = float(record[col])
    
    output_file = OUTPUT_DIR / "approval_denial_trends.json"
    with open(output_file, 'w') as f:
        json.dump(records, f, indent=2)
    
    print(f"  ✓ {len(records)} records → {output_file}")
    return records


def export_approval_denial_summary():
    """Export 10-year summary statistics."""
    print("📈 Exporting 10-year summary...")
    
    df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
    
    # Get PERM data (most relevant for employment-based)
    perm_data = df[df['data_source'] == 'PERM_Labor_Certification'].copy()
    perm_data = perm_data.sort_values('fiscal_year')
    
    # Last 10 years
    last_10 = perm_data.tail(10).copy()
    
    summary = {
        "period": "Last 10 Fiscal Years (PERM)",
        "data_source": "PERM Labor Certification",
        "total_cases": int(last_10['total_cases'].sum()),
        "total_approved": int(last_10['APPROVED'].sum()),
        "total_denied": int(last_10['DENIED'].sum()),
        "avg_approval_rate": float(last_10['approval_rate_pct'].mean()),
        "min_approval_rate": float(last_10['approval_rate_pct'].min()),
        "max_approval_rate": float(last_10['approval_rate_pct'].max()),
        "trend": "increasing" if last_10.iloc[-1]['approval_rate_pct'] > last_10.iloc[0]['approval_rate_pct'] else "decreasing",
        "yearly_breakdown": last_10[['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 'approval_rate_pct', 'denial_rate_pct']].to_dict('records')
    }
    
    # Normalize fiscal year
    for rec in summary['yearly_breakdown']:
        rec['fiscal_year'] = int(rec['fiscal_year'])
        rec['APPROVED'] = int(rec['APPROVED'])
        rec['DENIED'] = int(rec['DENIED'])
        rec['total_cases'] = int(rec['total_cases'])
        rec['approval_rate_pct'] = float(rec['approval_rate_pct'])
        rec['denial_rate_pct'] = float(rec['denial_rate_pct'])
    
    output_file = OUTPUT_DIR / "approval_denial_summary.json"
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"  ✓ Summary stats → {output_file}")
    return summary


def export_approval_denial_by_category():
    """Export approval rates by data source/category."""
    print("🏷️  Exporting by category...")
    
    df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
    
    # Aggregate by data source and visa category
    by_category = df.groupby(['data_source', 'visa_category']).agg({
        'APPROVED': 'sum',
        'DENIED': 'sum',
        'total_cases': 'sum'
    }).reset_index()
    
    by_category['approval_rate_pct'] = (by_category['APPROVED'] / by_category['total_cases'] * 100).round(2)
    by_category['denial_rate_pct'] = (by_category['DENIED'] / by_category['total_cases'] * 100).round(2)
    
    records = []
    for _, row in by_category.iterrows():
        records.append({
            'data_source': row['data_source'],
            'visa_category': row['visa_category'],
            'total_cases': int(row['total_cases']),
            'approved': int(row['APPROVED']),
            'denied': int(row['DENIED']),
            'approval_rate_pct': float(row['approval_rate_pct']),
            'denial_rate_pct': float(row['denial_rate_pct'])
        })
    
    output_file = OUTPUT_DIR / "approval_denial_by_category.json"
    with open(output_file, 'w') as f:
        json.dump(records, f, indent=2)
    
    print(f"  ✓ {len(records)} categories → {output_file}")
    return records


def export_perm_trends_detailed():
    """Export detailed PERM trends for chart visualization."""
    print("📉 Exporting PERM detailed trends...")
    
    df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
    perm = df[df['data_source'] == 'PERM_Labor_Certification'].copy()
    perm = perm.sort_values('fiscal_year')
    
    # Format for charting
    chart_data = {
        "title": "PERM Labor Certification: 19-Year Approval Trends",
        "subtitle": "Worldwide Employment-Based First Preference Cases",
        "source": "US Department of Labor",
        "fiscal_years": int(perm['fiscal_year'].min()),
        "last_fiscal_year": int(perm['fiscal_year'].max()),
        "data_points": []
    }
    
    for _, row in perm.iterrows():
        chart_data['data_points'].append({
            'fiscal_year': int(row['fiscal_year']),
            'approved': int(row['APPROVED']),
            'denied': int(row['DENIED']),
            'total': int(row['total_cases']),
            'approval_rate': float(row['approval_rate_pct']),
            'denial_rate': float(row['denial_rate_pct'])
        })
    
    # Calculate year-over-year change
    for i in range(1, len(chart_data['data_points'])):
        curr = chart_data['data_points'][i]
        prev = chart_data['data_points'][i-1]
        curr['yoy_total_change_pct'] = round(((curr['total'] - prev['total']) / prev['total'] * 100), 2)
        curr['yoy_approval_rate_change'] = round((curr['approval_rate'] - prev['approval_rate']), 2)
    
    output_file = OUTPUT_DIR / "perm_trends_detailed.json"
    with open(output_file, 'w') as f:
        json.dump(chart_data, f, indent=2)
    
    print(f"  ✓ {len(chart_data['data_points'])} years → {output_file}")
    return chart_data


def main():
    print("\n" + "="*70)
    print("P3 APPROVAL/DENIAL EXPORT")
    print("="*70 + "\n")
    
    trends = export_approval_denial_trends()
    summary = export_approval_denial_summary()
    categories = export_approval_denial_by_category()
    detailed = export_perm_trends_detailed()
    
    print("\n" + "="*70)
    print("✅ Export complete!")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print("="*70)
    print(f"\n📋 Files created:")
    for f in sorted(OUTPUT_DIR.glob('*.json')):
        print(f"  • {f.name}")
    
    print(f"\n💡 Summary Statistics (PERM Last 10 Years):")
    print(f"  • Total cases: {summary['total_cases']:,}")
    print(f"  • Total approved: {summary['total_approved']:,}")
    print(f"  • Total denied: {summary['total_denied']:,}")
    print(f"  • Avg approval rate: {summary['avg_approval_rate']:.1f}%")
    print(f"  • Trend: {summary['trend'].upper()}")
    print()


if __name__ == "__main__":
    main()
