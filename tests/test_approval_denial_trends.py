#!/usr/bin/env python3
"""
Smoke tests for approval_denial_trends artifacts.
"""

import pandas as pd
from pathlib import Path
import pytest

ARTIFACTS_DIR = Path("artifacts/tables")
RAG_DIR = Path("artifacts/rag") / "approval_denial_export"


class TestApprovalDenialTrends:
    """Test approval_denial_trends.parquet"""
    
    def test_artifact_exists(self):
        """Verify artifact exists."""
        assert (ARTIFACTS_DIR / "approval_denial_trends.parquet").exists()
    
    def test_artifact_loads(self):
        """Verify artifact can be loaded."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        assert len(df) > 0
        assert df.shape[1] >= 7
    
    def test_required_columns(self):
        """Verify all required columns exist."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        required_cols = ['fiscal_year', 'APPROVED', 'DENIED', 'total_cases', 
                        'approval_rate_pct', 'denial_rate_pct', 'data_source']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"
    
    def test_data_sources(self):
        """Verify expected data sources are present."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        sources = df['data_source'].unique()
        assert len(sources) >= 2  # Should have at least PERM and one other
        assert 'PERM_Labor_Certification' in sources
    
    def test_fiscal_years_coverage(self):
        """Verify fiscal year coverage."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        perm_data = df[df['data_source'] == 'PERM_Labor_Certification']
        assert len(perm_data) >= 10  # At least 10 years of PERM data
    
    def test_approval_rates_reasonable(self):
        """Verify approval rates are between 0 and 100."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        assert (df['approval_rate_pct'] >= 0).all()
        assert (df['approval_rate_pct'] <= 100.01).all()  # Allow for rounding
    
    def test_counts_non_negative(self):
        """Verify counts are non-negative."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        assert (df['APPROVED'] >= 0).all()
        assert (df['DENIED'] >= 0).all()
        assert (df['total_cases'] >= 0).all()
    
    def test_totals_sum_correctly(self):
        """Verify total cases includes all outcomes including withdrawn."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        # PERM includes WITHDRAWN cases in total_cases, so:
        # total_cases = APPROVED + DENIED + OTHER (Withdrawn)
        # Approval/denial rates are calculated on decided cases only (APPROVED + DENIED)
        perm = df[df['data_source'] == 'PERM_Labor_Certification']
        
        # For each PERM row, verify the math makes sense
        for idx, row in perm.iterrows():
            # total_cases should be >= APPROVED + DENIED (includes OTHER)
            assert row['total_cases'] >= (row['APPROVED'] + row['DENIED']), \
                f"Row {idx}: total_cases {row['total_cases']} < approved+denied {row['APPROVED'] + row['DENIED']}"
    
    def test_perm_approval_rate_reasonable(self):
        """Verify PERM has reasonable approval rates (70%+)."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_trends.parquet")
        perm = df[df['data_source'] == 'PERM_Labor_Certification']
        # Some early years had lower approval rates (< 80%), but recent years much higher
        assert (perm['approval_rate_pct'] >= 75).all(), "PERM approval rate too low"


class TestApprovalDenialDetailed:
    """Test approval_denial_detailed.parquet"""
    
    def test_artifact_exists(self):
        """Verify artifact exists."""
        assert (ARTIFACTS_DIR / "approval_denial_detailed.parquet").exists()
    
    def test_artifact_loads(self):
        """Verify artifact can be loaded."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_detailed.parquet")
        assert len(df) > 0
    
    def test_required_columns(self):
        """Verify all required columns exist."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_detailed.parquet")
        required_cols = ['fiscal_year', 'employer_country', 'approved_count', 'denied_count',
                        'total_count', 'approval_rate_pct', 'data_source']
        for col in required_cols:
            assert col in df.columns
    
    def test_granular_breakdown_exists(self):
        """Verify we have country/class breakdowns."""
        df = pd.read_parquet(ARTIFACTS_DIR / "approval_denial_detailed.parquet")
        assert df['employer_country'].nunique() >= 50  # Multiple countries/classes


class TestP3Exports:
    """Test JSON exports for P3"""
    
    def test_approval_denial_trends_json_exists(self):
        """Verify main trends JSON exists."""
        assert (RAG_DIR / "approval_denial_trends.json").exists()
    
    def test_summary_json_exists(self):
        """Verify summary JSON exists."""
        assert (RAG_DIR / "approval_denial_summary.json").exists()
    
    def test_by_category_json_exists(self):
        """Verify category breakdown JSON exists."""
        assert (RAG_DIR / "approval_denial_by_category.json").exists()
    
    def test_perm_trends_detailed_json_exists(self):
        """Verify detailed PERM trends JSON exists."""
        assert (RAG_DIR / "perm_trends_detailed.json").exists()
    
    def test_summary_json_content(self):
        """Verify summary JSON has expected structure."""
        import json
        with open(RAG_DIR / "approval_denial_summary.json") as f:
            summary = json.load(f)
        
        assert 'total_cases' in summary
        assert 'avg_approval_rate' in summary
        assert 'yearly_breakdown' in summary
        assert len(summary['yearly_breakdown']) >= 10
    
    def test_trends_json_content(self):
        """Verify trends JSON is valid."""
        import json
        with open(RAG_DIR / "approval_denial_trends.json") as f:
            trends = json.load(f)
        
        assert len(trends) > 0
        assert 'fiscal_year' in trends[0]
        assert 'approval_rate_pct' in trends[0]


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
