"""
Test coverage expectations: Assert minimum file coverage thresholds.
"""
import os
import subprocess
import sys
import json
from pathlib import Path
import pytest

def _safe_env():
    """Return env dict with CHAT_TAP_DISABLED=1 to prevent subprocess hang."""
    env = os.environ.copy()
    env["CHAT_TAP_DISABLED"] = "1"
    return env


def run_coverage_audit():
    """
    Run the input coverage auditor and return the JSON report data.
    """
    paths_config = Path("configs/paths.yaml")
    json_report = Path("artifacts/metrics/input_coverage_report.json")
    md_report = Path("artifacts/metrics/input_coverage_report.md")
    
    if not paths_config.exists():
        pytest.skip("configs/paths.yaml not found")
    
    # Ensure artifacts/metrics directory exists
    json_report.parent.mkdir(parents=True, exist_ok=True)
    
    # Run the auditor
    result = subprocess.run(
        [
            sys.executable,
            "scripts/audit_input_coverage.py",
            "--paths", str(paths_config),
            "--report", str(md_report),
            "--json", str(json_report)
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=_safe_env(),
    )
    
    # Check if JSON report was created
    if not json_report.exists():
        pytest.fail(f"Coverage audit failed to generate JSON report:\n{result.stdout}\n{result.stderr}")
    
    # Read JSON report
    with open(json_report, 'r') as f:
        return json.load(f)


def test_coverage_thresholds():
    """
    Test that coverage meets minimum thresholds for datasets with ≥10 expected files.
    """
    # Run coverage audit
    coverage_data = run_coverage_audit()
    
    # Track failures
    failures = []
    warnings = []
    
    # Check each dataset
    for dataset, metrics in coverage_data.items():
        expected = metrics["expected"]
        coverage_pct = metrics["coverage_pct"]
        
        # Skip datasets with < 10 files (informational only)
        if expected < 10:
            if coverage_pct < 0.95 and expected > 0:
                warnings.append(
                    f"{dataset}: {coverage_pct*100:.1f}% coverage (WARN: <10 files, threshold not enforced)"
                )
            continue
        
        # Assert ≥95% coverage for datasets with ≥10 files
        if coverage_pct < 0.95:
            failures.append(
                f"{dataset}: {coverage_pct*100:.1f}% coverage < 95% threshold "
                f"(expected {expected}, processed {metrics['processed']})"
            )
    
    # Print warnings
    if warnings:
        print("\nWarnings (informational only):")
        for warning in warnings:
            print(f"  ⚠️  {warning}")
    
    # Assert no failures
    if failures:
        failure_msg = "\nCoverage threshold failures:\n" + "\n".join(f"  ✗ {f}" for f in failures)
        pytest.fail(failure_msg)


def test_coverage_report_structure():
    """
    Test that the coverage report has the expected structure.
    """
    # Run coverage audit
    coverage_data = run_coverage_audit()
    
    # Check that we have data for key datasets
    expected_datasets = ["PERM", "OEWS", "Visa_Bulletin"]  # LCA may not be implemented yet
    
    for dataset in expected_datasets:
        assert dataset in coverage_data, f"Missing dataset in coverage report: {dataset}"
        
        # Check required fields
        metrics = coverage_data[dataset]
        assert "expected" in metrics, f"{dataset}: missing 'expected' field"
        assert "processed" in metrics, f"{dataset}: missing 'processed' field"
        assert "coverage_pct" in metrics, f"{dataset}: missing 'coverage_pct' field"
        assert "missing" in metrics, f"{dataset}: missing 'missing' field"
        assert "stale" in metrics, f"{dataset}: missing 'stale' field"
        
        # Check field types
        assert isinstance(metrics["expected"], int), f"{dataset}: 'expected' should be int"
        assert isinstance(metrics["processed"], int), f"{dataset}: 'processed' should be int"
        assert isinstance(metrics["coverage_pct"], (int, float)), f"{dataset}: 'coverage_pct' should be numeric"
        assert isinstance(metrics["missing"], list), f"{dataset}: 'missing' should be list"
        assert isinstance(metrics["stale"], list), f"{dataset}: 'stale' should be list"


def test_no_stale_files():
    """
    Test that there are no stale files (processed but no longer present in data_root).
    This is a warning, not a hard failure.
    """
    # Run coverage audit
    coverage_data = run_coverage_audit()
    
    stale_issues = []
    
    for dataset, metrics in coverage_data.items():
        stale_files = metrics.get("stale", [])
        if stale_files:
            stale_issues.append(f"{dataset}: {len(stale_files)} stale file(s)")
    
    # Print warnings if any
    if stale_issues:
        warning_msg = "\n⚠️  Stale files detected (processed but not found in data_root):\n"
        warning_msg += "\n".join(f"  - {issue}" for issue in stale_issues)
        print(warning_msg)
        # This is informational only, not a test failure


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
