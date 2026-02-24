#!/usr/bin/env python3
"""
Aggregated Audit Bundle: Create a ZIP bundle of all audit reports and logs for external review.
"""

import argparse
import sys
import zipfile
import json
from pathlib import Path
from datetime import datetime
import yaml


def load_paths_config(config_path: Path) -> dict:
    """Load paths configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def generate_readme(metrics_dir: Path, data_root: Path) -> str:
    """
    Generate README content for the audit bundle.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    readme = f"""# Immigration Model Builder - Audit Bundle

Generated: {timestamp}
Data Root: {data_root}

## Contents

This bundle contains audit reports and logs for the immigration model builder pipeline.

### Input Coverage Report
- `input_coverage_report.md` - Human-readable coverage report
- `input_coverage_report.json` - Machine-readable coverage data

Tracks which input files under data_root were processed vs. expected.

### Output Audit Report
- `output_audit_report.md` - Human-readable output validation report
- `output_audit_report.json` - Machine-readable output validation data

Validates curated outputs: row counts, required columns, PK uniqueness, partitions.

### Logs
- `*.log` - Pipeline execution logs (if present)

## Summary

"""
    
    # Try to read coverage summary
    try:
        coverage_json = metrics_dir / "input_coverage_report.json"
        if coverage_json.exists():
            with open(coverage_json, 'r') as f:
                coverage_data = json.load(f)
            
            readme += "### Input Coverage\n\n"
            readme += "| Dataset | Expected | Processed | Coverage % |\n"
            readme += "|---------|----------|-----------|------------|\n"
            
            for dataset, metrics in sorted(coverage_data.items()):
                exp = metrics["expected"]
                proc = metrics["processed"]
                cov_pct = metrics["coverage_pct"] * 100
                readme += f"| {dataset} | {exp} | {proc} | {cov_pct:.1f}% |\n"
            
            readme += "\n"
    except Exception as e:
        readme += f"(Unable to load coverage summary: {e})\n\n"
    
    # Try to read output audit summary
    try:
        output_json = metrics_dir / "output_audit_report.json"
        if output_json.exists():
            with open(output_json, 'r') as f:
                output_data = json.load(f)
            
            readme += "### Output Audit\n\n"
            readme += "| Table | Exists | Rows | Status |\n"
            readme += "|-------|--------|------|--------|\n"
            
            for table, metrics in sorted(output_data.items()):
                exists = "✓" if metrics["exists"] else "✗"
                rows = f"{metrics['rows']:,}" if metrics["exists"] else "N/A"
                
                if metrics["error"]:
                    status = "ERROR"
                elif not metrics["exists"]:
                    status = "MISSING"
                elif metrics["required_missing"] or metrics["pk_unique"] == False:
                    status = "FAIL"
                else:
                    status = "PASS"
                
                readme += f"| {table} | {exists} | {rows} | {status} |\n"
            
            readme += "\n"
    except Exception as e:
        readme += f"(Unable to load output audit summary: {e})\n\n"
    
    readme += """
## Regeneration Instructions

To regenerate these reports:

1. **Dry-run preview** (no writes):
   ```bash
   python -m src.curate.run_curate --paths configs/paths.yaml --dry-run
   ```

2. **Build curated outputs** (real run):
   ```bash
   python -m src.curate.run_curate --paths configs/paths.yaml
   ```

3. **Generate audit reports**:
   ```bash
   python scripts/audit_input_coverage.py --paths configs/paths.yaml --report artifacts/metrics/input_coverage_report.md --json artifacts/metrics/input_coverage_report.json
   python scripts/audit_outputs.py --paths configs/paths.yaml --schemas configs/schemas.yml --report artifacts/metrics/output_audit_report.md --json artifacts/metrics/output_audit_report.json
   ```

4. **Run tests** (includes coverage checks):
   ```bash
   pytest -q
   ```

5. **Create audit bundle**:
   ```bash
   python scripts/make_audit_bundle.py --out artifacts/metrics/audit_bundle.zip
   ```

## Notes

- Coverage thresholds: ≥95% for datasets with ≥10 expected files
- Output validation: All required columns present, PK uniqueness for dimensions
- Sampling limits may explain lower coverage percentages (intentional for development)

"""
    
    return readme


def main():
    parser = argparse.ArgumentParser(description="Create audit bundle ZIP for external review")
    parser.add_argument("--out", required=True, help="Path to output ZIP file")
    parser.add_argument("--paths", default="configs/paths.yaml", help="Path to paths.yaml config")
    args = parser.parse_args()
    
    print("="*60)
    print("AUDIT BUNDLE CREATION")
    print("="*60)
    
    # Load configuration
    config_path = Path(args.paths)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    
    config = load_paths_config(config_path)
    data_root = Path(config['data_root'])
    artifacts_root = Path(config['artifacts_root'])
    metrics_dir = artifacts_root / "metrics"
    
    print(f"Artifacts root: {artifacts_root}")
    print(f"Metrics directory: {metrics_dir}")
    print()
    
    # Check if metrics directory exists
    if not metrics_dir.exists():
        print(f"WARNING: Metrics directory not found: {metrics_dir}")
        print("Creating empty directory...")
        metrics_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect files to bundle
    files_to_bundle = []
    
    # Input coverage reports
    input_md = metrics_dir / "input_coverage_report.md"
    input_json = metrics_dir / "input_coverage_report.json"
    
    if input_md.exists():
        files_to_bundle.append(("input_coverage_report.md", input_md))
    else:
        print(f"  WARNING: {input_md.name} not found")
    
    if input_json.exists():
        files_to_bundle.append(("input_coverage_report.json", input_json))
    else:
        print(f"  WARNING: {input_json.name} not found")
    
    # Output audit reports
    output_md = metrics_dir / "output_audit_report.md"
    output_json = metrics_dir / "output_audit_report.json"
    
    if output_md.exists():
        files_to_bundle.append(("output_audit_report.md", output_md))
    else:
        print(f"  WARNING: {output_md.name} not found")
    
    if output_json.exists():
        files_to_bundle.append(("output_audit_report.json", output_json))
    else:
        print(f"  WARNING: {output_json.name} not found")
    
    # Collect all log files
    log_files = list(metrics_dir.glob("*.log"))
    for log_file in log_files:
        files_to_bundle.append((f"logs/{log_file.name}", log_file))
    
    print(f"Collected {len(files_to_bundle)} file(s) to bundle:")
    for arc_name, _ in files_to_bundle:
        print(f"  - {arc_name}")
    print()
    
    # Generate README
    print("Generating README_audit.txt...")
    readme_content = generate_readme(metrics_dir, data_root)
    
    # Create ZIP bundle
    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Creating ZIP bundle: {output_path}")
    
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Add README
        zf.writestr("README_audit.txt", readme_content)
        
        # Add all collected files
        for arc_name, file_path in files_to_bundle:
            zf.write(file_path, arcname=arc_name)
    
    # Verify ZIP was created
    if not output_path.exists():
        print(f"ERROR: Failed to create ZIP bundle", file=sys.stderr)
        sys.exit(1)
    
    # Get ZIP size
    zip_size = output_path.stat().st_size
    
    print()
    print("="*60)
    print("✓ Audit bundle created successfully")
    print(f"  Location: {output_path.absolute()}")
    print(f"  Size: {zip_size:,} bytes ({zip_size / 1024:.1f} KB)")
    print(f"  Contents: {len(files_to_bundle) + 1} file(s) (including README)")
    print("="*60)
    print()
    print(f"READY_TO_UPLOAD_BUNDLE: {output_path.absolute()}")


if __name__ == "__main__":
    main()
