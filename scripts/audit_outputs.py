#!/usr/bin/env python3
"""
Output Partition & Row Auditor: Verify curated outputs exist, have required columns,
PK uniqueness, and row counts.

With --manifest: reads directly from build_manifest.json — no filesystem scanning,
no parquet reads (fast, deterministic).
Without --manifest: legacy behaviour (rglob + parquet reads).
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

# ── Global performance knobs ─────────────────────────────────────────────────
CHUNK_SIZE: int = int(os.environ.get("CHUNK_SIZE", 250_000))
CONCURRENCY: int = int(os.environ.get("CONCURRENCY", 3))
ONLY_REWRITE_PARQUET: bool = os.environ.get("ONLY_REWRITE_PARQUET", "true").lower() == "true"

EXCLUDE_PATTERNS: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)


def load_config(config_path: Path) -> dict:
    """Load YAML configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_table_path(artifacts_root: Path, table_name: str) -> Path:
    """Resolve output path for a table (single parquet or partitioned directory).

    For fact tables that are always partitioned, the single-file path is skipped
    to avoid accidentally reading stale flat-file backups.
    """
    # Known partitioned fact tables — never read as a single .parquet file
    PARTITIONED_TABLES = {"fact_perm", "fact_lca", "fact_oews", "fact_cutoffs"}

    if table_name not in PARTITIONED_TABLES:
        # Try single parquet first (OK for dims)
        single_path = artifacts_root / "tables" / f"{table_name}.parquet"
        if single_path.exists():
            return single_path

    # Try partitioned directory
    dir_path = artifacts_root / "tables" / table_name
    if dir_path.exists() and dir_path.is_dir():
        return dir_path

    # Not found
    return None


def audit_table(
    table_name: str,
    table_schema: dict,
    artifacts_root: Path
) -> dict:
    """
    Audit a single table.
    Returns dict with: {exists, row_count, required_missing, pk_unique, partitions, error}
    """
    result = {
        "exists": False,
        "row_count": 0,
        "columns_present": [],
        "columns_sample": [],
        "required_missing": [],
        "pk_unique": None,
        "partitions": [],
        "error": None
    }
    
    # Find table path
    table_path = get_table_path(artifacts_root, table_name)
    
    if table_path is None:
        result["error"] = "Table not found"
        return result
    
    result["exists"] = True
    
    try:
        # Read table (handle both single file and partitioned)
        if table_path.is_file():
            df = pd.read_parquet(table_path)
        else:
            # Partitioned directory
            # For some tables (e.g., fact_perm), PyArrow cannot read partitioned directories
            # directly due to "Cannot yet unify dictionaries with nulls" error.
            # Solution: Read each partition file separately and concatenate.
            parquet_files = [
                pf for pf in table_path.rglob("*.parquet")
                if not any(
                    excl in str(pf)
                    for excl in ("_backup", "_quarantine", ".tmp_", "/tmp_")
                )
            ]
            if not parquet_files:
                result["error"] = "No parquet files found in directory"
                return result
            
            # Read each partition file individually
            dfs = []
            for pf in parquet_files:
                partition_df = pd.read_parquet(pf)
                
                # Restore partition columns from directory structure
                # Example: fiscal_year=2008/data.parquet → add fiscal_year=2008 column
                parts = pf.parts
                for part in parts:
                    if '=' in part:
                        col_name, col_value = part.split('=', 1)
                        if col_name not in partition_df.columns:
                            partition_df[col_name] = col_value
                
                dfs.append(partition_df)
            
            df = pd.concat(dfs, ignore_index=True)
        
        result["row_count"] = len(df)
        result["columns_present"] = list(df.columns)
        result["columns_sample"] = list(df.columns)[:20]
        
        # Check required columns
        required_fields = [field['name'] for field in table_schema.get('fields', [])]
        missing_cols = [col for col in required_fields if col not in df.columns]
        result["required_missing"] = missing_cols
        
        # Check PK uniqueness only if primary_key is defined in schema
        # For facts without a PK in schemas.yml, skip PK checks
        primary_key = table_schema.get('primary_key', [])
        if primary_key and all(pk_col in df.columns for pk_col in primary_key):
            if len(primary_key) == 1:
                pk_col = primary_key[0]
                unique_count = df[pk_col].nunique()
                result["pk_unique"] = (unique_count == len(df))
            else:
                # Composite PK
                pk_df = df[primary_key].dropna()
                unique_count = len(pk_df.drop_duplicates())
                result["pk_unique"] = (unique_count == len(pk_df))
        
        # Detect partitions (common partition columns)
        partition_cols = ['fy', 'ref_year', 'fiscal_year', 'bulletin_year']
        for pcol in partition_cols:
            if pcol in df.columns:
                partition_values = sorted(df[pcol].dropna().unique())
                result["partitions"] = [
                    {"column": pcol, "values": [int(v) if isinstance(v, (int, float)) else str(v) for v in partition_values]}
                ]
                break
    
    except Exception as e:
        result["error"] = str(e)
    
    return result


def generate_markdown_report(
    report_path: Path,
    table_names: list,
    schemas: dict,
    audit_results: dict
):
    """Generate markdown output audit report."""
    
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("# Output Audit Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")
        
        # Overall summary
        f.write("## Overall Summary\n\n")
        f.write("| Table | Exists | Rows | Required Cols | PK Unique | Status |\n")
        f.write("|-------|--------|------|---------------|-----------|--------|\n")
        
        for table in table_names:
            result = audit_results[table]
            exists = "✓" if result["exists"] else "✗"
            rows = f"{result['row_count']:,}" if result["exists"] else "N/A"
            
            if result["error"]:
                req_cols = "ERROR"
                pk_unique = "N/A"
                status = "✗ ERROR"
            elif not result["exists"]:
                req_cols = "N/A"
                pk_unique = "N/A"
                status = "✗ MISSING"
            else:
                missing_count = len(result["required_missing"])
                req_cols = "✗ MISSING" if missing_count > 0 else "✓ OK"
                
                if result["pk_unique"] is None:
                    pk_unique = "N/A"
                elif result["pk_unique"]:
                    pk_unique = "✓"
                else:
                    pk_unique = "✗"
                
                if missing_count > 0 or result["pk_unique"] == False:
                    status = "✗ FAIL"
                else:
                    status = "✓ PASS"
            
            f.write(f"| {table} | {exists} | {rows} | {req_cols} | {pk_unique} | {status} |\n")
        
        f.write("\n---\n\n")
        
        # Per-table details
        for table in table_names:
            result = audit_results[table]
            schema = schemas.get(table, {})
            
            f.write(f"## {table}\n\n")
            f.write(f"**Description:** {schema.get('description', 'N/A')}\n\n")
            
            if not result["exists"]:
                f.write("**Status:** ✗ Table not found\n\n")
                f.write("---\n\n")
                continue
            
            if result["error"]:
                f.write(f"**Status:** ✗ Error reading table\n\n")
                f.write(f"**Error:** {result['error']}\n\n")
                f.write("---\n\n")
                continue
            
            f.write(f"**Row Count:** {result['row_count']:,}\n\n")
            
            # Required columns
            required_fields = [field['name'] for field in schema.get('fields', [])]
            missing = result["required_missing"]
            
            if missing:
                f.write(f"**Required Columns:** ✗ {len(missing)} missing\n\n")
                f.write("Missing columns:\n")
                for col in sorted(missing):
                    f.write(f"- `{col}`\n")
                f.write("\n")
            else:
                f.write(f"**Required Columns:** \u2713 All {len(required_fields)} present\n\n")
            
            # Show actual columns (sample 20)
            actual_cols = result.get("columns_sample", result.get("columns_present", []))[:20]
            if actual_cols:
                f.write(f"**Actual Columns (sample {len(actual_cols)}):** {', '.join(f'`{c}`' for c in actual_cols)}\n\n")
            
            # PK uniqueness
            primary_key = schema.get('primary_key', [])
            if primary_key:
                pk_str = ", ".join(primary_key)
                if result["pk_unique"] is None:
                    f.write(f"**Primary Key:** `{pk_str}` - Unable to check\n\n")
                elif result["pk_unique"]:
                    f.write(f"**Primary Key:** `{pk_str}` - ✓ Unique\n\n")
                else:
                    f.write(f"**Primary Key:** `{pk_str}` - ✗ Duplicates found\n\n")
            
            # Partitions
            if result["partitions"]:
                f.write("**Partitions:**\n\n")
                for pinfo in result["partitions"]:
                    pcol = pinfo["column"]
                    pvals = pinfo["values"]
                    f.write(f"- Column: `{pcol}`\n")
                    f.write(f"- Values: {', '.join(map(str, pvals))}\n")
                f.write("\n")
            
            f.write("---\n\n")
        
        # Notes
        f.write("## Notes\n\n")
        f.write("- **Required Columns**: Checks if all fields defined in schema are present in output.\n")
        f.write("- **PK Unique**: For dimensions, verifies primary key has no duplicates.\n")
        f.write("- **Partitions**: For facts, lists detected partition columns and values.\n")
        f.write("- Exit code 1 if any required column is missing OR a dim PK uniqueness check fails.\n\n")


def generate_json_report(
    json_path: Path,
    table_names: list,
    audit_results: dict
):
    """Generate JSON output audit report."""
    
    json_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build compact JSON structure
    report = {}
    
    for table in table_names:
        result = audit_results[table]
        report[table] = {
            "exists": result["exists"],
            "rows": result["row_count"],
            "columns_present": result.get("columns_present", []),
            "columns_sample": result.get("columns_sample", []),
            "required_missing": result["required_missing"],
            "pk_unique": result["pk_unique"],
            "partitions": result["partitions"],
            "error": result["error"]
        }
    
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_partition_suffix(partitions: list[dict]) -> str:
    """
    Build a compact partition-info string for stdout display.

    For bulletin_year:  shows span, e.g. "bulletin_year: 2015-2025"
    For bulletin_month: shows count, e.g. "bulletin_month: 12 vals"
    For fiscal_year / ref_year: shows "col: N vals"
    """
    if not partitions:
        return ""
    items: list[str] = []
    for pinfo in partitions:
        pcol = pinfo.get("column", "")
        pvals = sorted(str(v) for v in pinfo.get("values", []))
        if not pvals:
            continue
        if pcol == "bulletin_month":
            items.append(f"{pcol}: {len(pvals)} vals")
        elif pcol == "bulletin_year" and len(pvals) > 1:
            items.append(f"{pcol}: {pvals[0]}\u2013{pvals[-1]}")
        elif len(pvals) == 1:
            items.append(f"{pcol}: {pvals[0]}")
        else:
            items.append(f"{pcol}: {len(pvals)} vals")
    return f"  [{' | '.join(items)}]" if items else ""


# ── Manifest-based fast path ─────────────────────────────────────────────────

def _audit_from_manifest(
    manifest: dict,
    table_names: list[str],
    schemas_config: dict,
) -> dict[str, dict]:
    """
    Build audit_results directly from manifest JSON.
    No filesystem scanning or parquet reads.
    """
    results: dict[str, dict] = {}
    tables = manifest.get("tables", {})

    for tname in table_names:
        schema = schemas_config.get(tname, {})
        required_fields = [f["name"] for f in schema.get("fields", [])]
        primary_key = schema.get("primary_key", [])

        entry = tables.get(tname)
        if entry is None:
            results[tname] = {
                "exists": False, "row_count": 0,
                "columns_present": [], "columns_sample": [],
                "required_missing": [], "pk_unique": None,
                "partitions": [], "error": "not in manifest",
            }
            continue

        cols = entry.get("columns", [])
        missing_cols = [c for c in required_fields if c not in cols]

        # Partition info
        partitions = []
        pk_vals = entry.get("partition_values", {})
        for pcol, pvals in pk_vals.items():
            partitions.append({"column": pcol, "values": pvals})

        results[tname] = {
            "exists": True,
            "row_count": entry.get("row_count", 0),
            "columns_present": cols,
            "columns_sample": cols[:20],
            "required_missing": missing_cols,
            "pk_unique": entry.get("pk_unique"),
            "pk_null_rows": entry.get("pk_null_rows", 0),
            "partitions": partitions,
            "error": entry.get("error"),
            "source": "manifest",
        }

    return results


def _vb_from_presentation(
    pres_path: Path,
    snapshot_path: Path,
    schemas_config: dict,
) -> dict:
    """
    Build audit_result for fact_cutoffs from the presentation table + snapshot JSON.
    Used when --vb_presentation is supplied; avoids scanning partitioned directory.
    """
    import pandas as _pd

    schema = schemas_config.get("fact_cutoffs", {})
    required_fields = [f["name"] for f in schema.get("fields", [])]

    if not pres_path.exists():
        return {
            "exists": False, "row_count": 0,
            "columns_present": [], "columns_sample": [],
            "required_missing": [], "pk_unique": None,
            "partitions": [], "error": f"presentation table not found: {pres_path}",
            "source": "presentation",
        }

    df = _pd.read_parquet(pres_path)
    cols = list(df.columns)
    missing_cols = [c for c in required_fields if c not in cols]

    # PK check on deduplicated presentation table
    pk_cols = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
    pk_sub = df[[c for c in pk_cols if c in df.columns]].dropna()
    pk_unique = len(pk_sub) == len(pk_sub.drop_duplicates())

    # Partition info from snapshot if available
    partitions: list[dict] = []
    if snapshot_path.exists():
        with open(snapshot_path) as _sf:
            snap = json.load(_sf)
        snap_summary = snap.get("summary", {})
        years_span = snap_summary.get("years_span", "")
        leaves = snap_summary.get("leaves", 0)
        # Build partition display entries
        if years_span and "-" in years_span:
            yr_min, yr_max = years_span.split("-", 1)
            # Enumerate distinct years
            distinct_yrs = [
                int(p["bulletin_year"])
                for p in snap.get("partitions", [])
            ]
            distinct_yrs = sorted(set(distinct_yrs))
            partitions.append({"column": "bulletin_year", "values": distinct_yrs})
            # Enumerate distinct months
            distinct_mos = sorted({
                int(p["bulletin_month"])
                for p in snap.get("partitions", [])
            })
            partitions.append({"column": "bulletin_month", "values": distinct_mos})
    else:
        # Fall back: infer from presentation table
        if "bulletin_year" in df.columns:
            partitions.append({
                "column": "bulletin_year",
                "values": sorted(df["bulletin_year"].dropna().unique().astype(int).tolist()),
            })
        if "bulletin_month" in df.columns:
            partitions.append({
                "column": "bulletin_month",
                "values": sorted(df["bulletin_month"].dropna().unique().astype(int).tolist()),
            })

    return {
        "exists": True,
        "row_count": len(df),
        "columns_present": cols,
        "columns_sample": cols[:20],
        "required_missing": missing_cols,
        "pk_unique": pk_unique,
        "partitions": partitions,
        "error": None,
        "source": "presentation",
    }


def main():
    parser = argparse.ArgumentParser(description="Audit curated output tables")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    parser.add_argument("--schemas", required=True, help="Path to schemas.yml config")
    parser.add_argument("--report", required=True, help="Path to output markdown report")
    parser.add_argument("--json", help="Path to output JSON report (optional)")
    parser.add_argument(
        "--manifest",
        help="Path to build_manifest.json (fast path; skips filesystem scanning)",
    )
    parser.add_argument(
        "--vb_presentation",
        help="Path to fact_cutoffs_all.parquet (presentation table); if supplied, "
             "audit_outputs reads fact_cutoffs rows/columns from this file and "
             "skips partition scanning entirely.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print results but do not write files")
    args = parser.parse_args()
    
    print("="*60)
    print("OUTPUT AUDIT")
    print("="*60)
    
    # Load configurations
    paths_config = load_config(Path(args.paths))
    schemas_config = load_config(Path(args.schemas))
    
    artifacts_root = Path(paths_config['artifacts_root'])
    
    print(f"Artifacts root: {artifacts_root}")
    print(f"Schemas: {args.schemas}")
    if args.manifest:
        print(f"Manifest: {args.manifest}")
    else:
        print("ERROR: --manifest is required.", file=sys.stderr)
        print("  Run: python scripts/make_build_manifest.py  then re-run this audit.", file=sys.stderr)
        sys.exit(1)
    print()

    # Get table definitions (dims + facts)
    table_names = list(schemas_config.keys())
    print(f"Auditing {len(table_names)} tables...")
    print()

    # Resolve snapshot path for VB presentation mode
    _snapshot_path = artifacts_root / "tables" / "fact_cutoffs" / "_snapshot.json"

    # ── Fast path: manifest supplied ────────────────────────────────────────
    if args.manifest:
        manifest_path = Path(args.manifest)
        if not manifest_path.exists():
            print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
            sys.exit(1)
        with open(manifest_path) as _mf:
            manifest_data = json.load(_mf)
        print(f"  [manifest mode] build_id={manifest_data.get('build_id', 'unknown')}")
        audit_results = _audit_from_manifest(manifest_data, table_names, schemas_config)

        # ── Override fact_cutoffs with presentation table if provided ─────
        if args.vb_presentation:
            pres_path = Path(args.vb_presentation)
            print(f"  [VB presentation] {pres_path}")
            audit_results["fact_cutoffs"] = _vb_from_presentation(
                pres_path, _snapshot_path, schemas_config
            )

        for tname, result in audit_results.items():
            if not result["exists"]:
                print(f"  {tname}: NOT FOUND")
            elif result.get("error") and result["error"] not in (None, ""):
                # non-fatal errors (e.g. pk_check failed on one partition) — show but don't fail
                status = "✓" if (len(result["required_missing"]) == 0 and result["pk_unique"] != False) else "✗"
                part_sfx = _format_partition_suffix(result.get("partitions", []))
                print(f"  {tname}: {status} {result['row_count']:,} rows{part_sfx}  [warn: {result['error']}]")
            else:
                status = "✓" if (len(result["required_missing"]) == 0 and result["pk_unique"] != False) else "✗"
                part_sfx = _format_partition_suffix(result.get("partitions", []))
                print(f"  {tname}: {status} {result['row_count']:,} rows{part_sfx}")
        print()
    else:
        # ── Legacy path: filesystem scanning ────────────────────────────────
        audit_results = {}
        for table_name in table_names:
            print(f"  Auditing {table_name}...", end=" ")
            table_schema = schemas_config[table_name]
            result = audit_table(table_name, table_schema, artifacts_root)
            audit_results[table_name] = result

            if not result["exists"]:
                print("NOT FOUND")
            elif result["error"]:
                print(f"ERROR: {result['error']}")
            else:
                status = "✓" if (len(result["required_missing"]) == 0 and result["pk_unique"] != False) else "✗"
                print(f"{status} {result['row_count']:,} rows")
        print()
    
    # Generate markdown report
    if not args.dry_run:
        print(f"Generating report: {args.report}")
        report_path = Path(args.report)
        generate_markdown_report(
            report_path,
            table_names,
            schemas_config,
            audit_results
        )
        print(f"Report written to: {report_path}")
    
        # Generate JSON report if requested
        if args.json:
            print(f"Generating JSON report: {args.json}")
            json_path = Path(args.json)
            generate_json_report(
                json_path,
                table_names,
                audit_results
            )
            print(f"JSON report written to: {json_path}")
    
    print()
    
    # Determine exit code
    exit_code = 0
    issues = []
    
    for table_name, result in audit_results.items():
        if not result["exists"]:
            # Table not found - warning but not necessarily failure
            continue
        
        if result["error"]:
            issues.append(f"{table_name}: Error - {result['error']}")
            exit_code = 1
            continue
        
        # Check for missing required columns
        if result["required_missing"]:
            missing_str = ", ".join(result["required_missing"][:3])
            if len(result["required_missing"]) > 3:
                missing_str += f" + {len(result['required_missing']) - 3} more"
            issues.append(f"{table_name}: Missing required columns - {missing_str}")
            exit_code = 1
        
        # Check PK uniqueness for dims
        if result["pk_unique"] == False:
            issues.append(f"{table_name}: Primary key not unique")
            exit_code = 1
    
    # Print summary
    print("="*60)
    if exit_code == 0:
        print("✓ Output audit PASSED")
    else:
        print("✗ Output audit FAILED")
        print("\nIssues:")
        for issue in issues:
            print(f"  ✗ {issue}")
    print("="*60)

    # ── Non-blocking derived tables check ─────────────────────────────────────
    # These tables are optional consumer views; their absence never fails the audit.
    derived_info: list[str] = []
    if args.manifest:
        manifest_tables = manifest_data.get("tables", {})
        for tname, entry in manifest_tables.items():
            if entry.get("type") == "derived":
                rc = entry.get("row_count", 0)
                nfiles = len(entry.get("files", []))
                err = entry.get("error")
                if err:
                    derived_info.append(f"  ↳ {tname}: not yet built ({err})")
                else:
                    derived_info.append(
                        f"  ↳ {tname}: {rc:,} rows  files={nfiles}"
                        f"  [derived: one row per case_number]"
                    )
    else:
        # Legacy: check directory directly
        for tname in ["fact_perm_unique_case"]:
            tdir = artifacts_root / "tables" / tname
            if tdir.exists():
                import pyarrow.parquet as _pq
                pfiles = list(tdir.rglob("*.parquet"))
                rc = sum(_pq.read_metadata(pf).num_rows for pf in pfiles)
                derived_info.append(
                    f"  ↳ {tname}: {rc:,} rows  [derived: one row per case_number]"
                )
            else:
                derived_info.append(f"  ↳ {tname}: not yet built")

    if derived_info:
        print()
        print("Derived tables (non-blocking):")
        for line in derived_info:
            print(line)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
