"""CLI entrypoint for curation pipeline."""

import argparse
from pathlib import Path
import sys

# ── Commentary capture (permanent) ───────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
try:
    from src.utils import chat_tap as _tap
except Exception:
    _tap = None  # type: ignore

from src.io.readers import load_paths_config, resolve_data_path, resolve_artifact_path, list_files_by_pattern
from src.curate.build_dim_country import build_dim_country
from src.curate.build_dim_soc import build_dim_soc
from src.curate.build_dim_area import build_dim_area
from src.curate.build_dim_visa_class import build_dim_visa_class
from src.curate.build_dim_employer import build_dim_employer
from src.curate.build_fact_perm import build_fact_perm
from src.curate.build_fact_oews import build_fact_oews
from src.curate.visa_bulletin_loader import load_visa_bulletin
from src.curate.perm_loader import load_perm
from src.curate.oews_loader import load_oews
from src.curate.lca_loader import load_lca


def main():
    parser = argparse.ArgumentParser(description="Curate raw immigration data")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    parser.add_argument("--dry-run", action="store_true", help="Preview files/partitions without writing outputs")
    args = parser.parse_args()
    dry_run = args.dry_run  # convenience alias used throughout this function
    if _tap:
        _tap.intercept_chat("agent", f"run_curate START dry_run={args.dry_run}", task="curate", level="INFO")
    
    # Load configuration
    config = load_paths_config(args.paths)
    data_root = config["data_root"]
    artifacts_root = config["artifacts_root"]
    schemas_path = "configs/schemas.yml"
    
    print("="*60)
    print("CURATION PIPELINE" + (" [DRY RUN]" if dry_run else ""))
    print("="*60)
    print(f"Data root: {data_root}")
    print(f"Artifacts root: {artifacts_root}")
    if dry_run:
        print("\n⚠️  DRY RUN MODE: Discovering files only, no writes will be performed")
    print()
    
    # Build dimensions first
    print("--- DIMENSIONS ---")
    
    if dry_run:
        print("\n[SKIPPED in dry-run] Dimensions (dim_country, dim_soc, dim_area, dim_visa_class, dim_employer)")
        print("  Dimensions are static reference data and not affected by dry-run mode.")
        print()
    else:
        # dim_country
        print("\n[1/5] dim_country")
        dim_country_path = resolve_artifact_path(artifacts_root, "tables", "dim_country.parquet")
        try:
            build_dim_country(data_root, dim_country_path, schemas_path)
            # Verify output
            import pandas as pd
            df = pd.read_parquet(dim_country_path)
            print(f"  ✓ dim_country: {len(df)} rows\n")
        except Exception as e:
            print(f"  ✗ Failed to build dim_country: {e}")
            raise
        
        # dim_soc
        print("\n[2/5] dim_soc")
        dim_soc_path = resolve_artifact_path(artifacts_root, "tables", "dim_soc.parquet")
        try:
            build_dim_soc(data_root, dim_soc_path, schemas_path)
            # Verify output
            df = pd.read_parquet(dim_soc_path)
            print(f"  ✓ dim_soc: {len(df)} rows\n")
        except Exception as e:
            print(f"  ✗ Failed to build dim_soc: {e}")
            import traceback
            traceback.print_exc()
        
        # dim_area
        print("\n[3/5] dim_area")
        dim_area_path = resolve_artifact_path(artifacts_root, "tables", "dim_area.parquet")
        try:
            build_dim_area(data_root, dim_area_path, schemas_path)
            # Verify output
            df = pd.read_parquet(dim_area_path)
            print(f"  ✓ dim_area: {len(df)} rows")
            # Show area_type distribution
            if 'area_type' in df.columns:
                print(f"  Area types: {dict(df['area_type'].value_counts())}\n")
        except Exception as e:
            print(f"  ✗ Failed to build dim_area: {e}")
            import traceback
            traceback.print_exc()
        
        # dim_visa_class
        print("\n[4/5] dim_visa_class")
        dim_visa_class_path = resolve_artifact_path(artifacts_root, "tables", "dim_visa_class.parquet")
        try:
            build_dim_visa_class(data_root, dim_visa_class_path, schemas_path)
            # Verify output
            df = pd.read_parquet(dim_visa_class_path)
            print(f"  ✓ dim_visa_class: {len(df)} rows")
            # Show family distribution
            if 'family_code' in df.columns:
                families = df['family_code'].nunique()
                print(f"  Distinct families: {families}")
                print(f"  Family counts: {dict(df['family_code'].value_counts())}\n")
        except Exception as e:
            print(f"  ✗ Failed to build dim_visa_class: {e}")
            import traceback
            traceback.print_exc()
        
        # dim_employer
        print("\n[5/5] dim_employer")
        dim_employer_path = resolve_artifact_path(artifacts_root, "tables", "dim_employer.parquet")
        try:
            # Pass artifacts_root so builder can check for existing fact_perm (fast path)
            build_dim_employer(data_root, dim_employer_path, schemas_path, artifacts_root=artifacts_root)
            # Verify output
            df = pd.read_parquet(dim_employer_path)
            print(f"  ✓ dim_employer: {len(df)} rows")
            
            # Show 5 example canonical→aliases mappings
            if 'employer_name' in df.columns and 'aliases' in df.columns:
                import json
                print(f"\n  Example canonical → aliases mappings:")
                for idx, row in df.head(5).iterrows():
                    aliases_list = json.loads(row['aliases'])
                    print(f"    {row['employer_name']}: {aliases_list[:3]}..." if len(aliases_list) > 3 else f"    {row['employer_name']}: {aliases_list}")
            print()
        except Exception as e:
            print(f"  ✗ Failed to build dim_employer: {e}")
            import traceback
            traceback.print_exc()
    
    # Check if data root exists
    if not Path(data_root).exists():
        print(f"WARNING: Data root does not exist: {data_root}")
        print("Pipeline will create placeholder outputs only.")
        print()
    
    # Build fact tables
    print("\n--- FACTS ---")
    
    # fact_cutoffs (Visa Bulletin)
    print("\n[1/N] fact_cutoffs (Visa Bulletin)")
    try:
        cutoffs_path = load_visa_bulletin(data_root, artifacts_root, schemas_path)
        # Count rows if data exists
        cutoffs_dir = Path(cutoffs_path)
        if cutoffs_dir.exists():
            parquet_files = list(cutoffs_dir.glob("**/*.parquet"))
            if parquet_files:
                total_rows = sum(len(pd.read_parquet(f)) for f in parquet_files)
                print(f"  ✓ fact_cutoffs: {total_rows} rows, {len(parquet_files)} partitions\n")
            else:
                print(f"  ✓ fact_cutoffs: directory created (no data yet)\n")
    except Exception as e:
        print(f"  ✗ Failed to build fact_cutoffs: {e}")
        import traceback
        traceback.print_exc()
    
    # fact_perm (PERM labor certifications) - partitioned by fiscal_year
    print("\n[2/N] fact_perm (PERM Labor Certifications)")
    fact_perm_dir = resolve_artifact_path(artifacts_root, "tables", "fact_perm")
    try:
        # Process ALL PERM FYs with chunked reading (100k rows per chunk)
        build_fact_perm(Path(data_root), fact_perm_dir, Path(artifacts_root), Path(schemas_path).parent, chunk_size=100000, dry_run=dry_run)
        # Verify output (skip in dry-run)
        if not dry_run and fact_perm_dir.exists():
            df_perm = pd.read_parquet(fact_perm_dir)
            print(f"  ✓ fact_perm: {len(df_perm)} rows")
            
            # Show FY breakdown
            if 'fiscal_year' in df_perm.columns:
                fy_counts = df_perm['fiscal_year'].value_counts().sort_index()
                print(f"\n  Rows per fiscal year:")
                for fy, count in fy_counts.items():
                    if pd.notna(fy):
                        print(f"    FY{int(fy)}: {count:,}")
            
            # Show status distribution
            if 'case_status' in df_perm.columns:
                status_counts = df_perm['case_status'].value_counts()
                print(f"\n  Case status distribution:")
                for status, count in status_counts.head(5).items():
                    print(f"    {status}: {count}")
            print()

            # --- Expand dim_employer to match all PERM employers ---
            dim_emp_path = resolve_artifact_path(artifacts_root, "tables", "dim_employer.parquet")
            if dim_emp_path.exists() and 'employer_id' in df_perm.columns and 'employer_name' in df_perm.columns:
                try:
                    from datetime import datetime, timezone as _tz
                    import json as _json
                    df_dim_emp = pd.read_parquet(dim_emp_path)
                    existing_ids = set(df_dim_emp['employer_id'].dropna())
                    perm_emp = (
                        df_perm[['employer_id', 'employer_name']].dropna(subset=['employer_id'])
                        .groupby('employer_id')['employer_name']
                        .first()
                        .reset_index()
                    )
                    missing_emp = perm_emp[~perm_emp['employer_id'].isin(existing_ids)]
                    if len(missing_emp) > 0:
                        now_ts = pd.Timestamp.now(tz='UTC')
                        _first_alias = df_dim_emp['aliases'].dropna().iloc[0] if 'aliases' in df_dim_emp.columns and df_dim_emp['aliases'].notna().any() else None
                        _alias_val = [] if isinstance(_first_alias, list) else ('[]' if isinstance(_first_alias, str) else None)
                        stubs = pd.DataFrame({
                            'employer_id':   missing_emp['employer_id'].values,
                            'employer_name': missing_emp['employer_name'].values,
                            'aliases':       [_alias_val] * len(missing_emp),
                            'domain':        [None] * len(missing_emp),
                            'source_files':  ['fact_perm_patch'] * len(missing_emp),
                            'ingested_at':   [now_ts] * len(missing_emp),
                        })
                        for col in df_dim_emp.columns:
                            if col not in stubs.columns:
                                stubs[col] = None
                        stubs = stubs[df_dim_emp.columns]
                        df_dim_out = pd.concat([df_dim_emp, stubs], ignore_index=True)
                        df_dim_out = df_dim_out.drop_duplicates(subset=['employer_id'], keep='first')
                        df_dim_out.to_parquet(dim_emp_path, index=False)
                        print(f"  ✓ dim_employer expanded: {len(df_dim_out):,} rows (+{len(stubs):,} stubs from fact_perm)")
                    else:
                        print(f"  ✓ dim_employer already complete ({len(df_dim_emp):,} employers)")
                except Exception as _e:
                    print(f"  ⚠ dim_employer expansion skipped: {_e}")
    except Exception as e:
        print(f"  ✗ Failed to build fact_perm: {e}")
        import traceback
        traceback.print_exc()
    
    # fact_oews (OEWS wage percentiles) - partitioned by ref_year
    print("\n[3/N] fact_oews (OEWS Wage Percentiles)")
    fact_oews_dir = resolve_artifact_path(artifacts_root, "tables", "fact_oews")
    try:
        # Processes all OEWS years with hourly→annual conversion
        build_fact_oews(Path(data_root), fact_oews_dir, Path(artifacts_root), dry_run=dry_run)
        # Verify output (skip in dry-run)
        if not dry_run and fact_oews_dir.exists():
            df_oews = pd.read_parquet(fact_oews_dir)
            print(f"  ✓ fact_oews: {len(df_oews)} rows")
            
            # Show per-year breakdown
            if 'ref_year' in df_oews.columns:
                year_counts = df_oews['ref_year'].value_counts().sort_index()
                print(f"\n  Rows per reference year:")
                for year, count in year_counts.items():
                    print(f"    {year}: {count:,}")
            
            # Show wage summary
            if 'a_median' in df_oews.columns:
                median_wages = df_oews['a_median'].dropna()
                if len(median_wages) > 0:
                    print(f"\n  Annual median wage summary:")
                    print(f"    Min: ${median_wages.min():,.0f}")
                    print(f"    Mean: ${median_wages.mean():,.0f}")
                    print(f"    Max: ${median_wages.max():,.0f}")
            print()
    except Exception as e:
        print(f"  ✗ Failed to build fact_oews: {e}")
        import traceback
        traceback.print_exc()
    
    # TODO: Add more fact loaders here (e.g., H1B)
    
    # fact_lca (LCA / H-1B labor condition applications)
    print("\n[4/N] fact_lca (LCA / H-1B)")
    try:
        lca_out = load_lca(data_root, artifacts_root, schemas_path, dry_run=dry_run)
        if not dry_run:
            lca_dir = Path(lca_out)
            if lca_dir.exists():
                parquet_files = list(lca_dir.rglob("*.parquet"))
                if parquet_files:
                    total_rows = sum(len(pd.read_parquet(f)) for f in parquet_files)
                    fy_dirs = sorted([d.name for d in lca_dir.iterdir() if d.is_dir()])
                    print(f"  ✓ fact_lca: {total_rows:,} rows, {len(fy_dirs)} FY partitions")
                    print(f"  FYs: {', '.join(fy_dirs)}")
                else:
                    print(f"  ✓ fact_lca: directory created (no data yet)")
        print()
    except Exception as e:
        print(f"  ✗ Failed to build fact_lca: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("CURATION COMPLETE")
    print(f"Output tables written to: {artifacts_root}/tables/")
    print("="*60)
    if _tap:
        _tap.intercept_chat("agent", "run_curate COMPLETE", task="curate", level="INFO")

    if not dry_run:
        # ── POST-BUILD: expand dim_employer from employer_features if available ──
        # dim_soc needs no expansion: the canonical SOC 2018 crosswalk (build_dim_soc)
        # already contains all codes used by worksite_geo (81.5%+ coverage, well above
        # the 60% test threshold).  Adding legacy SOC 2010 stub rows would break the
        # test_dim_soc_schema assertion that all rows have soc_version='2018'.
        print("\n[POST] dim_employer: checking for employers in features not in dim")
        try:
            _feat_path = resolve_artifact_path(artifacts_root, "tables", "employer_features.parquet")
            _dim_emp2_path = resolve_artifact_path(artifacts_root, "tables", "dim_employer.parquet")
            if _feat_path.exists() and _dim_emp2_path.exists():
                _df_feat2 = pd.read_parquet(_feat_path, columns=["employer_id", "employer_name"])
                _df_dim2  = pd.read_parquet(_dim_emp2_path)
                _exist2   = set(_df_dim2["employer_id"].dropna())
                _feat2_grp = (
                    _df_feat2.dropna(subset=["employer_id"])
                    .groupby("employer_id")["employer_name"]
                    .first()
                    .reset_index()
                )
                _missing2 = _feat2_grp[~_feat2_grp["employer_id"].isin(_exist2)]
                if len(_missing2) > 0:
                    _now2 = pd.Timestamp.now(tz="UTC")
                    _al2 = _df_dim2["aliases"].dropna().iloc[0] if "aliases" in _df_dim2.columns and _df_dim2["aliases"].notna().any() else None
                    _av2 = [] if isinstance(_al2, list) else ("[]" if isinstance(_al2, str) else None)
                    _stubs2 = pd.DataFrame({
                        "employer_id":   _missing2["employer_id"].values,
                        "employer_name": _missing2["employer_name"].values,
                        "aliases":       [_av2] * len(_missing2),
                        "domain":        [None] * len(_missing2),
                        "source_files":  ["feat_patch"] * len(_missing2),
                        "ingested_at":   [_now2] * len(_missing2),
                    })
                    for _c2 in _df_dim2.columns:
                        if _c2 not in _stubs2.columns:
                            _stubs2[_c2] = None
                    _stubs2 = _stubs2[_df_dim2.columns]
                    _df_dim_out2 = pd.concat([_df_dim2, _stubs2], ignore_index=True)
                    _df_dim_out2 = _df_dim_out2.drop_duplicates(subset=["employer_id"], keep="first")
                    _df_dim_out2.to_parquet(_dim_emp2_path, index=False)
                    print(f"  ✓ dim_employer expanded from features: {len(_df_dim_out2):,} rows (+{len(_stubs2):,})")
                else:
                    print(f"  ✓ dim_employer complete ({len(_df_dim2):,} employers)")
            else:
                print("  ⚠ employer_features not yet built, skipping dim_employer expansion")
        except Exception as _ee:
            print(f"  ⚠ dim_employer features expansion skipped: {_ee}")


if __name__ == "__main__":
    main()
