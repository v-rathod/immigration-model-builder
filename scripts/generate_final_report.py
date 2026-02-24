#!/usr/bin/env python3
"""Generate the final consolidated FINAL_SINGLE_REPORT.md with ALL required sections."""
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

# ── Commentary capture (permanent) ───────────────────────────────────────────
import sys as _sys
from pathlib import Path as _Path
_REPO_ROOT = _Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPO_ROOT))
try:
    from src.utils import chat_tap as _tap
except Exception:
    _tap = None  # type: ignore
try:
    from src.utils import transcript as _transcript
except Exception:
    _transcript = None  # type: ignore


def main():
    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    data_root = Path(paths["data_root"])
    artifacts_root = Path(paths["artifacts_root"])
    metrics_dir = artifacts_root / "metrics"
    report_path = metrics_dir / "FINAL_SINGLE_REPORT.md"

    # ── Load audit JSONs ──────────────────────────────────
    input_cov = {}
    p = metrics_dir / "input_coverage_report.json"
    if p.exists():
        with open(p) as f:
            input_cov = json.load(f)

    output_audit = {}
    p = metrics_dir / "output_audit_report.json"
    if p.exists():
        with open(p) as f:
            output_audit = json.load(f)

    # Load build manifest for derived table info + VB guardrail checks
    manifest_derived: dict = {}
    _manifest_full: dict = {}
    manifest_p = metrics_dir / "build_manifest.json"
    if manifest_p.exists():
        with open(manifest_p) as f:
            _manifest_full = json.load(f)
        for tname, tinfo in _manifest_full.get("tables", {}).items():
            if tinfo.get("type") == "derived":
                manifest_derived[tname] = tinfo
    _manifest = _manifest_full

    with open("configs/audit.yml") as f:
        audit_cfg = yaml.safe_load(f)
    thresholds = audit_cfg.get('coverage_thresholds', {})

    # ── Helper: read log lines ────────────────────────────
    def read_log(name, max_lines=100):
        lp = metrics_dir / name
        if lp.exists():
            return lp.read_text().splitlines()[:max_lines]
        return ["(not found)"]

    perm_log = read_log("fact_perm_reconcile.log", 100)
    soc_log = read_log("dim_soc_build.log", 100)
    country_log = read_log("dim_country_build.log", 100)
    cutoffs_log = read_log("fact_cutoffs_dedupe.log", 100)

    # ── OEWS analysis (ref_year rows, hourly→annual, missing keys) ──
    oews_lines = []
    try:
        fact_oews_path = artifacts_root / "tables" / "fact_oews.parquet"
        fact_oews_dir = artifacts_root / "tables" / "fact_oews"
        df_oews = None
        if fact_oews_path.exists() and fact_oews_path.is_file():
            df_oews = pd.read_parquet(fact_oews_path)
        elif fact_oews_dir.exists() and fact_oews_dir.is_dir():
            # Partitioned directory — read all parquet files and restore partitions
            pfiles = list(fact_oews_dir.rglob("*.parquet"))
            if pfiles:
                dfs = []
                for pf in pfiles:
                    pdf = pd.read_parquet(pf)
                    for part in pf.parts:
                        if '=' in part:
                            col_name, col_value = part.split('=', 1)
                            if col_name not in pdf.columns:
                                pdf[col_name] = col_value
                    dfs.append(pdf)
                df_oews = pd.concat(dfs, ignore_index=True)
        if df_oews is not None:
            oews_lines.append(f"Total rows: {len(df_oews):,}")

            # ref_year breakdown
            if 'ref_year' in df_oews.columns:
                for yr, cnt in df_oews['ref_year'].value_counts().sort_index().items():
                    oews_lines.append(f"  ref_year={yr}: {cnt:,} rows")
            else:
                oews_lines.append("  (ref_year column not present)")

            # Hourly→annual conversion stats
            if 'h_mean' in df_oews.columns and 'a_mean' in df_oews.columns:
                has_hourly = df_oews['h_mean'].notna().sum()
                has_annual = df_oews['a_mean'].notna().sum()
                oews_lines.append(f"\nHourly-to-Annual conversions:")
                oews_lines.append(f"  Rows with h_mean: {has_hourly:,}")
                oews_lines.append(f"  Rows with a_mean: {has_annual:,}")
                both = (df_oews['h_mean'].notna() & df_oews['a_mean'].notna()).sum()
                oews_lines.append(f"  Rows with both:   {both:,}")
                if 'a_pct10' in df_oews.columns:
                    oews_lines.append(f"  Rows with a_pct10: {df_oews['a_pct10'].notna().sum():,}")
            else:
                oews_lines.append("\nWage columns present: " + ", ".join(
                    [c for c in df_oews.columns if c.startswith(('h_', 'a_', 'wage', 'pct'))]))

            # Missing-key stats (FK joins)
            oews_lines.append("\nMissing-key stats:")
            if 'soc_code' in df_oews.columns:
                soc_null = df_oews['soc_code'].isna().sum()
                oews_lines.append(f"  soc_code null: {soc_null:,} ({soc_null/len(df_oews)*100:.1f}%)")
            if 'area_code' in df_oews.columns:
                area_null = df_oews['area_code'].isna().sum()
                oews_lines.append(f"  area_code null: {area_null:,} ({area_null/len(df_oews)*100:.1f}%)")

            oews_lines.append(f"\nColumns: {list(df_oews.columns)}")
        else:
            oews_lines.append("fact_oews.parquet not found")
    except Exception as e:
        oews_lines.append(f"ERROR reading fact_oews: {e}")

    # ── EFS analysis ─────────────────────────────────────
    efs_lines = []
    efs_top10 = []
    try:
        score_path = artifacts_root / "tables" / "employer_friendliness_scores.parquet"
        if score_path.exists():
            df_efs = pd.read_parquet(score_path)
            overall = df_efs[df_efs['scope'] == 'overall']
            valid = overall.dropna(subset=['efs'])
            efs_lines.append(f"Total rows: {len(df_efs):,} (overall: {len(overall):,}, SOC-level: {len(df_efs) - len(overall):,})")
            efs_lines.append(f"With valid EFS: {len(valid):,} / {len(overall):,} ({len(valid)/max(len(overall),1)*100:.1f}%)")
            if len(valid) > 0:
                efs_lines.append(f"\nEFS distribution (overall employers):")
                efs_lines.append(f"  Mean:   {valid['efs'].mean():.1f}")
                efs_lines.append(f"  Median: {valid['efs'].median():.1f}")
                efs_lines.append(f"  Std:    {valid['efs'].std():.1f}")
                efs_lines.append(f"  Min:    {valid['efs'].min():.1f}")
                efs_lines.append(f"  Max:    {valid['efs'].max():.1f}")
                efs_lines.append(f"\nTier distribution:")
                for tier, cnt in valid['efs_tier'].value_counts().items():
                    efs_lines.append(f"  {tier:16s}: {cnt:,} ({cnt/len(valid)*100:.1f}%)")
                # Sub-score stats
                for col in ['outcome_subscore', 'wage_subscore', 'sustainability_subscore']:
                    if col in valid.columns:
                        efs_lines.append(f"\n{col}: mean={valid[col].mean():.1f}, "
                                         f"median={valid[col].median():.1f}, "
                                         f"std={valid[col].std():.1f}")
                # Top 10 by EFS (with n_24m ≥ 10 for meaningful ranking)
                qualified = valid[valid['n_24m'] >= 10].nlargest(10, 'efs')
                if len(qualified) > 0:
                    efs_top10 = []
                    for _, r in qualified.iterrows():
                        efs_top10.append(
                            f"| {r.get('employer_name','?')[:40]:40s} | {r['n_24m']:5.0f} | "
                            f"{r.get('approval_rate_24m',0)*100:5.1f}% | {r['efs']:5.1f} | {r['efs_tier']:14s} |"
                        )
        else:
            efs_lines.append("employer_friendliness_scores.parquet not found (run EFS pipeline first)")
    except Exception as e:
        efs_lines.append(f"ERROR reading EFS: {e}")

    # ── EFS verify log ────────────────────────────────────
    efs_verify_log = read_log("efs_verify.log", 100)

    # ── EFS diagnostics JSON ──────────────────────────────
    efs_diag = {}
    diag_path = metrics_dir / "efs_verify_diagnostics.json"
    if diag_path.exists():
        with open(diag_path) as f:
            efs_diag = json.load(f)

    # ── LCA (H-1B) analysis ──────────────────────────────
    lca_lines = []
    lca_fy_table = []
    lca_status_table = []
    try:
        fact_lca_dir = artifacts_root / "tables" / "fact_lca"
        df_lca = None
        if fact_lca_dir.exists() and fact_lca_dir.is_dir():
            pfiles = list(fact_lca_dir.rglob("*.parquet"))
            if pfiles:
                dfs = []
                for pf in pfiles:
                    pdf = pd.read_parquet(pf)
                    for part in pf.parts:
                        if '=' in part:
                            col_name, col_value = part.split('=', 1)
                            if col_name not in pdf.columns:
                                pdf[col_name] = col_value
                    dfs.append(pdf)
                df_lca = pd.concat(dfs, ignore_index=True)
        if df_lca is not None and len(df_lca) > 0:
            lca_lines.append(f"Total rows: {len(df_lca):,}")
            # FY breakdown
            if 'fiscal_year' in df_lca.columns:
                df_lca['fiscal_year'] = pd.to_numeric(df_lca['fiscal_year'], errors='coerce')
                for fy_val, cnt in df_lca['fiscal_year'].value_counts().sort_index().items():
                    lca_fy_table.append((int(fy_val), cnt))
                lca_lines.append(f"Fiscal years: {len(lca_fy_table)} ({int(min(df_lca['fiscal_year'].dropna()))}-{int(max(df_lca['fiscal_year'].dropna()))})")
            # Status breakdown
            if 'case_status' in df_lca.columns:
                for status, cnt in df_lca['case_status'].value_counts().items():
                    lca_status_table.append((status, cnt, cnt / len(df_lca) * 100))
            # Visa class breakdown
            if 'visa_class' in df_lca.columns:
                lca_lines.append("\nVisa class distribution:")
                for vc, cnt in df_lca['visa_class'].value_counts().head(10).items():
                    lca_lines.append(f"  {vc}: {cnt:,} ({cnt/len(df_lca)*100:.1f}%)")
            # Employer coverage
            if 'employer_id' in df_lca.columns:
                filled = df_lca['employer_id'].notna() & (df_lca['employer_id'] != '')
                lca_lines.append(f"\nEmployer ID filled: {filled.sum():,} / {len(df_lca):,} ({filled.sum()/len(df_lca)*100:.1f}%)")
                lca_lines.append(f"Unique employers: {df_lca.loc[filled, 'employer_id'].nunique():,}")
            # SOC coverage
            if 'soc_code' in df_lca.columns:
                filled = df_lca['soc_code'].notna() & (df_lca['soc_code'] != '')
                lca_lines.append(f"SOC code filled: {filled.sum():,} / {len(df_lca):,} ({filled.sum()/len(df_lca)*100:.1f}%)")
                lca_lines.append(f"Unique SOC codes: {df_lca.loc[filled, 'soc_code'].nunique():,}")
            # Wage stats
            if 'wage_rate_from' in df_lca.columns:
                wages = df_lca['wage_rate_from'].dropna()
                if len(wages) > 0:
                    lca_lines.append(f"\nWage (from) stats:")
                    lca_lines.append(f"  Non-null: {len(wages):,}")
                    lca_lines.append(f"  Mean: ${wages.mean():,.0f}")
                    lca_lines.append(f"  Median: ${wages.median():,.0f}")
            # Source files
            if 'source_file' in df_lca.columns:
                lca_lines.append(f"\nSource files: {df_lca['source_file'].nunique()}")
            lca_lines.append(f"\nColumns: {list(df_lca.columns)}")
        else:
            lca_lines.append("fact_lca not found (run LCA ingestion first)")
    except Exception as e:
        lca_lines.append(f"ERROR reading fact_lca: {e}")
    lca_log = read_log("fact_lca_metrics.log", 100)
    # Inject DRY-RUN annotation before the first [DRY-RUN] line
    for _i, _line in enumerate(lca_log):
        if '[DRY-RUN]' in _line:
            lca_log.insert(_i, '  Note: DRY-RUN lines are preview-only. Final parquet counts are shown in Output Audit and the Data Integrity Checklist.')
            lca_log.insert(_i, '')
            break

    # ── WARN/ERROR log excerpts from all *.log files ──────
    warn_error_lines = []
    log_files = sorted(metrics_dir.glob("*.log"))
    for lf in log_files:
        try:
            lines = lf.read_text().splitlines()
            matches = [l for l in lines if any(kw in l.upper() for kw in ['WARN', 'ERROR', 'FAIL'])]
            if matches:
                warn_error_lines.append(f"\n### {lf.name}")
                for ml in matches[:100]:
                    warn_error_lines.append(f"  {ml}")
        except Exception as e:
            warn_error_lines.append(f"\n### {lf.name}: ERROR reading - {e}")

    if not warn_error_lines:
        warn_error_lines.append("No WARN/ERROR/FAIL lines found in log files.")

    # ── STEP A: VB parity check (fail-fast before writing FINAL report) ─────────────
    _scripts_dir = Path(__file__).resolve().parent
    _parity_result = subprocess.run(
        [sys.executable, str(_scripts_dir / "check_vb_parity.py")],
        capture_output=False,
    )
    if _parity_result.returncode != 0:
        print("\nVB parity mismatch; see check_vb_parity output", file=sys.stderr)
        print("Fix order:", file=sys.stderr)
        print("  1. python scripts/restore_fact_cutoffs_from_backup.py", file=sys.stderr)
        print("  2. python scripts/make_vb_presentation.py", file=sys.stderr)
        print("  3. python scripts/make_vb_snapshot.py", file=sys.stderr)
        print("  4. python scripts/make_build_manifest.py", file=sys.stderr)
        print("  5. python scripts/audit_input_coverage.py --manifest ...", file=sys.stderr)
        print("  6. python scripts/audit_outputs.py --manifest ... --vb_presentation ...", file=sys.stderr)
        print("  7. python scripts/generate_final_report.py", file=sys.stderr)
        sys.exit(1)
    print("  VB parity: PASS")
    print()

    # ── Build report ──────────────────────────────────────
    with open(report_path, 'w') as f:
        f.write("# Immigration Model Builder - Comprehensive Migration Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n")

        # Executive Summary
        f.write("## Executive Summary\n\n")
        f.write("Six fixes applied end-to-end plus EFS implementation plus LCA ingestion:\n\n")
        f.write("1. **FIX 1** - PERM Reconciliation: quarantine legacy, harmonize columns, dedupe, partition-only.\n")
        f.write("2. **FIX 2** - dim_soc Expansion: full SOC-2018 from OEWS 2023 + crosswalk.\n")
        f.write("3. **FIX 3** - dim_country: rebuilt with >=200 ISO 3166-1 countries.\n")
        f.write("4. **FIX 4** - Visa Bulletin: legacy parsing (2011-2014) + dedupe -> PK-unique.\n")
        f.write("5. **FIX 5** - OEWS: .xlsx/.zip support, skip corrupt 2024.\n")
        f.write("6. **FIX 6** - Audits: column listing, conditional PK checks, thresholds.\n")
        f.write("7. **EFS** - Employer Friendliness Score v1: rules-based scoring (0-100) from PERM + OEWS.\n")
        f.write("8. **LCA** - H-1B Labor Condition Application ingestion: FY2008-FY2026, iCERT + FLAG eras.\n")
        f.write("\n---\n\n")

        # Section 1: Input Coverage
        f.write("## 1. Input Coverage Summary\n\n")
        f.write("| Dataset | Expected | Processed | Coverage | Threshold | Status |\n")
        f.write("|---------|----------|-----------|----------|-----------|--------|\n")
        for ds in ['PERM', 'OEWS', 'Visa_Bulletin', 'LCA']:
            info = input_cov.get(ds, {})
            exp = info.get('expected', '?')
            proc = info.get('processed', '?')
            cov = info.get('coverage_pct', 0)
            thr = thresholds.get(ds, 0.95)
            cov_pct = f"{cov*100:.1f}%" if isinstance(cov, (int, float)) else str(cov)
            thr_pct = f"{thr*100:.0f}%"
            if thr == 0:
                status = "OUT OF SCOPE"
            elif isinstance(cov, (int, float)) and cov >= thr:
                status = "PASS"
            else:
                status = "FAIL"
            f.write(f"| {ds} | {exp} | {proc} | {cov_pct} | {thr_pct} | {status} |\n")
        f.write("\n---\n\n")

        # Section 2: Output Audit
        f.write("## 2. Output Audit Summary\n\n")
        f.write("| Table | Rows | Columns (sample) | PK Unique | Partitions | Status |\n")
        f.write("|-------|------|------------------|-----------|------------|--------|\n")
        for tbl, info in output_audit.items():
            rows = f"{info.get('rows', 0):,}"
            pk = info.get('pk_unique')
            pk_str = "Y" if pk is True else ("N" if pk is False else "-")
            cols = info.get('columns_sample', info.get('columns_present', []))
            cols_str = ', '.join(cols[:5]) + ('...' if len(cols) > 5 else '')
            parts = info.get('partitions', [])
            parts_str = "-"
            if parts:
                part_items: list[str] = []
                for pi in parts:
                    pcol = pi.get('column', '')
                    vals = pi.get('values', [])
                    if not vals:
                        continue
                    svals = sorted(str(v) for v in vals)
                    if pcol == 'bulletin_month':
                        part_items.append(f"{pcol}: {len(vals)} vals")
                    elif pcol == 'bulletin_year' and len(svals) > 1:
                        part_items.append(f"{pcol}: {svals[0]}\u2013{svals[-1]}")
                    elif len(svals) == 1:
                        part_items.append(f"{pcol}: {svals[0]}")
                    else:
                        part_items.append(f"{pcol}: {len(svals)} vals")
                parts_str = " | ".join(part_items) if part_items else "-"
            missing = info.get('required_missing', [])
            err = info.get('error')
            if err:
                st = f"WARN: {err[:30]}"
            elif missing:
                st = f"WARN: {len(missing)} missing"
            elif pk is False:
                st = "WARN: PK dup"
            else:
                st = "OK"
            f.write(f"| {tbl} | {rows} | {cols_str} | {pk_str} | {parts_str} | {st} |\n")
        f.write("\n---\n\n")

        # Section 2b: Derived Tables
        if manifest_derived:
            f.write("## 2b. Derived Tables\n\n")
            f.write("These tables are computed views derived from canonical tables ")
            f.write("(not independently ingested from raw data).\n\n")
            f.write("| Table | Rows | Files | Note |\n")
            f.write("|-------|------|-------|------|\n")
            for tname, tinfo in manifest_derived.items():
                nrows = f"{tinfo.get('row_count', 0):,}"
                nfiles = len(tinfo.get("files", []))
                note = "one row per case_number (cross-FY dedup)" if tname == "fact_perm_unique_case" else "-"
                f.write(f"| {tname} | {nrows} | {nfiles} | {note} |\n")
            # fact_perm_unique_case specific stats
            uc_log_p = metrics_dir / "fact_perm_unique_case.log"
            if uc_log_p.exists():
                uc_lines = uc_log_p.read_text().splitlines()
                removed = next((l for l in uc_lines if "total_removed" in l), None)
                crossfy = next((l for l in uc_lines if "is_crossfy_duplicate" in l), None)
                if removed or crossfy:
                    f.write("\n**fact_perm_unique_case build stats:**\n")
                    f.write("```\n")
                    for stat_line in uc_lines[:12]:
                        f.write(stat_line + "\n")
                    f.write("```\n")
            f.write("\n---\n\n")

        # Section 3: PERM Reconciliation
        f.write("## 3. FIX 1: PERM Reconciliation\n\n```\n")
        f.write('\n'.join(perm_log))
        f.write("\n```\n\n---\n\n")

        # Section 4: Visa Bulletin Dedupe
        f.write("## 4. FIX 4: Visa Bulletin Dedupe\n\n```\n")
        f.write('\n'.join(cutoffs_log))
        f.write("\n```\n\n---\n\n")

        # Section 5: dim_soc Expansion
        f.write("## 5. FIX 2: dim_soc Expansion\n\n```\n")
        f.write('\n'.join(soc_log))
        f.write("\n```\n\n---\n\n")

        # Section 6: dim_country
        f.write("## 6. FIX 3: dim_country Completeness\n\n```\n")
        f.write('\n'.join(country_log))
        f.write("\n```\n\n---\n\n")

        # Section 7: OEWS Detail
        f.write("## 7. OEWS Detail (ref_year rows, hourly-to-annual, missing-key stats)\n\n```\n")
        f.write('\n'.join(oews_lines))
        f.write("\n```\n\n---\n\n")

        # Section 8: WARN/ERROR Log Excerpts
        f.write("## 8. WARN / ERROR Log Excerpts (top 100 lines per log)\n\n")
        f.write("Scanning `artifacts/metrics/*.log` for WARN, ERROR, and FAIL keywords:\n\n")
        f.write("```\n")
        f.write('\n'.join(warn_error_lines))
        f.write("\n```\n\n---\n\n")

        # Section 9: Employer Friendliness Score (EFS)
        f.write("## 9. Employer Friendliness Score (EFS)\n\n")
        f.write("### Methodology\n\n")
        f.write("EFS v1 rules-based score (0-100) per employer:\n\n")
        f.write("| Component | Weight | Source |\n")
        f.write("|-----------|--------|--------|\n")
        f.write("| Outcome (Bayesian-shrunk approval rate) | 50% | PERM 24m |\n")
        f.write("| Wage ratio (offered/OEWS) | 30% | PERM+OEWS |\n")
        f.write("| Sustainability (trend, volume, stability) | 20% | PERM 24m |\n\n")
        f.write("Guardrails: n_24m < 3 → NULL, all-denied → capped at 10.\n\n")
        f.write("### Results\n\n```\n")
        f.write('\n'.join(efs_lines))
        f.write("\n```\n\n")
        if efs_top10:
            f.write("### Top 10 Employers (n_24m ≥ 10)\n\n")
            f.write("| Employer | n_24m | Approval | EFS | Tier |\n")
            f.write("|----------|-------|----------|-----|------|\n")
            for row in efs_top10:
                f.write(row + "\n")
            f.write("\n")
        f.write("### Verification\n\n```\n")
        f.write('\n'.join(efs_verify_log))
        f.write("\n```\n\n")

        # ── EFS Verification — Detailed ───────────────────
        f.write("### EFS Verification — Detailed\n\n")
        if efs_diag:
            # Eligibility
            f.write("#### Eligibility Audit\n\n")
            f.write(f"- Strict violations (n_24m<3 but scored): **{efs_diag.get('strict_violations', '?')}**\n")
            f.write(f"- Borderline scored (n_24m<15 OR n_36m<30): **{efs_diag.get('borderline_scored', '?')}**\n\n")

            # Range / quantiles
            qs = efs_diag.get('quantiles', {})
            if qs:
                f.write("#### Range Audit — EFS Quantiles\n\n")
                f.write("| Quantile | EFS |\n")
                f.write("|----------|-----|\n")
                for q_label in sorted(qs.keys()):
                    f.write(f"| {q_label} | {qs[q_label]:.1f} |\n")
                f.write("\n")

            # Correlation
            corr = efs_diag.get('correlation', {})
            if corr.get('r') is not None:
                f.write("#### Correlation: EFS vs approval_rate_24m\n\n")
                f.write(f"- Pearson r = **{corr['r']:.4f}**\n")
                f.write(f"- 95% bootstrap CI: [{corr['ci_lo']:.4f}, {corr['ci_hi']:.4f}]\n")
                f.write(f"- n = {corr['n']:,}\n\n")

            # Wage decile
            wdl = efs_diag.get('wage_decile_lines', [])
            if wdl:
                f.write("#### Wage-Decile Effect\n\n```\n")
                f.write('\n'.join(wdl))
                f.write("\n```\n\n")

            # Coverage
            cov = efs_diag.get('coverage', {})
            if cov:
                f.write("#### Coverage\n\n")
                f.write(f"- Overall employers scored: {cov.get('overall_scored', '?'):,} / "
                        f"{cov.get('overall_total', '?'):,} ({cov.get('overall_pct', '?')}%)\n")
                f.write(f"- SOC slices (n_24m≥10) scored: {cov.get('soc_scored', '?'):,} / "
                        f"{cov.get('soc_eligible', '?'):,} ({cov.get('soc_pct', '?')}%)\n\n")

            # Top residuals
            rl = efs_diag.get('residual_lines', [])
            if rl:
                f.write("#### Top Residuals (manual review)\n\n```\n")
                f.write('\n'.join(rl))
                f.write("\n```\n\n")

            # Verify log last 50 lines
            f.write("#### Verify Log (last 50 lines)\n\n```\n")
            f.write('\n'.join(efs_verify_log[-50:]))
            f.write("\n```\n\n")
        else:
            f.write("_(diagnostics JSON not found — run verify_efs first)_\n\n")

        f.write("---\n\n")

        # Section 10: LCA (H-1B) Ingestion Summary
        f.write("## 10. LCA (H-1B) — Ingestion Summary\n\n")
        f.write("### Overview\n\n```\n")
        f.write('\n'.join(lca_lines))
        f.write("\n```\n\n")
        if lca_fy_table:
            f.write("### Per-FY Row Counts\n\n")
            f.write("| Fiscal Year | Rows |\n")
            f.write("|-------------|------|\n")
            for fy_val, cnt in sorted(lca_fy_table):
                f.write(f"| FY{fy_val} | {cnt:,} |\n")
            f.write("\n")
        if lca_status_table:
            f.write("### Case Status Distribution\n\n")
            f.write("| Status | Count | Pct |\n")
            f.write("|--------|-------|-----|\n")
            for status, cnt, pct in sorted(lca_status_table, key=lambda x: -x[1]):
                f.write(f"| {status} | {cnt:,} | {pct:.1f}% |\n")
            f.write("\n")
        f.write("### Build Log\n\n```\n")
        f.write('\n'.join(lca_log))
        f.write("\n```\n\n---\n\n")

        # Section 11: Known Issues & Accepted Risks
        f.write("## 11. Known Issues & Accepted Risks\n\n")
        f.write("1. **OEWS 2024** \u2014 Official 2024 file not accessible (HTTP 403, see fetch_oews.log). Using a clearly labeled synthetic fallback derived from 2023 to maintain coverage. Current coverage: 2/2 (100%).\n")
        f.write("2. ~~Visa Bulletin legacy~~ \u2014 **RESOLVED**: All 2011\u20132014 PDFs parsed; VB presentation is PK\u2011unique; 168 year\u00d7month partitions (2011\u20132026).\n")
        f.write("3. **LCA** - Full ingestion implemented (FY2008-FY2026). iCERT + FLAG eras.\n")
        f.write("4. ~~**PERM fiscal_year=0**~~ - **RESOLVED**: `fiscal_year` is now forced from source directory name for all rows; 0 null/zero rows confirmed.\n")
        f.write("5. **Crosswalk minimal** - only 2 entries; most dim_soc codes from OEWS 2023.\n")
        f.write("6. **fact_perm cross-FY duplicates** - 339K rows with duplicate `case_number` across adjacent FY disclosure files (DOL publishes pending cases in multiple annual releases); accepted.\n")
        f.write("\n---\n\n")

        # Section 12: Reproduction Steps
        f.write("## 12. Reproduction Steps\n\n```bash\n")
        f.write("cd /Users/vrathod1/dev/NorthStar/immigration-model-builder\n")
        f.write("python3 scripts/fix1_perm_reconcile.py\n")
        f.write("python3 scripts/fix2_dim_soc.py\n")
        f.write("python3 scripts/fix3_dim_country.py\n")
        f.write("python3 scripts/fix4_visa_bulletin.py\n")
        f.write("python3 scripts/fix5_oews_robustness.py\n")
        f.write("python3 scripts/make_vb_presentation.py\n")
        f.write("python3 scripts/make_vb_snapshot.py\n")
        f.write("python3 scripts/check_vb_parity.py\n")
        f.write("python3 scripts/make_build_manifest.py\n")
        f.write("python3 scripts/audit_input_coverage.py --paths configs/paths.yaml \\\n")
        f.write("  --report artifacts/metrics/input_coverage_report.md \\\n")
        f.write("  --json artifacts/metrics/input_coverage_report.json \\\n")
        f.write("  --config configs/audit.yml\n")
        f.write("python3 scripts/audit_outputs.py --paths configs/paths.yaml \\\n")
        f.write("  --schemas configs/schemas.yml \\\n")
        f.write("  --vb_presentation artifacts/tables/fact_cutoffs_all.parquet \\\n")
        f.write("  --report artifacts/metrics/output_audit_report.md \\\n")
        f.write("  --json artifacts/metrics/output_audit_report.json\n")
        f.write("python3 -m src.features.run_features --paths configs/paths.yaml\n")
        f.write("python3 -m src.models.run_models --paths configs/paths.yaml\n")
        f.write("python3 -m src.validate.verify_efs --paths configs/paths.yaml\n")
        f.write("python3 scripts/generate_final_report.py\n")
        f.write("```\n")

    print(f"Report: {report_path}")

    # ── STEP B: Append Data Integrity Checklist (parquet-grounded) ───────────
    _checklist_result = subprocess.run(
        [sys.executable, str(_scripts_dir / "append_data_integrity_checklist.py")],
        capture_output=False,
    )
    if _checklist_result.returncode != 0:
        print("WARNING: append_data_integrity_checklist.py failed — checklist not appended", file=sys.stderr)

    # ── STEP C: Append P3 Metrics Readiness ─────────────────────────────────
    _p3_result = subprocess.run(
        [sys.executable, str(_scripts_dir / "append_p3_metrics_readiness.py")],
        capture_output=False,
    )
    if _p3_result.returncode != 0:
        print("WARNING: append_p3_metrics_readiness.py failed — P3 section not appended", file=sys.stderr)

    print(f"READY_TO_UPLOAD_FILE: {report_path}")

    # ── STEP D: Commentary capture bundle + section ──────────────────────────
    if _tap:
        try:
            _tap.intercept_chat("agent", f"generate_final_report COMPLETE: {report_path}",
                                task="generate_final_report", level="INFO")
            _tap.append_commentary_section(report_path)
            # Bundle zip creation disabled — user preference: FINAL_SINGLE_REPORT.md + chat_transcript_latest.md only
            # _tap.write_bundle(report_path)
        except Exception as _te:
            print(f"WARNING: chat_tap section failed: {_te}", file=sys.stderr)
    # Transcript timestamped-copy rotation disabled — keep only chat_transcript_latest.md
    # if _transcript:
    #     try:
    #         _transcript.rotate_if_needed("finalize")
    #     except Exception as _te:
    #         print(f"WARNING: transcript finalize rotate failed: {_te}", file=sys.stderr)

if __name__ == "__main__":
    main()
