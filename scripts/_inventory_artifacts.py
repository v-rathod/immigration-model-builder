#!/usr/bin/env python3
"""Generate a comprehensive inventory of all P2 artifacts with row counts,
columns, data sources, and P3 usage mapping."""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

ROOT = Path("artifacts")
TABLES = ROOT / "tables"
MODELS = ROOT / "models"

results = []

def inspect(name, path, category, p3_usage, sources):
    """Inspect a single artifact and record metadata."""
    info = {"name": name, "category": category, "p3_usage": p3_usage, "sources": sources}
    if isinstance(path, Path) and path.is_dir():
        # Partitioned parquet
        parts = list(path.glob("**/*.parquet"))
        if parts:
            try:
                df = pd.read_parquet(path)
                info["rows"] = len(df)
                info["cols"] = len(df.columns)
                info["columns"] = df.columns.tolist()
                info["partitions"] = len(parts)
            except Exception as e:
                # Schema merge error (e.g., fact_lca)
                total = 0
                cols = set()
                for p in parts:
                    try:
                        sub = pd.read_parquet(p)
                        total += len(sub)
                        cols.update(sub.columns.tolist())
                    except:
                        pass
                info["rows"] = total
                info["cols"] = len(cols)
                info["columns"] = sorted(cols)
                info["partitions"] = len(parts)
                info["note"] = f"schema merge error: {e}"
        else:
            info["rows"] = 0
            info["cols"] = 0
            info["partitions"] = 0
    elif isinstance(path, Path) and path.exists() and path.suffix == ".parquet":
        try:
            df = pd.read_parquet(path)
            info["rows"] = len(df)
            info["cols"] = len(df.columns)
            info["columns"] = df.columns.tolist()
        except Exception as e:
            info["rows"] = 0
            info["note"] = str(e)
    elif isinstance(path, Path) and path.exists() and path.suffix == ".json":
        with open(path) as f:
            data = json.load(f)
        info["rows"] = len(data.get("series", []))
        info["format"] = "JSON"
    else:
        info["rows"] = 0
        info["note"] = "not found"
    results.append(info)

# ── Dimension Tables ─────────────────────────────────────────────
inspect("dim_employer.parquet", TABLES / "dim_employer.parquet",
    "Dimension", "Employer lookup, EFS dashboard, search",
    "fact_perm (all 20 FY partitions via patch script)")

inspect("dim_soc.parquet", TABLES / "dim_soc.parquet",
    "Dimension", "SOC occupation lookup, demand dashboard",
    "BLS SOC-2018 + crosswalk files")

inspect("dim_country.parquet", TABLES / "dim_country.parquet",
    "Dimension", "Country lookup, visa bulletin filter",
    "ISO 3166-1 country list")

inspect("dim_area.parquet", TABLES / "dim_area.parquet",
    "Dimension", "Geography lookup, worksite dashboard",
    "BLS OEWS area definitions")

inspect("dim_visa_class.parquet", TABLES / "dim_visa_class.parquet",
    "Dimension", "Visa category labels for UI",
    "Manual EB1-EB5 category definitions")

inspect("dim_visa_ceiling.parquet", TABLES / "dim_visa_ceiling.parquet",
    "Dimension", "Annual visa limits context, backlog estimates",
    "INA statutory limits + DOS adjustments")

# ── Fact Tables ──────────────────────────────────────────────────
inspect("fact_perm/", TABLES / "fact_perm",
    "Fact", "EFS input, employer features, approval rates, wage analysis",
    "DOL PERM Excel files FY2005-FY2024 (20 files)")

inspect("fact_perm_all.parquet", TABLES / "fact_perm_all.parquet",
    "Fact", "Flat denormalized copy of fact_perm for quick queries",
    "DOL PERM (same as fact_perm, non-partitioned)")

inspect("fact_perm_unique_case/", TABLES / "fact_perm_unique_case",
    "Fact", "Deduplicated PERM cases (note: ~20% dupe case_numbers)",
    "fact_perm deduplicated by case_number")

inspect("fact_cutoffs/", TABLES / "fact_cutoffs",
    "Fact", "PD forecast model input, visa bulletin dashboard",
    "DOS Visa Bulletin PDFs 2011-2026 (pdfplumber)")

inspect("fact_cutoffs_all.parquet", TABLES / "fact_cutoffs_all.parquet",
    "Fact", "Deduplicated cutoff presentation copy",
    "fact_cutoffs deduplicated")

inspect("fact_lca/", TABLES / "fact_lca",
    "Fact", "H-1B LCA filing analysis, employer H-1B volume",
    "DOL LCA disclosure Excel files")

inspect("fact_oews/", TABLES / "fact_oews",
    "Fact", "Salary benchmarks, prevailing wage context",
    "BLS OEWS Excel files 2022-2024")

inspect("fact_oews.parquet", TABLES / "fact_oews.parquet",
    "Fact", "Flat copy of OEWS wages",
    "BLS OEWS (non-partitioned)")

inspect("fact_niv_issuance.parquet", TABLES / "fact_niv_issuance.parquet",
    "Fact", "Nonimmigrant visa issuance trends dashboard",
    "DOS nonimmigrant visa issuance statistics")

inspect("fact_visa_issuance.parquet", TABLES / "fact_visa_issuance.parquet",
    "Fact", "Immigrant visa issuance trends dashboard",
    "DOS immigrant visa issuance statistics")

inspect("fact_visa_applications.parquet", TABLES / "fact_visa_applications.parquet",
    "Fact", "Visa demand metrics, application trends",
    "DOS visa application statistics")

inspect("fact_dhs_admissions.parquet", TABLES / "fact_dhs_admissions.parquet",
    "Fact", "Immigration volume trends dashboard",
    "DHS admissions summary data")

inspect("fact_uscis_approvals.parquet", TABLES / "fact_uscis_approvals.parquet",
    "Fact", "USCIS processing trends, approval volumes",
    "USCIS form approval count data")

inspect("fact_warn_events.parquet", TABLES / "fact_warn_events.parquet",
    "Fact", "Employer risk signals, layoff events",
    "WARN Act layoff notification data")

inspect("fact_waiting_list.parquet", TABLES / "fact_waiting_list.parquet",
    "Fact", "Visa demand context",
    "DOS waiting list summary")

inspect("fact_trac_adjudications.parquet", TABLES / "fact_trac_adjudications.parquet",
    "Fact (stub)", "TRAC adjudication data — placeholder",
    "TRAC (requires subscription — no data)")

inspect("fact_acs_wages.parquet", TABLES / "fact_acs_wages.parquet",
    "Fact (stub)", "Census wage benchmarks — placeholder",
    "Census ACS API (HTTP 404 — no data)")

# ── Feature Tables ───────────────────────────────────────────────
inspect("employer_features.parquet", TABLES / "employer_features.parquet",
    "Feature", "Input to EFS models, employer analytics",
    "fact_perm aggregated: approval rates, wage ratios, volume, trends")

inspect("salary_benchmarks.parquet", TABLES / "salary_benchmarks.parquet",
    "Feature", "Salary comparison dashboard, wage context",
    "fact_oews + fact_perm → SOC × area median & P75 wages")

inspect("employer_monthly_metrics.parquet", TABLES / "employer_monthly_metrics.parquet",
    "Feature", "Employer trend charts, monthly filing volume",
    "fact_perm monthly aggregation per employer")

inspect("employer_risk_features.parquet", TABLES / "employer_risk_features.parquet",
    "Feature", "Employer risk signals panel",
    "employer_features filtered to high-volume + risk indicators")

inspect("soc_demand_metrics.parquet", TABLES / "soc_demand_metrics.parquet",
    "Feature", "Occupation demand dashboard, SOC trends",
    "fact_perm + fact_lca → 3 windows × 2 datasets")

inspect("visa_demand_metrics.parquet", TABLES / "visa_demand_metrics.parquet",
    "Feature", "Visa demand by category × country dashboard",
    "fact_cutoffs + fact_visa_applications aggregated")

inspect("worksite_geo_metrics.parquet", TABLES / "worksite_geo_metrics.parquet",
    "Feature", "Geographic distribution dashboard, worksite heatmap",
    "fact_perm worksites → state/metro/city grains + competitiveness_ratio")

inspect("category_movement_metrics.parquet", TABLES / "category_movement_metrics.parquet",
    "Feature", "Visa bulletin movement trends dashboard",
    "fact_cutoffs aggregated movement by category × country")

inspect("backlog_estimates.parquet", TABLES / "backlog_estimates.parquet",
    "Feature", "Backlog context for PD forecast, wait time estimates",
    "fact_cutoffs + fact_perm → estimated queue depth")

inspect("fact_cutoff_trends.parquet", TABLES / "fact_cutoff_trends.parquet",
    "Feature", "PD forecast model input, cutoff movement analysis",
    "fact_cutoffs → monthly velocity, retrogression flags")

inspect("processing_times_trends.parquet", TABLES / "processing_times_trends.parquet",
    "Feature", "Processing times dashboard, I-485 trends",
    "USCIS I-485 quarterly processing data FY2014-FY2025")

# ── Model Outputs ────────────────────────────────────────────────
inspect("employer_friendliness_scores.parquet", TABLES / "employer_friendliness_scores.parquet",
    "Model Output", "EFS dashboard Panel D, employer search, tier labels",
    "employer_features → rules-based: 50% outcome + 30% wage + 20% sustainability")

inspect("employer_friendliness_scores_ml.parquet", TABLES / "employer_friendliness_scores_ml.parquet",
    "Model Output", "ML EFS for top employers, enhanced accuracy",
    "fact_perm case-level → gradient boosting + calibration")

inspect("pd_forecasts.parquet", TABLES / "pd_forecasts.parquet",
    "Model Output", "PD forecast Panel A — the #1 P3 feature",
    "fact_cutoff_trends → v2.1 long-term anchored seasonal model (56 series)")

inspect("pd_forecast_model.json", MODELS / "pd_forecast_model.json",
    "Model Params", "PD forecast model parameters and metadata",
    "Fitted from fact_cutoff_trends (v2.1)")

inspect("employer_scores.parquet", TABLES / "employer_scores.parquet",
    "Model Output (stub)", "Legacy — superseded by EFS",
    "N/A")

inspect("oews_wages.parquet", TABLES / "oews_wages.parquet",
    "Model Output (stub)", "Legacy — data is in fact_oews",
    "N/A")

inspect("visa_bulletin.parquet", TABLES / "visa_bulletin.parquet",
    "Model Output (stub)", "Legacy — data is in fact_cutoffs",
    "N/A")

# ── Print results ────────────────────────────────────────────────
print("=" * 100)
print("COMPLETE P2 ARTIFACT INVENTORY")
print(f"Generated: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}")
print("=" * 100)

for cat in ["Dimension", "Fact", "Fact (stub)", "Feature", "Model Output", "Model Params", "Model Output (stub)"]:
    items = [r for r in results if r["category"] == cat]
    if not items:
        continue
    print(f"\n{'─' * 100}")
    print(f"  {cat.upper()} {'TABLES' if 'stub' not in cat else '(STUBS/EMPTY)'}")
    print(f"{'─' * 100}")
    print(f"  {'Artifact':<45} {'Rows':>10} {'Cols':>5} {'P3 Usage'}")
    print(f"  {'─'*44} {'─'*10} {'─'*5} {'─'*40}")
    for r in items:
        rows = f"{r.get('rows', 0):,}" if r.get("rows", 0) > 0 else "0"
        cols = str(r.get("cols", "")) if r.get("cols") else ""
        parts = f" ({r['partitions']}p)" if r.get("partitions") else ""
        fmt = r.get("format", "")
        name = r["name"]
        usage = r["p3_usage"][:55]
        print(f"  {name:<45} {rows:>10}{parts:>5} {cols:>5} {usage}")

print(f"\n{'─' * 100}")
print(f"  TOTAL: {len(results)} artifacts")
total_rows = sum(r.get("rows", 0) for r in results)
print(f"  TOTAL ROWS: {total_rows:,}")
print()
