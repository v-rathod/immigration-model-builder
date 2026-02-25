"""
QA Generator — Pre-compute answers to common immigration questions.

Reads Meridian artifacts and generates a Q&A cache that Compass (P3) can
serve directly or use as grounding context for an LLM. This dramatically
reduces LLM token usage and latency for frequent questions.

Output: artifacts/rag/qa_cache.json

Each entry:
  {
    "question": "What is the EB2 India wait time?",
    "answer": "Based on the latest visa bulletin...",
    "sources": ["pd_forecasts.parquet", "fact_cutoffs_all.parquet"],
    "topic": "pd_forecast",
    "confidence": "high",
    "generated_at": "2026-02-24T..."
  }

Budget rationale:
  - 100 visitors × 5 questions each → 80% are common questions
  - Pre-computed answers avoid LLM calls entirely for these
  - Only novel/complex questions need LLM inference (~$0.15/mo)
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ARTIFACTS_ROOT = Path("artifacts/tables")
RAG_ROOT = Path("artifacts/rag")


def _safe_read(path: Path) -> pd.DataFrame | None:
    """Read parquet, return None on failure."""
    try:
        if path.is_dir():
            return pd.read_parquet(path)
        elif path.exists():
            return pd.read_parquet(path)
    except Exception:
        pass
    return None


def _qa(question: str, answer: str, sources: list[str], topic: str,
        confidence: str = "high") -> dict:
    """Create a Q&A entry."""
    return {
        "question": question,
        "answer": answer,
        "sources": sources,
        "topic": topic,
        "confidence": confidence,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Q&A generators by topic
# ---------------------------------------------------------------------------

def _pd_forecast_qas(qa_list: list) -> None:
    """Generate PD forecast Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "pd_forecasts.parquet")
    if df is None:
        return

    # General forecast methodology
    qa_list.append(_qa(
        "How does the priority date forecast model work?",
        "The NorthStar Meridian PD forecast uses an exponential-weighted seasonal model (v2.1). "
        "It blends 50% full-history net velocity + 25% capped 24-month rolling velocity + "
        "25% capped 12-month rolling velocity. A velocity cap of max(1.25× long-term, "
        "long-term + 5 days/month) prevents recent recovery spikes from dominating. "
        "Seasonal factors are computed from P5/P95-trimmed monthly data across a 12-bin "
        "cycle anchored to the October visa year start. All 56 series have been "
        "cross-verified within ±18% of 10-year actual velocity.",
        ["pd_forecasts.parquet", "pd_forecast_model.json"],
        "pd_forecast"
    ))

    # Per-category × country forecasts
    if "category" in df.columns and "country" in df.columns:
        for (cat, cty), grp in df.groupby(["category", "country"]):
            # Find forecast columns
            forecast_cols = [c for c in grp.columns
                            if c not in ("category", "country", "chart_type")]

            if len(grp) == 0:
                continue

            # Build answer text from the first few forecast rows
            sample = grp.head(6)
            lines = []
            for _, row in sample.iterrows():
                parts = []
                for c in forecast_cols:
                    val = row.get(c)
                    if pd.notna(val):
                        parts.append(f"{c}: {val}")
                if parts:
                    lines.append(", ".join(parts))

            answer = (
                f"For {cat} / {cty}, the Meridian model forecasts:\n" +
                "\n".join(lines)
            )
            if len(grp) > 6:
                answer += f"\n... plus {len(grp) - 6} more months of projections."

            qa_list.append(_qa(
                f"What is the {cat} {cty} priority date forecast?",
                answer,
                ["pd_forecasts.parquet"],
                "pd_forecast"
            ))

            # Also add natural variant
            country_names = {
                "IND": "India", "CHN": "China", "MEX": "Mexico",
                "PHL": "Philippines", "ROW": "Rest of World",
            }
            cty_name = country_names.get(cty, cty)
            qa_list.append(_qa(
                f"When will my {cat} {cty_name} priority date become current?",
                answer,
                ["pd_forecasts.parquet"],
                "pd_forecast"
            ))


def _employer_qas(qa_list: list) -> None:
    """Generate employer-related Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "employer_friendliness_scores.parquet")
    if df is None:
        return

    score_col = None
    for c in ["efs", "efs_score", "friendliness_score", "score"]:
        if c in df.columns:
            score_col = c
            break

    tier_col = None
    for c in ["tier", "efs_tier", "friendliness_tier"]:
        if c in df.columns:
            tier_col = c
            break

    if not score_col or "employer_name" not in df.columns:
        return

    # EFS methodology
    qa_list.append(_qa(
        "How is the employer friendliness score calculated?",
        "The Employer Friendliness Score (EFS) is a 0–100 composite score: "
        "50% outcome (Bayesian-shrunk approval rate with prior=0.88, strength=20), "
        "30% wage competitiveness (linear map of median wage ratio: 0.5→0, 1.0→75, 1.3→100), "
        "20% sustainability (months_active + volume + trend + low_volatility blend). "
        "Tiers: Excellent ≥85, Good ≥70, Moderate ≥50, Below Average ≥30, Poor <30. "
        f"Total employers scored: {len(df):,}. "
        "An ML-enhanced version (HistGradientBoosting) covers the top 956 high-volume employers.",
        ["employer_friendliness_scores.parquet", "employer_friendliness_scores_ml.parquet"],
        "employer"
    ))

    # Top employers list
    rated = df.dropna(subset=[score_col])
    top = rated.nlargest(20, score_col)
    lines = []
    for _, row in top.iterrows():
        tier = f" ({row[tier_col]})" if tier_col else ""
        lines.append(f"  {row['employer_name']}: {row[score_col]:.1f}{tier}")

    qa_list.append(_qa(
        "Which are the best employers for green card sponsorship?",
        "Top 20 employers by EFS score:\n" + "\n".join(lines),
        ["employer_friendliness_scores.parquet"],
        "employer"
    ))

    qa_list.append(_qa(
        "What are the most immigration-friendly companies?",
        "Top 20 immigration-friendly companies by EFS score:\n" + "\n".join(lines),
        ["employer_friendliness_scores.parquet"],
        "employer"
    ))

    # Per-employer lookups for top 100
    top100 = rated.nlargest(100, score_col)
    for _, row in top100.iterrows():
        name = row["employer_name"]
        score = row[score_col]
        tier = row[tier_col] if tier_col else "N/A"

        # Collect extra details
        details = [f"EFS Score: {score:.1f}", f"Tier: {tier}"]
        for info_col in ["approval_rate_24m", "bayesian_approval_rate",
                         "wage_ratio_med", "n_24m", "n_36m",
                         "total_cases", "n_cases", "months_active_24m"]:
            if info_col in row.index and pd.notna(row[info_col]):
                details.append(f"{info_col}: {row[info_col]}")

        answer = f"{name} — " + ", ".join(details)

        qa_list.append(_qa(
            f"What is the EFS score for {name}?",
            answer,
            ["employer_friendliness_scores.parquet"],
            "employer"
        ))


def _salary_qas(qa_list: list) -> None:
    """Generate salary benchmark Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "salary_benchmarks.parquet")
    if df is None:
        return

    qa_list.append(_qa(
        "How can I check if my salary is competitive for immigration?",
        f"Meridian provides salary benchmarks for {len(df):,} SOC × area combinations. "
        "Compare your offered wage against OEWS prevailing wage percentiles "
        "(P10, P25, median, P75, P90) for your specific occupation code and geographic area. "
        "A wage ratio ≥1.0 means your offered wage meets or exceeds the median prevailing wage. "
        "Employers offering ≥P75 wages tend to have higher EFS scores and lower audit risk.",
        ["salary_benchmarks.parquet", "fact_oews.parquet"],
        "salary"
    ))

    # Top paying occupations
    if "median" in df.columns and "soc_code" in df.columns:
        top_socs = df.groupby("soc_code")["median"].mean().nlargest(15)
        dim_soc = _safe_read(ARTIFACTS_ROOT / "dim_soc.parquet")
        soc_titles = {}
        if dim_soc is not None:
            title_col = next((c for c in dim_soc.columns
                              if c in ("soc_title", "title", "occupation_title")), None)
            code_col = next((c for c in dim_soc.columns
                             if c in ("soc_code", "code", "occ_code")), None)
            if title_col and code_col:
                soc_titles = dict(zip(dim_soc[code_col], dim_soc[title_col]))

        lines = []
        for soc, med in top_socs.items():
            title = soc_titles.get(soc, soc)
            lines.append(f"  {soc} ({title}): median ${med:,.0f}")

        qa_list.append(_qa(
            "What are the highest paying occupations for immigrants?",
            "Top 15 highest-paying occupations (by median prevailing wage):\n" +
            "\n".join(lines),
            ["salary_benchmarks.parquet", "dim_soc.parquet"],
            "salary"
        ))

    # Tech role wages
    tech_socs = {
        "15-1252": "Software Developers",
        "15-1256": "Software Developers (Apps)",
        "15-1211": "Computer Systems Analysts",
        "15-1299": "Computer Occupations, All Other",
        "15-1221": "Computer & IS Security Analysts",
        "15-1245": "Database Administrators",
        "15-2051": "Data Scientists",
    }
    if "soc_code" in df.columns and "median" in df.columns:
        tech_df = df[df["soc_code"].isin(tech_socs.keys())]
        if len(tech_df) > 0:
            tech_summary = tech_df.groupby("soc_code")["median"].agg(["mean", "min", "max"])
            lines = []
            for soc, row in tech_summary.iterrows():
                title = tech_socs.get(soc, soc)
                lines.append(f"  {title} ({soc}): avg=${row['mean']:,.0f}, "
                             f"range ${row['min']:,.0f}–${row['max']:,.0f}")
            qa_list.append(_qa(
                "What is the prevailing wage for software engineers and tech workers?",
                "Prevailing wages for common immigration-sponsored tech roles:\n" +
                "\n".join(lines) +
                "\n\nNote: Wages vary significantly by geographic area. "
                "Use salary_benchmarks to look up your specific SOC × area.",
                ["salary_benchmarks.parquet"],
                "salary"
            ))

    # What is wage ratio
    qa_list.append(_qa(
        "What is wage ratio and why does it matter?",
        "Wage ratio = offered_wage / prevailing_wage for the same SOC code and area. "
        "A ratio ≥1.0 means the employer offers at or above the median prevailing wage. "
        "Higher wage ratios correlate with: (1) higher employer friendliness scores, "
        "(2) lower audit risk for PERM applications, (3) better chances of approval. "
        "The DOL considers prevailing wage levels: Level 1 (17th percentile), "
        "Level 2 (34th), Level 3 (50th), Level 4 (67th).",
        ["salary_benchmarks.parquet", "employer_features.parquet"],
        "salary"
    ))

    # What is prevailing wage
    qa_list.append(_qa(
        "What is a prevailing wage determination?",
        "The prevailing wage is what the DOL determines employers must pay for a position "
        "in a specific geographic area and for a specific occupation (SOC code). "
        "It is required for both LCA (H-1B) and PERM (green card) filings. "
        f"Meridian has {len(df):,} SOC × area wage records from BLS OEWS surveys, "
        "covering percentiles P10 through P90.",
        ["salary_benchmarks.parquet", "fact_oews.parquet"],
        "salary"
    ))


def _geo_qas(qa_list: list) -> None:
    """Generate geographic Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "worksite_geo_metrics.parquet")
    if df is None:
        return

    # State-level data
    if "grain" in df.columns:
        states = df[df["grain"] == "state"]
    else:
        states = df

    if "state" in df.columns and "filings_count" in df.columns:
        top_states = states.groupby("state")["filings_count"].sum().nlargest(10)
        lines = []
        for st, cnt in top_states.items():
            lines.append(f"  {st}: {cnt:,.0f} filings")
        qa_list.append(_qa(
            "Which states have the most immigration sponsorship?",
            f"Top 10 states by total immigration sponsorship filings:\n" +
            "\n".join(lines) +
            f"\n\nTotal geographic records: {len(df):,} across state, metro, county, "
            "and city grains.",
            ["worksite_geo_metrics.parquet"],
            "geographic"
        ))

    qa_list.append(_qa(
        "Where are most H-1B and green card filings concentrated?",
        "Sponsorship filings are concentrated in major metro areas: "
        "San Francisco/San Jose, New York/New Jersey, Seattle, "
        "Chicago, Dallas/Houston, Atlanta, and the DC metro area. "
        "The worksite_geo_metrics table covers all grains from state-level "
        "down to individual cities with filing counts and competitiveness ratios.",
        ["worksite_geo_metrics.parquet"],
        "geographic"
    ))

    # Top cities
    if "grain" in df.columns and "city" in df.columns and "filings_count" in df.columns:
        cities = df[df["grain"] == "city"].nlargest(10, "filings_count")
        if len(cities) > 0:
            lines = []
            for _, row in cities.iterrows():
                city = row.get("city", "Unknown")
                st = row.get("state", "")
                cnt = row["filings_count"]
                lines.append(f"  {city}, {st}: {cnt:,.0f} filings")
            qa_list.append(_qa(
                "Which cities have the most immigration sponsorship filings?",
                "Top 10 cities by immigration sponsorship filings:\n" +
                "\n".join(lines),
                ["worksite_geo_metrics.parquet"],
                "geographic"
            ))

    # Competitiveness
    if "competitiveness_ratio" in df.columns:
        cr = df["competitiveness_ratio"].dropna()
        if len(cr) > 0:
            qa_list.append(_qa(
                "What is the competitiveness ratio for immigration sponsorship?",
                f"The competitiveness ratio measures filings per employer in a geographic area. "
                f"Higher ratios indicate more intense competition. "
                f"Overall statistics: mean={cr.mean():.2f}, median={cr.median():.2f}, "
                f"max={cr.max():.1f}. "
                f"Areas with ratio >2.0 are highly competitive. "
                f"Areas with ratio <1.0 have sponsorship distributed across many employers.",
                ["worksite_geo_metrics.parquet"],
                "geographic"
            ))

    qa_list.append(_qa(
        "Can I work remotely on an H-1B visa?",
        "H-1B visas are tied to a specific worksite location listed on the LCA. "
        "If you work remotely from a different location, your employer may need to file "
        "an amended LCA for the new worksite. The worksite_geo_metrics shows which "
        "geographic areas have the most sponsorship activity for remote-friendly evaluation.",
        ["worksite_geo_metrics.parquet"],
        "geographic"
    ))


def _general_qas(qa_list: list) -> None:
    """Generate general immigration Q&A pairs."""
    qa_list.append(_qa(
        "What data sources does NorthStar use?",
        "NorthStar Meridian curates data from 14+ authoritative U.S. government sources:\n"
        "• DOL PERM labor certification disclosures (FY2005–FY2024, 1.67M+ records)\n"
        "• DOL H-1B LCA filings (FY2008–FY2026, 9.5M+ records)\n"
        "• DOS Visa Bulletin PDFs (~180 bulletins, 2011–2026)\n"
        "• BLS OEWS wage data (446K occupation × area records)\n"
        "• DOS immigrant visa issuances, applications, NIV statistics\n"
        "• USCIS form performance data, processing times\n"
        "• DHS Yearbook admissions data\n"
        "• WARN Act layoff notices (CA + TX)\n"
        "All data is publicly available from the respective agencies.",
        ["catalog.json"],
        "general"
    ))

    qa_list.append(_qa(
        "What is NorthStar?",
        "NorthStar is an immigration data intelligence platform with three components:\n"
        "• Horizon (P1): Collects raw data from government sources\n"
        "• Meridian (P2): Curates, analyzes, and models the data (41 artifacts, 17.4M rows)\n"
        "• Compass (P3): User-facing web app with personalized immigration insights\n"
        "The platform covers priority date forecasting, employer sponsorship analysis, "
        "salary benchmarks, geographic patterns, and processing time trends.",
        [],
        "general"
    ))

    qa_list.append(_qa(
        "Should I file EB2 or EB3?",
        "This depends on your specific situation. Key factors to consider:\n"
        "1. Qualification: EB2 requires a master's degree or bachelor's + 5 years experience. "
        "EB3 requires a bachelor's degree or 2 years experience.\n"
        "2. Priority date movement: Compare EB2 vs EB3 cutoff date advancement for your "
        "country of chargeability using the category_movement_metrics data.\n"
        "3. Backlog: Check backlog_estimates for both categories.\n"
        "4. EB2→EB3 downgrade: Some applicants file as EB3 when EB3 dates are moving faster.\n"
        "Use the Compass forecast tool to compare projected dates for both categories.",
        ["category_movement_metrics.parquet", "backlog_estimates.parquet", "pd_forecasts.parquet"],
        "pd_forecast"
    ))

    qa_list.append(_qa(
        "What is retrogression?",
        "Retrogression occurs when the visa bulletin cutoff date moves backward instead of "
        "forward. This typically happens at the start of a new fiscal year (October) when "
        "USCIS reduces dates to manage demand. Retrogression is tracked in the "
        "category_movement_metrics table which includes retrogression frequency and "
        "volatility scores for each category × country combination.",
        ["category_movement_metrics.parquet", "fact_cutoffs_all.parquet"],
        "visa_bulletin"
    ))

    qa_list.append(_qa(
        "How long does PERM processing take?",
        "PERM processing times vary. Based on Meridian's processing_times_trends data "
        "(FY2014–FY2025), typical timelines range from 6–18 months depending on the "
        "processing center workload and whether an audit is triggered. "
        "Employers with higher EFS scores tend to have fewer audits and smoother processing.",
        ["processing_times_trends.parquet"],
        "processing"
    ))

    # WARN Act layoffs
    warn_df = _safe_read(ARTIFACTS_ROOT / "fact_warn_events.parquet")
    if warn_df is not None and len(warn_df) > 0:
        qa_list.append(_qa(
            "How do layoffs affect immigration sponsorship?",
            f"Meridian tracks {len(warn_df):,} WARN Act layoff events. "
            "Mass layoffs can impact your immigration case: (1) H-1B holders have 60 days "
            "to find a new employer or change status if laid off, (2) PERM applications in "
            "progress may need to be refiled if the employer does a mass layoff, "
            "(3) I-140 petitions are portable after 180 days (AC21). "
            "Check the WARN events data to see if your employer has recent layoff history.",
            ["fact_warn_events.parquet"],
            "general"
        ))

    qa_list.append(_qa(
        "What is the difference between H-1B and green card?",
        "H-1B is a temporary nonimmigrant work visa (3 years, extendable to 6). "
        "A green card (permanent residence) is, well, permanent. The typical path is:\n"
        "1. H-1B approval → start working\n"
        "2. Employer files PERM (labor certification) → 6-18 months\n"
        "3. Employer files I-140 (immigrant petition) → 4-12 months\n"
        "4. Wait for priority date to become current (varies by category/country)\n"
        "5. File I-485 (adjustment of status) → 8-24 months\n"
        "For EB2 India, total wait can be 10+ years due to backlog.",
        ["pd_forecasts.parquet", "processing_times_trends.parquet"],
        "general"
    ))


def _processing_qas(qa_list: list) -> None:
    """Generate USCIS processing time Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "processing_times_trends.parquet")
    if df is None:
        return

    qa_list.append(_qa(
        "How long does I-485 processing take?",
        "Based on Meridian's I-485 processing time trends (FY2014–FY2025), "
        "processing times have varied significantly. "
        "During the COVID-19 period (FY2020–2021), backlogs increased substantially. "
        "Recent trends show improvement in throughput. "
        "Check the processing_times_trends data for quarterly approval rates, "
        "backlog months, and throughput numbers.",
        ["processing_times_trends.parquet"],
        "processing"
    ))

    # Actual data-driven QA
    if "backlog_months" in df.columns:
        bl = df["backlog_months"].dropna()
        if len(bl) > 0:
            qa_list.append(_qa(
                "What is the current I-485 backlog?",
                f"Based on the latest processing data, the I-485 backlog has ranged from "
                f"{bl.min():.1f} to {bl.max():.1f} months over FY2014–FY2025. "
                f"The most recent data point shows {bl.iloc[-1]:.1f} months. "
                f"Backlog months = (pending cases) / (monthly completions).",
                ["processing_times_trends.parquet"],
                "processing"
            ))

    if "approval_rate" in df.columns:
        ar = df["approval_rate"].dropna()
        if len(ar) > 0:
            qa_list.append(_qa(
                "What is the I-485 approval rate?",
                f"The I-485 approval rate has ranged from {ar.min():.1%} to {ar.max():.1%} "
                f"over FY2014–FY2025. Latest: {ar.iloc[-1]:.1%}. "
                "Approval rates reflect both green card and other adjustment filings.",
                ["processing_times_trends.parquet"],
                "processing"
            ))

    qa_list.append(_qa(
        "How does USCIS process applications?",
        "USCIS processes employment-based immigration in stages:\n"
        "1. I-140 (Immigrant Petition): Employer files after PERM approval\n"
        "2. Priority date becomes current (wait for visa bulletin)\n"
        "3. I-485 (Adjustment of Status) or Consular Processing\n"
        "4. EAD/AP cards issued during I-485 pendency\n"
        "Each stage has its own processing times tracked by USCIS service centers.",
        ["processing_times_trends.parquet"],
        "processing"
    ))


def _visa_bulletin_qas(qa_list: list) -> None:
    """Generate visa bulletin Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "fact_cutoffs_all.parquet")
    if df is None:
        return

    cat_col = next((c for c in df.columns if c in ("category", "preference_category",
                                                    "visa_category")), None)
    country_col = next((c for c in df.columns if c in ("country", "chargeability_area")), None)
    bd_col = next((c for c in df.columns if c in ("bulletin_date", "bulletin_month")), None)
    cd_col = next((c for c in df.columns if c in ("cutoff_date",)), None)

    qa_list.append(_qa(
        "What is the visa bulletin?",
        f"The Visa Bulletin is published monthly by the U.S. Department of State. "
        f"It shows cutoff dates for each employment-based (EB) preference category "
        f"and country of chargeability. Meridian has parsed {len(df):,} cutoff records "
        f"from ~180 bulletins spanning 2011–2026. If your priority date is before the "
        f"cutoff date, your visa is 'current' and you can file I-485.",
        ["fact_cutoffs_all.parquet"],
        "visa_bulletin"
    ))

    qa_list.append(_qa(
        "How often does the visa bulletin update?",
        "The visa bulletin is published monthly by DOS, usually around the 15th of the "
        "month for the following month. For example, the March bulletin is published in "
        "mid-February. Each bulletin has two charts: 'Final Action Dates' (for filing "
        "I-485) and 'Dates for Filing' (earlier dates, used when USCIS opens filing).",
        ["fact_cutoffs_all.parquet"],
        "visa_bulletin"
    ))

    # Latest cutoffs per category
    if all(c is not None for c in [cat_col, country_col, bd_col, cd_col]):
        idx = df.groupby([cat_col, country_col])[bd_col].idxmax()
        latest = df.loc[idx].sort_values([cat_col, country_col])

        # EB2 India specific
        eb2_india = latest[
            (latest[cat_col].astype(str).str.contains("2", na=False)) &
            (latest[country_col].astype(str).str.contains("IND|India", case=False, na=False))
        ]
        if len(eb2_india) > 0:
            row = eb2_india.iloc[0]
            cutoff = str(row[cd_col])[:10] if pd.notna(row[cd_col]) else "Current"
            qa_list.append(_qa(
                "What is the current EB2 India cutoff date?",
                f"The latest EB2 India cutoff date is {cutoff} "
                f"(from bulletin dated {str(row[bd_col])[:10]}). "
                "EB2 India has one of the longest backlogs due to "
                "high demand relative to the 7% per-country limit.",
                ["fact_cutoffs_all.parquet"],
                "visa_bulletin"
            ))

        # EB3 India specific
        eb3_india = latest[
            (latest[cat_col].astype(str).str.contains("3", na=False)) &
            (latest[country_col].astype(str).str.contains("IND|India", case=False, na=False))
        ]
        if len(eb3_india) > 0:
            row = eb3_india.iloc[0]
            cutoff = str(row[cd_col])[:10] if pd.notna(row[cd_col]) else "Current"
            qa_list.append(_qa(
                "What is the current EB3 India cutoff date?",
                f"The latest EB3 India cutoff date is {cutoff} "
                f"(from bulletin dated {str(row[bd_col])[:10]}). "
                "EB3 India processing has sometimes been faster than EB2 India "
                "leading some applicants to consider EB2→EB3 downgrade.",
                ["fact_cutoffs_all.parquet"],
                "visa_bulletin"
            ))

        # EB2 China specific
        eb2_china = latest[
            (latest[cat_col].astype(str).str.contains("2", na=False)) &
            (latest[country_col].astype(str).str.contains("CHN|China", case=False, na=False))
        ]
        if len(eb2_china) > 0:
            row = eb2_china.iloc[0]
            cutoff = str(row[cd_col])[:10] if pd.notna(row[cd_col]) else "Current"
            qa_list.append(_qa(
                "What is the current EB2 China cutoff date?",
                f"The latest EB2 China cutoff date is {cutoff} "
                f"(from bulletin dated {str(row[bd_col])[:10]}). "
                "China is the second most backlogged country after India.",
                ["fact_cutoffs_all.parquet"],
                "visa_bulletin"
            ))

    # Movement metrics
    mv_df = _safe_read(ARTIFACTS_ROOT / "category_movement_metrics.parquet")
    if mv_df is not None:
        qa_list.append(_qa(
            "Which EB category is moving fastest?",
            f"Use the category_movement_metrics table ({len(mv_df):,} records) to compare "
            "monthly advancement rates across EB categories and countries. Key metrics: "
            "avg_monthly_advance_days (positive = forward movement), volatility, "
            "retrogression_count. EB1 ROW is typically current. "
            "EB2/EB3 India and China have the slowest movement.",
            ["category_movement_metrics.parquet"],
            "visa_bulletin"
        ))


def _occupation_qas(qa_list: list) -> None:
    """Generate occupation/SOC-related Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "soc_demand_metrics.parquet")
    if df is None:
        return

    qa_list.append(_qa(
        "Which occupations have the most immigration sponsorship?",
        f"Meridian tracks SOC-level demand across {len(df):,} records (multiple time windows). "
        "Software developers (15-1252/15-1256), computer systems analysts (15-1211), "
        "and computer occupations (15-1299) dominate sponsorship filings. "
        "Healthcare roles (physicians, physical therapists) and financial analysts "
        "also feature prominently.",
        ["soc_demand_metrics.parquet", "dim_soc.parquet"],
        "occupation"
    ))

    # Top SOCs by filing volume
    soc_col = next((c for c in df.columns if c in ("soc_code", "occ_code")), None)
    count_col = next((c for c in df.columns if c in ("filing_count", "total_filings",
                                                      "demand_count", "n_filings")), None)
    if soc_col and count_col:
        top = df.groupby(soc_col)[count_col].sum().nlargest(20)
        dim_soc = _safe_read(ARTIFACTS_ROOT / "dim_soc.parquet")
        titles = {}
        if dim_soc is not None:
            t_col = next((c for c in dim_soc.columns
                          if c in ("soc_title", "title", "occupation_title")), None)
            c_col = next((c for c in dim_soc.columns
                          if c in ("soc_code", "code", "occ_code")), None)
            if t_col and c_col:
                titles = dict(zip(dim_soc[c_col], dim_soc[t_col]))

        lines = []
        for soc, cnt in top.items():
            title = titles.get(soc, soc)
            lines.append(f"  {soc} ({title}): {cnt:,.0f}")
        qa_list.append(_qa(
            "What are the top 20 SOC codes by immigration filings?",
            "Top 20 occupations by sponsorship filing count:\n" +
            "\n".join(lines),
            ["soc_demand_metrics.parquet", "dim_soc.parquet"],
            "occupation"
        ))

    qa_list.append(_qa(
        "What is a SOC code?",
        "SOC (Standard Occupational Classification) is a system used by federal agencies "
        "to classify workers into occupational categories. The 2018 SOC system has ~867 "
        "detailed codes (6-digit). Immigration filings (PERM, LCA) require a SOC code. "
        "Meridian's dim_soc table includes 1,396 codes with crosswalks between "
        "SOC-2010 and SOC-2018 systems.",
        ["dim_soc.parquet"],
        "occupation"
    ))


def _visa_demand_qas(qa_list: list) -> None:
    """Generate visa demand Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "visa_demand_metrics.parquet")
    if df is None:
        return

    qa_list.append(_qa(
        "How many employment-based visas are issued each year?",
        f"Meridian tracks {len(df):,} visa demand records from DOS immigrant visa "
        "issuances, applications, and NIV statistics. The annual EB visa allocation "
        "is approximately 140,000 (plus unused family-based visas that overflow). "
        "In practice, actual issuances fluctuate due to processing capacity, "
        "COVID disruptions, and policy changes.",
        ["visa_demand_metrics.parquet", "dim_visa_ceiling.parquet"],
        "visa_demand"
    ))

    country_col = next((c for c in df.columns if c in ("country", "nationality",
                                                        "chargeability_area")), None)
    count_col = next((c for c in df.columns if c in ("count_issued", "count", "total",
                                                      "issuances")), None)
    if country_col and count_col:
        top = df.groupby(country_col)[count_col].sum().nlargest(10)
        lines = []
        for ctry, cnt in top.items():
            lines.append(f"  {ctry}: {cnt:,.0f}")
        qa_list.append(_qa(
            "Which countries have the highest visa demand?",
            "Top 10 countries by total visa demand:\n" +
            "\n".join(lines) +
            "\n\nIndia and China typically dominate EB visa demand, "
            "leading to significant backlogs due to the 7% per-country limit.",
            ["visa_demand_metrics.parquet"],
            "visa_demand"
        ))

    qa_list.append(_qa(
        "What is the 7% per-country limit?",
        "U.S. immigration law limits any single country to no more than 7% of the "
        "total EB visas issued annually (~9,800 out of 140,000). This creates massive "
        "backlogs for high-demand countries like India and China, where applicants may "
        "wait 10+ years, while applicants from other countries often have current dates.",
        ["visa_demand_metrics.parquet", "dim_visa_ceiling.parquet"],
        "visa_demand"
    ))


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_qa_cache() -> dict:
    """Generate all pre-computed Q&A pairs.

    Returns:
        Summary dict with counts.
    """
    RAG_ROOT.mkdir(parents=True, exist_ok=True)

    print("[QA] Generating pre-computed Q&A pairs...")
    qa_list: list[dict] = []

    _pd_forecast_qas(qa_list)
    _employer_qas(qa_list)
    _salary_qas(qa_list)
    _geo_qas(qa_list)
    _processing_qas(qa_list)
    _visa_bulletin_qas(qa_list)
    _occupation_qas(qa_list)
    _visa_demand_qas(qa_list)
    _general_qas(qa_list)

    # Deduplicate by question text
    seen = set()
    unique = []
    for qa in qa_list:
        key = qa["question"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(qa)

    # Write cache
    cache_path = RAG_ROOT / "qa_cache.json"
    cache_path.write_text(json.dumps(unique, indent=2, default=str))

    # Stats by topic
    topic_counts: dict[str, int] = {}
    for qa in unique:
        t = qa["topic"]
        topic_counts[t] = topic_counts.get(t, 0) + 1

    print(f"  → {cache_path} ({len(unique)} Q&A pairs)")
    for topic, count in sorted(topic_counts.items()):
        print(f"    {topic}: {count} pairs")

    summary = {
        "cache_path": str(cache_path),
        "total_qa_pairs": len(unique),
        "topics": topic_counts,
    }
    return summary


if __name__ == "__main__":
    print("=" * 60)
    print("NorthStar Meridian — QA Generator")
    print("=" * 60)
    result = generate_qa_cache()
    print(f"\nDone. {result['total_qa_pairs']} Q&A pairs generated.")
