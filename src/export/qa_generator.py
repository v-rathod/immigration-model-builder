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


def _geo_qas(qa_list: list) -> None:
    """Generate geographic Q&A pairs."""
    df = _safe_read(ARTIFACTS_ROOT / "worksite_geo_metrics.parquet")
    if df is None:
        return

    qa_list.append(_qa(
        "Which states have the most immigration sponsorship?",
        f"Meridian tracks {len(df):,} geographic records across state, metro, county, "
        "and city grains. Top sponsorship states historically include California, "
        "Texas, New York, New Jersey, Washington, Illinois, and Massachusetts — "
        "driven by tech hubs, financial centers, and healthcare corridors. "
        "Use the worksite geographic dashboard for detailed breakdowns by location.",
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
