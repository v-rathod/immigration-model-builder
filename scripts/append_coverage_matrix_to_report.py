#!/usr/bin/env python3
"""
append_coverage_matrix_to_report.py
Appends (or replaces) the '## Dataset Coverage Matrix (P2 vs. Downloads)' section
in FINAL_SINGLE_REPORT.md.  Idempotent: strips old section before re-appending.
NON-INTERACTIVE, no data ingestion.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
REPO_ROOT    = Path(__file__).resolve().parents[1]
METRICS_DIR  = REPO_ROOT / "artifacts" / "metrics"
REPORT_PATH  = METRICS_DIR / "FINAL_SINGLE_REPORT.md"
MATRIX_MD    = METRICS_DIR / "dataset_coverage_matrix.md"
COVERAGE_JSON = METRICS_DIR / "dataset_coverage_matrix.json"

SECTION_HEADING = "## Dataset Coverage Matrix (P2 vs. Downloads)"
# ---------------------------------------------------------------------------


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def build_section(coverage: list[dict], ts: str) -> str:
    """Build the full markdown section to inject into the report."""
    n_downloaded = sum(1 for d in coverage if d["downloaded"])
    n_curated    = sum(1 for d in coverage if d["curated_present"])
    n_gaps       = sum(1 for d in coverage if d["gap"])
    n_total      = len(coverage)

    lines = [
        SECTION_HEADING,
        f"_Last generated: {ts}_",
        "",
        "### Inventory Summary",
        f"- Total datasets detected in downloads: **{n_downloaded}**",
        f"- Datasets curated in P2: **{n_curated}/{n_total}**",
        f"- Gap datasets (downloaded but not yet curated): **{n_gaps}**",
        "",
        "### Coverage Matrix",
        "",
        "| Dataset | Downloaded? | Files | Size | Curated? | Curated Outputs | Notes |",
        "|---------|-------------|-------|------|----------|-----------------|-------|",
    ]

    for d in coverage:
        dl_flag  = "✅" if d["downloaded"] else "—"
        gap_flag = d.get("gap", False)
        if d["curated_present"]:
            cur_flag = "✅"
        elif gap_flag:
            cur_flag = "⚠️ GAP"
        else:
            cur_flag = "— ref"

        files = f"{d['download_files']:,}" if d["downloaded"] else "—"
        size  = fmt_size(d["download_size"]) if d["downloaded"] else "—"

        if d["outputs"]:
            outputs_str = " · ".join(
                f"`{o['table']}`" for o in d["outputs"]
            )
        else:
            outputs_str = "—"

        note = (d.get("note") or "—")[:80]  # truncate for table readability
        lines.append(
            f"| {d['dataset']} | {dl_flag} | {files} | {size} | {cur_flag} | {outputs_str} | {note} |"
        )

    lines += [
        "",
        "### Gap Plan (prioritized)",
        "",
        "_Datasets with downloads present but no curated P2 output yet._",
        "",
    ]

    # Priority-sorted gaps
    GAP_PRIORITY_ORDER = [
        "DOS_Numerical_Limits",
        "DOS_Waiting_List",
        "Visa_Annual_Reports",
        "Visa_Statistics",
        "NIV_Statistics",
        "USCIS_IMMIGRATION",
        "DHS_Yearbook",
        "TRAC",
        "WARN",
        "ACS",
    ]

    PLACEMENT = {
        "DOS_Numerical_Limits":  ("P2.1", "Annual per-country visa ceilings — foundational for backlog projections. Parse Excel → `dim_visa_ceiling`."),
        "DOS_Waiting_List":      ("P2.1", "Priority-date waiting-list data — extends backlog estimation. Parse PDFs/CSVs → `fact_waiting_list`."),
        "Visa_Annual_Reports":   ("P2.1", "Yearly issuance totals validate demand estimates. Parse Excel → `fact_visa_issuance`."),
        "Visa_Statistics":       ("P2.1", "Application/refusal stats — leading demand indicator. Parse DOS NIV CSVs → `fact_visa_applications`."),
        "NIV_Statistics":        ("P2.1", "Non-immigrant visa trends — H-1B/L1 demand signal. Parse Excel → `fact_niv_issuance`."),
        "USCIS_IMMIGRATION":     ("P2.2", "Annual USCIS approval/denial stats — cross-validate fact_perm/fact_lca. Parse Excel → `fact_uscis_approvals`."),
        "DHS_Yearbook":          ("P2.2", "Long historical series — cohort-level backlog analysis. Ingest CSVs → `fact_dhs_admissions`."),
        "TRAC":                  ("P3",   "Adjudication timelines — enrich employer-level risk scoring. Parse FOIA CSVs → `fact_trac_adjudications`."),
        "WARN":                  ("P3",   "Layoff notices — sponsorship stability risk signal. Parse state files → `fact_warn_events`; fuzzy-join dim_employer."),
        "ACS":                   ("P3",   "ACS occupation wage distributions supplement OEWS benchmarks. Parse Census PUMS → `fact_acs_wages`."),
    }

    idx = 1
    gap_ds = {d["dataset"] for d in coverage if d.get("gap")}

    for ds in GAP_PRIORITY_ORDER:
        if ds not in gap_ds:
            continue
        placement, idea = PLACEMENT.get(ds, ("P3", "—"))
        inv = next((d for d in coverage if d["dataset"] == ds), None)
        size_note = f"{inv['download_files']:,} files, {fmt_size(inv['download_size'])}" if inv else ""
        lines += [
            f"{idx}. **{ds}** `[{placement}]`  ({size_note})",
            f"   {idea}",
            "",
        ]
        idx += 1

    # Emit any gap datasets not in the priority list
    for d in coverage:
        if d.get("gap") and d["dataset"] not in GAP_PRIORITY_ORDER:
            size_note = f"{d['download_files']:,} files, {fmt_size(d['download_size'])}"
            lines += [
                f"{idx}. **{d['dataset']}** `[P3]`  ({size_note})",
                f"   {d.get('note', 'No ingestion plan yet.')}",
                "",
            ]
            idx += 1

    return "\n".join(lines) + "\n"


def strip_old_section(text: str) -> str:
    """Remove any existing Dataset Coverage Matrix section."""
    marker = SECTION_HEADING + "\n"
    if marker not in text:
        return text
    idx = text.index(marker)
    # Find next top-level ## heading after the section (or end of file)
    rest = text[idx + len(marker):]
    next_h2 = rest.find("\n## ")
    if next_h2 == -1:
        return text[:idx].rstrip() + "\n"
    else:
        return text[:idx].rstrip() + "\n" + rest[next_h2 + 1:]


def main():
    for required in (MATRIX_MD, COVERAGE_JSON, REPORT_PATH):
        if not required.exists():
            print(f"ERROR: required file not found: {required}")
            raise SystemExit(1)

    with COVERAGE_JSON.open() as f:
        cov_data = json.load(f)

    coverage = cov_data["coverage"]
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    section_text = build_section(coverage, ts)

    report_text = REPORT_PATH.read_text(encoding="utf-8")
    report_text = strip_old_section(report_text)

    # Ensure trailing newline before appending
    if not report_text.endswith("\n"):
        report_text += "\n"

    report_text += "\n" + section_text

    REPORT_PATH.write_text(report_text, encoding="utf-8")

    n_curated = sum(1 for d in coverage if d["curated_present"])
    n_gaps    = sum(1 for d in coverage if d["gap"])
    print(f"Appended coverage matrix to: {REPORT_PATH}")
    print(f"  {n_curated}/{len(coverage)} datasets curated — {n_gaps} gaps documented")
    print(f"READY_TO_UPLOAD_FILE: {REPORT_PATH}")


if __name__ == "__main__":
    main()
