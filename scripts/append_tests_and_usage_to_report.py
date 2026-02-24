#!/usr/bin/env python3
"""
append_tests_and_usage_to_report.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Appends two new sections to artifacts/metrics/FINAL_SINGLE_REPORT.md:

  ## Model Usage Matrix           — which source tables feed which derived outputs
  ## Test & QA Summary (New)      — pytest pass-rate, per-file breakdown, stub catalogue
"""

from __future__ import annotations

import json
import pathlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPORT  = ROOT / "artifacts" / "metrics" / "FINAL_SINGLE_REPORT.md"
REGISTRY_JSON = ROOT / "artifacts" / "metrics" / "usage_registry.json"
TESTS_SUMMARY = ROOT / "artifacts" / "metrics" / "tests_summary.txt"
JUNIT_XML     = ROOT / "artifacts" / "metrics" / "pytest_results.xml"

_SENTINEL_USAGE = "## Model Usage Matrix"
_SENTINEL_QA    = "## Test & QA Summary (New)"


# ─────────────────────────── helpers ────────────────────────────────────────

def _read_summary() -> dict[str, str]:
    if not TESTS_SUMMARY.exists():
        return {}
    return dict(
        line.strip().split("=", 1)
        for line in TESTS_SUMMARY.read_text().splitlines()
        if "=" in line
    )


def _parse_junit(path: pathlib.Path) -> tuple[list[dict], dict[str, list[dict]]]:
    """Return (all_cases, {testfile: [cases]})."""
    if not path.exists():
        return [], {}
    root = ET.parse(str(path)).getroot()
    all_cases: list[dict] = []
    by_file: dict[str, list[dict]] = {}
    for tc in root.iter("testcase"):
        classname = tc.attrib.get("classname", "")
        name      = tc.attrib.get("name", "")
        time_s    = float(tc.attrib.get("time", 0))
        failed    = tc.find("failure") is not None or tc.find("error") is not None
        skipped   = tc.find("skipped") is not None
        # derive the file name from classname (e.g. "tests.datasets.test_foo.TestBar")
        parts = classname.split(".")
        fname = parts[-2] if len(parts) >= 2 else classname
        rec = {"name": name, "class": parts[-1] if parts else "", "file": fname,
               "failed": failed, "skipped": skipped, "time": time_s}
        all_cases.append(rec)
        by_file.setdefault(fname, []).append(rec)
    return all_cases, by_file


def _registry_tasks() -> dict:
    if not REGISTRY_JSON.exists():
        return {}
    try:
        return json.loads(REGISTRY_JSON.read_text()).get("tasks", {})
    except Exception:
        return {}


# ─────────────────────────── section builders ───────────────────────────────

def _build_usage_matrix(tasks: dict) -> str:
    lines: list[str] = [
        _SENTINEL_USAGE,
        "",
        "The table below summarises how each **curated source table** is consumed by a "
        "downstream task (script or model) to produce a **derived artefact**.",
        "",
        "| Task | Input Sources | Output / Artefact | Key Metrics |",
        "|------|--------------|-------------------|-------------|",
    ]
    # Sort tasks: stubs last
    def _sort_key(item: tuple[str, dict]) -> tuple[int, str]:
        t, v = item
        is_stub = bool(v.get("metrics", {}).get("skip_reason"))
        return (int(is_stub), t)

    def _short(name: str) -> str:
        """Return basename without extension for parquet paths, else unchanged."""
        p = pathlib.Path(name)
        if p.suffix in (".parquet", ".ndjson", ".json"):
            return p.stem
        return name

    for task_name, v in sorted(tasks.items(), key=_sort_key):
        inputs  = ", ".join(f"`{_short(x)}`" for x in v.get("inputs",  []) if x) or "—"
        outputs = ", ".join(f"`{_short(x)}`" for x in v.get("outputs", []) if x) or "—"
        m = v.get("metrics", {})
        skip_reason = m.pop("skip_reason", None) if m else None

        if skip_reason:
            key_m = f"*Stub — {skip_reason}*"
        else:
            # format metrics as short KV list
            kv = [f"{k}={v}" for k, v in (m or {}).items()
                  if k not in ("skip_reason",)]
            key_m = "; ".join(kv[:5]) if kv else "—"

        lines.append(f"| `{task_name}` | {inputs} | {outputs} | {key_m} |")

    lines += [
        "",
        "> **Generated** " + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "",
    ]
    return "\n".join(lines)


def _build_qa_summary(
        summary: dict[str, str],
        all_cases: list[dict],
        by_file: dict[str, list[dict]],
) -> str:
    passed  = int(summary.get("PASSED",  0))
    failed  = int(summary.get("FAILED",  0))
    skipped = int(summary.get("SKIPPED", 0))
    total   = int(summary.get("TOTAL",   0)) or len(all_cases)
    rate    = float(summary.get("PASS_RATE", 0))

    badge = "✅ PASS" if rate >= 0.95 else "❌ BELOW THRESHOLD"

    lines: list[str] = [
        _SENTINEL_QA,
        "",
        "### Overall Result",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| **Pass rate** | **{rate:.1%}** {badge} |",
        f"| Passed | {passed} |",
        f"| Failed | {failed} |",
        f"| Skipped | {skipped} |",
        f"| Total | {total} |",
        f"| Threshold | ≥ 95% |",
        "",
        "### Per-File Breakdown",
        "",
        "| Test file | Tests | Passed | Failed | Skipped | Pass% |",
        "|-----------|-------|--------|--------|---------|-------|",
    ]

    for fname, cases in sorted(by_file.items()):
        n_total   = len(cases)
        n_failed  = sum(1 for c in cases if c["failed"])
        n_skipped = sum(1 for c in cases if c["skipped"])
        n_passed  = n_total - n_failed - n_skipped
        pct       = n_passed / n_total if n_total else 1.0
        icon      = "✅" if pct == 1.0 else ("⚠️" if pct >= 0.90 else "❌")
        lines.append(
            f"| `{fname}.py` | {n_total} | {n_passed} | {n_failed} | {n_skipped} | "
            f"{icon} {pct:.0%} |"
        )

    lines += [
        "",
        "### Test Scope",
        "",
        "| Test category | Files | What is validated |",
        "|--------------|-------|-------------------|",
        "| Schema & PK (core) | `test_schema_and_pk_core.py` | Required columns, PK uniqueness, row counts, partition counts for PERM/LCA/OEWS/VB/Derived datasets |",
        "| Schema & PK (new) | `test_schema_and_pk_new.py` | Same checks for all 10 P2 gap tables (visa_ceiling … fact_acs_wages) |",
        "| Referential Integrity | `test_referential_integrity.py` | FK join rates: employer_id→dim_employer, soc_code→dim_soc, country→dim_country (per-table thresholds applied for DOS naming) |",
        "| Coverage & Files | `test_coverage_files.py` | Parquet row counts ≥ ingested-file counts × coverage threshold |",
        "| Value Ranges | `test_value_ranges_and_continuity.py` | Non-negative counts, backlog cap [0,600], FY span continuity, salary percentile ordering |",
        "| Model Usage Matrix | `test_model_usage_matrix.py` | Usage-registry task entries for all 5 tracked tasks; stub skip_reason presence |",
        "| Integration / E2E | `test_integration_e2e_sanity.py` | visa_demand_metrics (537 K rows) country RI, backlog non-null cols, EFS acceptance |",
        "",
        "### Threshold Notes",
        "",
        "| Dataset | Metric | Threshold | Rationale |",
        "|---------|--------|-----------|-----------|",
        "| DOS visa tables | Country RI | 50–70% | DOS/FAO plain names differ from ISO-3166 (e.g. \"Vietnam\" vs \"Viet Nam\", \"Africa Total\" is a regional aggregate) |",
        "| soc_demand_metrics | SOC RI | 80% | Legacy SOC-2000/2010 codes present in PERM data do not exist in current dim_soc |",
        "| fact_cutoffs partitions | Count | ≥168 | 280 leaves observed; extra partitions from snapshot rebuilds are harmless |",
        "| TRAC / ACS | Row count | 0 acceptable | Both tables are stubs; source files/API unavailable |",
        "",
        "> **Generated** " + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "",
    ]
    return "\n".join(lines)


# ─────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    tasks = _registry_tasks()
    summary = _read_summary()
    all_cases, by_file = _parse_junit(JUNIT_XML)

    usage_section = _build_usage_matrix(tasks)
    qa_section    = _build_qa_summary(summary, all_cases, by_file)

    current = REPORT.read_text(encoding="utf-8") if REPORT.exists() else ""

    # Strip any previous copies of these sections so we don't duplicate
    def _strip_section(text: str, sentinel: str) -> str:
        # Remove from sentinel to next ## sentinel or end-of-string
        pattern = re.compile(
            rf"^{re.escape(sentinel)}.*?(?=^## |\Z)",
            re.MULTILINE | re.DOTALL,
        )
        return pattern.sub("", text)

    current = _strip_section(current, _SENTINEL_USAGE)
    current = _strip_section(current, _SENTINEL_QA)
    current = current.rstrip()

    new_report = current + "\n\n" + usage_section + "\n" + qa_section
    REPORT.write_text(new_report, encoding="utf-8")
    print(f"Report updated → {REPORT}  ({len(new_report.splitlines())} lines)")


if __name__ == "__main__":
    main()
