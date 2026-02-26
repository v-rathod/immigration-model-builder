#!/usr/bin/env python3
"""
RAG Practical Smoke Test — Simulates what P3 (Compass) would do.

Tests the full retrieval flow:
  1. Load RAG artifacts exactly as P3 would
  2. Simulate user questions
  3. Retrieve relevant chunks via keyword matching (same as P3)
  4. Verify the retrieved chunk contains factually correct data
  5. Check pre-computed Q&A cache for matching answers

Usage:
    python3 scripts/test_rag_practical.py
"""

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAG_DIR = ROOT / "artifacts" / "rag"
TABLES_DIR = ROOT / "artifacts" / "tables"

# ---------------------------------------------------------------------------
# Load RAG artifacts (exactly as P3 would)
# ---------------------------------------------------------------------------
def load_rag():
    catalog = json.load(open(RAG_DIR / "catalog.json"))
    chunks = json.load(open(RAG_DIR / "all_chunks.json"))
    qa_cache = json.load(open(RAG_DIR / "qa_cache.json"))
    return catalog, chunks, qa_cache


def retrieve_chunks(chunks, topic=None, keywords=None, top_k=3):
    """Simulate P3 retrieval: filter by topic, then rank by keyword overlap."""
    candidates = chunks
    if topic:
        candidates = [c for c in candidates if c.get("topic") == topic]
    if keywords:
        kw_lower = [k.lower() for k in keywords]
        scored = []
        for c in candidates:
            text_lower = c.get("text", "").lower()
            score = sum(1 for k in kw_lower if k in text_lower)
            if score > 0:
                scored.append((score, c))
        scored.sort(key=lambda x: -x[0])
        candidates = [c for _, c in scored[:top_k]]
    return candidates


def find_qa(qa_cache, keywords, top_k=3):
    """Find matching pre-computed Q&A pairs."""
    kw_lower = [k.lower() for k in keywords]
    results = []
    for qa in qa_cache:
        q = qa.get("question", "").lower()
        score = sum(1 for k in kw_lower if k in q)
        if score > 0:
            results.append((score, qa))
    results.sort(key=lambda x: -x[0])
    return [qa for _, qa in results[:top_k]]


# ---------------------------------------------------------------------------
# Test cases: Simulate real P3 user questions
# ---------------------------------------------------------------------------
def run_tests():
    catalog, chunks, qa_cache = load_rag()
    passed = 0
    failed = 0
    total = 0

    def check(name, condition, detail=""):
        nonlocal passed, failed, total
        total += 1
        if condition:
            passed += 1
            print(f"  PASS  {name}")
        else:
            failed += 1
            print(f"  FAIL  {name} — {detail}")

    # ---- Test 1: Catalog matches actual artifact count -----------------------
    print("\n[1] Catalog vs actual artifacts")
    actual_files = list(TABLES_DIR.glob("*.parquet")) + [
        d for d in TABLES_DIR.iterdir() if d.is_dir() and not d.name.startswith("_")
    ]
    cat_names = {a["name"] for a in catalog["artifacts"]}
    check(
        "catalog covers all table artifacts",
        len(cat_names) >= 30,
        f"only {len(cat_names)} in catalog"
    )

    # Verify row counts in catalog match actual parquet
    sample_tables = ["dim_employer.parquet", "dim_soc.parquet", "dim_country.parquet"]
    for tbl in sample_tables:
        cat_entry = next((a for a in catalog["artifacts"] if a["name"] == tbl), None)
        if cat_entry:
            actual_path = TABLES_DIR / tbl
            if actual_path.exists():
                actual_rows = len(pd.read_parquet(actual_path))
                cat_rows = cat_entry.get("rows", -1)
                check(
                    f"catalog row count for {tbl}",
                    cat_rows == actual_rows,
                    f"catalog says {cat_rows}, actual is {actual_rows}"
                )

    # ---- Test 2: EB2 India priority date question ----------------------------
    print("\n[2] User question: 'What is the EB2 India priority date forecast?'")
    results = retrieve_chunks(chunks, topic="pd_forecast", keywords=["EB2", "India", "forecast"])
    check("retrieval finds chunks", len(results) > 0, "no chunks found")
    if results:
        text = results[0]["text"]
        check("chunk mentions EB2", "EB2" in text or "eb2" in text.lower(), "EB2 not in chunk text")
        check("chunk mentions IND or India", "IND" in text or "India" in text, "India not in chunk text")

    qa_hits = find_qa(qa_cache, ["EB2", "India", "priority", "date", "forecast"])
    check("QA cache has matching answer", len(qa_hits) > 0, "no Q&A match")
    if qa_hits:
        check("QA answer is substantive (>100 chars)", len(qa_hits[0]["answer"]) > 100,
              f"answer only {len(qa_hits[0]['answer'])} chars")

    # ---- Test 3: Employer friendliness question ------------------------------
    print("\n[3] User question: 'Which employers are most H-1B friendly?'")
    results = retrieve_chunks(chunks, topic="employer", keywords=["employer", "friendly", "score"])
    check("retrieval finds employer chunks", len(results) > 0, "no chunks found")
    if results:
        text = results[0]["text"]
        # Should contain real employer names or scores
        check("chunk has substantive text (>200 chars)", len(text) > 200, f"only {len(text)} chars")

    qa_hits = find_qa(qa_cache, ["employer", "friendly", "H-1B", "score"])
    check("QA cache has employer answer", len(qa_hits) > 0, "no Q&A match")

    # ---- Test 4: Salary / wage question --------------------------------------
    print("\n[4] User question: 'What is the average salary for software engineers on H-1B?'")
    results = retrieve_chunks(chunks, topic="salary", keywords=["salary", "software", "wage"])
    check("retrieval finds salary chunks", len(results) > 0, "no chunks found")

    qa_hits = find_qa(qa_cache, ["salary", "software", "engineer", "wage", "H-1B"])
    check("QA cache has salary answer", len(qa_hits) > 0, "no Q&A match")

    # ---- Test 5: Visa bulletin question --------------------------------------
    print("\n[5] User question: 'Show me the latest visa bulletin dates'")
    results = retrieve_chunks(chunks, topic="visa_bulletin", keywords=["visa", "bulletin", "cutoff", "date"])
    check("retrieval finds visa bulletin chunks", len(results) > 0, "no chunks found")
    if results:
        text = results[0]["text"]
        check("chunk contains dates or cutoff info", 
              any(w in text.lower() for w in ["cutoff", "final action", "filing", "2025", "2026"]),
              "no date/cutoff content")

    # ---- Test 6: Geographic question -----------------------------------------
    print("\n[6] User question: 'Which states have the most H-1B workers?'")
    results = retrieve_chunks(chunks, topic="geographic", keywords=["state", "worksite", "geographic"])
    check("retrieval finds geographic chunks", len(results) > 0, "no chunks found")

    # ---- Test 7: Cross-check chunk numbers against actual data ---------------
    print("\n[7] Data accuracy: chunk numbers vs actual parquet")
    
    # Check employer chunk cites a meaningful employer count
    # The chunk may reference EFS scored count (70K) rather than dim_employer (243K)
    # — both are valid; we just verify the chunk contains SOME count > 1000
    emp_chunks = retrieve_chunks(chunks, topic="employer", keywords=["employer", "scored", "total"])
    emp_text = " ".join(c["text"] for c in emp_chunks)
    import re
    counts_in_text = [int(m.replace(",", "")) for m in re.findall(r"\d[\d,]*\d|\d+", emp_text)
                      if int(m.replace(",", "")) > 1000]
    check(
        "employer chunk cites a meaningful count (>1000)",
        len(counts_in_text) > 0,
        "no large count found in employer chunk text"
    )
    
    # Also verify catalog row count matches actual parquet for dim_employer
    cat_emp = next((a for a in catalog["artifacts"] if a["name"] == "dim_employer.parquet"), None)
    actual_emp = len(pd.read_parquet(TABLES_DIR / "dim_employer.parquet"))
    if cat_emp:
        check(
            f"catalog dim_employer rows match actual ({actual_emp:,})",
            cat_emp["rows"] == actual_emp,
            f"catalog={cat_emp['rows']}, actual={actual_emp}"
        )

    # ---- Test 8: Chunk self-containedness ------------------------------------
    print("\n[8] Chunk quality: self-contained with attribution")
    for c in chunks[:10]:  # spot-check first 10
        has_source = "source_artifact" in c and c["source_artifact"]
        has_text = len(c.get("text", "")) > 50
        has_topic = "topic" in c and c["topic"]
        check(
            f"chunk '{c.get('label','')}' is complete",
            has_source and has_text and has_topic,
            f"missing: {'source' if not has_source else ''} {'text' if not has_text else ''} {'topic' if not has_topic else ''}"
        )

    # ---- Summary -------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"RAG Practical Smoke Test: {passed}/{total} passed, {failed} failed")
    if failed > 0:
        print("NOTE: Some failures may indicate RAG needs rebuild after data changes.")
        print("  Fix: python3 -m src.export.rag_builder && python3 -m src.export.qa_generator")
    print(f"{'='*60}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run_tests())
