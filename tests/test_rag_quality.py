"""RAG Quality Tests A–G — Comprehensive chat-feature quality assurance.

A: Answer ↔ Artifact Data Fidelity — spot-check QA answers contain real numbers from Parquet
B: Topic Balance Minimums — every topic has ≥N chunks AND ≥N QA pairs
C: Chunk ↔ Source Traceability — source_artifact file or directory actually exists
D: Token Budget Compliance — no chunk exceeds max char length, avg within budget
E: Freshness / Staleness Detection — QA cache not older than underlying artifacts
F: Retrieval Simulation — keyword queries match expected topic chunks
G: QA ↔ Chunk Topic Alignment — every QA topic has at least one matching chunk

Milestone 15 — NorthStar Meridian RAG quality hardening.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

ARTIFACTS_ROOT = Path("artifacts/tables")
RAG_ROOT = Path("artifacts/rag")
CHUNKS_DIR = RAG_ROOT / "chunks"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def all_chunks():
    path = RAG_ROOT / "all_chunks.json"
    if not path.exists():
        pytest.skip("all_chunks.json not found — run rag_builder first")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def qa_cache():
    path = RAG_ROOT / "qa_cache.json"
    if not path.exists():
        pytest.skip("qa_cache.json not found — run qa_generator first")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def catalog():
    path = RAG_ROOT / "catalog.json"
    if not path.exists():
        pytest.skip("catalog.json not found")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Test A: Answer ↔ Artifact Data Fidelity
# ---------------------------------------------------------------------------

class TestAnswerFidelity:
    """Spot-check that QA answers contain real numbers from source artifacts."""

    def test_employer_count_in_efs_answer(self, qa_cache):
        """EFS methodology answer must mention the actual employer count."""
        efs_df = pd.read_parquet(ARTIFACTS_ROOT / "employer_friendliness_scores.parquet")
        actual_count = len(efs_df)

        efs_qa = [q for q in qa_cache
                  if "friendliness score" in q["question"].lower()
                  and "calculated" in q["question"].lower()]
        assert len(efs_qa) >= 1, "Missing EFS methodology Q&A"
        assert f"{actual_count:,}" in efs_qa[0]["answer"], \
            f"EFS answer should mention {actual_count:,} employers"

    def test_salary_count_in_salary_answer(self, qa_cache):
        """Salary Q&A must reference actual row count from salary_benchmarks."""
        sb_df = pd.read_parquet(ARTIFACTS_ROOT / "salary_benchmarks.parquet")
        actual_count = len(sb_df)

        sal_qa = [q for q in qa_cache
                  if "competitive" in q["question"].lower()
                  and "salary" in q["question"].lower()]
        assert len(sal_qa) >= 1, "Missing salary competitiveness Q&A"
        assert f"{actual_count:,}" in sal_qa[0]["answer"], \
            f"Salary answer should mention {actual_count:,} records"

    def test_forecast_count_in_chunk(self, all_chunks):
        """PD forecast summary chunk must have real record count."""
        pd_df = pd.read_parquet(ARTIFACTS_ROOT / "pd_forecasts.parquet")
        actual = len(pd_df)

        summary = [c for c in all_chunks
                   if c["topic"] == "pd_forecast"
                   and "summary" in c["label"].lower()]
        assert len(summary) >= 1, "Missing PD forecast summary chunk"
        assert str(actual) in summary[0]["text"] or f"{actual:,}" in summary[0]["text"], \
            f"PD forecast summary should mention {actual} records"

    def test_geo_record_count_in_chunk(self, all_chunks):
        """Geographic summary chunk must reflect actual row count."""
        geo_df = pd.read_parquet(ARTIFACTS_ROOT / "worksite_geo_metrics.parquet")
        actual = len(geo_df)

        geo_chunks = [c for c in all_chunks
                      if c["topic"] == "geographic"
                      and "summary" in c["label"].lower()]
        assert len(geo_chunks) >= 1, "Missing geographic summary chunk"
        assert f"{actual:,}" in geo_chunks[0]["text"], \
            f"Geographic summary should mention {actual:,} records"

    def test_cutoffs_count_in_visa_bulletin_chunk(self, all_chunks):
        """Visa bulletin chunk must mention actual fact_cutoffs_all row count."""
        cutoffs_df = pd.read_parquet(ARTIFACTS_ROOT / "fact_cutoffs_all.parquet")
        actual = len(cutoffs_df)

        vb_chunks = [c for c in all_chunks
                     if c["topic"] == "visa_bulletin"
                     and "summary" in c["label"].lower()
                     and "visa_bulletin" in c["label"].lower()]
        assert len(vb_chunks) >= 1, "Missing visa bulletin summary chunk"
        assert f"{actual:,}" in vb_chunks[0]["text"], \
            f"Visa bulletin summary should mention {actual:,} records"

    def test_processing_count_in_chunk(self, all_chunks):
        """Processing chunk must mention actual record count."""
        proc_df = pd.read_parquet(ARTIFACTS_ROOT / "processing_times_trends.parquet")
        actual = len(proc_df)

        proc_chunks = [c for c in all_chunks
                       if c["topic"] == "processing"
                       and "summary" in c["label"].lower()]
        assert len(proc_chunks) >= 1, "Missing processing summary chunk"
        assert str(actual) in proc_chunks[0]["text"], \
            f"Processing summary should mention {actual} records"

    def test_top_employers_are_real(self, qa_cache):
        """Top employer names in QA must exist in employer_features."""
        efs_df = pd.read_parquet(ARTIFACTS_ROOT / "employer_friendliness_scores.parquet")
        if "employer_name" not in efs_df.columns:
            pytest.skip("employer_name not in employer_friendliness_scores")

        all_employers = set(efs_df["employer_name"].dropna().str.upper())

        top_qa = [q for q in qa_cache
                  if "best employers" in q["question"].lower()
                  or "immigration-friendly" in q["question"].lower()]
        assert len(top_qa) >= 1, "Missing top employers Q&A"

        # Extract employer names from answer lines
        answer = top_qa[0]["answer"]
        lines = [l.strip() for l in answer.split("\n") if ":" in l and l.strip().startswith(" ")]
        if not lines:
            lines = [l.strip() for l in answer.split("\n") if ":" in l]
        matched = 0
        for line in lines[:10]:
            name = line.split(":")[0].strip()
            if name.upper() in all_employers:
                matched += 1
        assert matched >= 3, \
            f"Expected ≥3 employer names in QA to match actual data, got {matched}"


# ---------------------------------------------------------------------------
# Test B: Topic Balance Minimums
# ---------------------------------------------------------------------------

class TestTopicBalance:
    """Every topic must have minimum representation in chunks and QAs."""

    REQUIRED_TOPICS = {
        "pd_forecast", "employer", "salary", "geographic",
        "processing", "visa_bulletin", "visa_demand", "general",
        "occupation",
    }

    MIN_CHUNKS_PER_TOPIC = 2
    MIN_QA_PER_TOPIC = 1

    def test_every_topic_has_chunks(self, all_chunks):
        """Each topic must have at least MIN_CHUNKS_PER_TOPIC chunks."""
        topic_counts = {}
        for c in all_chunks:
            t = c["topic"]
            topic_counts[t] = topic_counts.get(t, 0) + 1

        for topic in self.REQUIRED_TOPICS:
            count = topic_counts.get(topic, 0)
            assert count >= self.MIN_CHUNKS_PER_TOPIC, \
                f"Topic '{topic}' has only {count} chunks (need ≥{self.MIN_CHUNKS_PER_TOPIC})"

    def test_every_topic_has_qa_pairs(self, qa_cache):
        """Each topic must have at least MIN_QA_PER_TOPIC Q&A pairs."""
        topic_counts = {}
        for q in qa_cache:
            t = q["topic"]
            topic_counts[t] = topic_counts.get(t, 0) + 1

        for topic in self.REQUIRED_TOPICS:
            count = topic_counts.get(topic, 0)
            assert count >= self.MIN_QA_PER_TOPIC, \
                f"Topic '{topic}' has only {count} QA pairs (need ≥{self.MIN_QA_PER_TOPIC})"

    def test_no_topic_exceeds_70_pct_of_chunks(self, all_chunks):
        """No single topic should dominate >70% of total chunks."""
        total = len(all_chunks)
        topic_counts = {}
        for c in all_chunks:
            t = c["topic"]
            topic_counts[t] = topic_counts.get(t, 0) + 1

        for topic, count in topic_counts.items():
            pct = count / total * 100
            assert pct <= 70, \
                f"Topic '{topic}' has {count}/{total} chunks ({pct:.0f}%) — exceeds 70% cap"

    def test_thin_topics_enriched(self, all_chunks):
        """Previously thin topics (salary, geographic, processing) must have ≥3 chunks."""
        thin_topics = {"salary", "geographic", "processing", "visa_demand", "general"}
        topic_counts = {}
        for c in all_chunks:
            t = c["topic"]
            topic_counts[t] = topic_counts.get(t, 0) + 1

        for topic in thin_topics:
            count = topic_counts.get(topic, 0)
            assert count >= 3, \
                f"Previously thin topic '{topic}' should have ≥3 chunks, got {count}"


# ---------------------------------------------------------------------------
# Test C: Chunk ↔ Source Traceability
# ---------------------------------------------------------------------------

class TestSourceTraceability:
    """Every chunk should reference a source artifact that exists."""

    def test_source_artifacts_exist(self, all_chunks):
        """source_artifact field must point to an existing file or directory."""
        missing = []
        for chunk in all_chunks:
            src = chunk.get("source_artifact", "")
            if not src:
                missing.append(chunk["label"])
                continue
            # source_artifact may be just a filename, a topic name, or a path
            candidates = [
                ARTIFACTS_ROOT / src,
                ARTIFACTS_ROOT / src.replace(".parquet", ""),
                Path("artifacts") / src,
                Path(src),
            ]
            found = any(p.exists() for p in candidates)
            # Allow non-file sources like "general", "immigration", etc.
            if not found and src.endswith(".parquet"):
                missing.append(f"{chunk['label']}: {src}")

        assert len(missing) <= 2, \
            f"Chunks with missing source artifacts: {missing}"

    def test_chunk_labels_are_descriptive(self, all_chunks):
        """Labels should be non-trivial identifiers."""
        for chunk in all_chunks:
            label = chunk.get("label", "")
            assert len(label) >= 3, \
                f"Chunk label too short: '{label}'"
            assert label != "unknown", \
                f"Chunk has placeholder label: '{label}'"


# ---------------------------------------------------------------------------
# Test D: Token Budget Compliance
# ---------------------------------------------------------------------------

class TestTokenBudget:
    """Chunks must fit within reasonable token budgets for LLM context."""

    MAX_CHARS_PER_CHUNK = 6000  # ~1,500 tokens at 4 chars/token
    AVG_CHARS_TARGET = 3200     # Target average
    MIN_CHARS_PER_CHUNK = 50    # Trivially small chunks are wasteful

    def test_no_chunk_exceeds_max(self, all_chunks):
        """No single chunk should exceed MAX_CHARS_PER_CHUNK characters."""
        oversized = []
        for chunk in all_chunks:
            text_len = len(chunk["text"])
            if text_len > self.MAX_CHARS_PER_CHUNK:
                oversized.append((chunk["label"], text_len))

        assert len(oversized) == 0, \
            f"Oversized chunks (>{self.MAX_CHARS_PER_CHUNK} chars): {oversized}"

    def test_no_trivially_small_chunks(self, all_chunks):
        """No chunk should be trivially small (<50 chars)."""
        tiny = [(c["label"], len(c["text"])) for c in all_chunks
                if len(c["text"]) < self.MIN_CHARS_PER_CHUNK]
        assert len(tiny) == 0, \
            f"Trivially small chunks (<{self.MIN_CHARS_PER_CHUNK} chars): {tiny}"

    def test_qa_answers_reasonable_length(self, qa_cache):
        """QA answers should be between 50 and 3000 chars."""
        bad = []
        for qa in qa_cache:
            alen = len(qa["answer"])
            if alen < 50:
                bad.append((qa["question"][:40], alen, "too short"))
            elif alen > 3000:
                bad.append((qa["question"][:40], alen, "too long"))
        assert len(bad) == 0, f"QA answers with bad length: {bad}"


# ---------------------------------------------------------------------------
# Test E: Freshness / Staleness Detection
# ---------------------------------------------------------------------------

class TestFreshness:
    """RAG artifacts should not be stale relative to source data."""

    def test_qa_cache_not_older_than_key_artifacts(self):
        """qa_cache.json should be newer than key source parquet files."""
        cache_path = RAG_ROOT / "qa_cache.json"
        if not cache_path.exists():
            pytest.skip("qa_cache.json not found")

        cache_mtime = cache_path.stat().st_mtime

        key_artifacts = [
            "employer_friendliness_scores.parquet",
            "pd_forecasts.parquet",
            "salary_benchmarks.parquet",
        ]
        stale = []
        for art in key_artifacts:
            art_path = ARTIFACTS_ROOT / art
            if art_path.exists() and art_path.stat().st_mtime > cache_mtime:
                stale.append(art)

        assert len(stale) == 0, \
            f"qa_cache.json is older than: {stale}. Re-run qa_generator."

    def test_chunks_not_older_than_key_artifacts(self):
        """all_chunks.json should be newer than key source parquet files."""
        chunks_path = RAG_ROOT / "all_chunks.json"
        if not chunks_path.exists():
            pytest.skip("all_chunks.json not found")

        chunks_mtime = chunks_path.stat().st_mtime

        key_artifacts = [
            "worksite_geo_metrics.parquet",
            "processing_times_trends.parquet",
        ]
        stale = []
        for art in key_artifacts:
            art_path = ARTIFACTS_ROOT / art
            if art_path.exists() and art_path.stat().st_mtime > chunks_mtime:
                stale.append(art)

        assert len(stale) == 0, \
            f"all_chunks.json is older than: {stale}. Re-run rag_builder."


# ---------------------------------------------------------------------------
# Test F: Retrieval Simulation
# ---------------------------------------------------------------------------

class TestRetrievalSimulation:
    """Simulate keyword-based retrieval and verify correct topic matching."""

    QUERY_TOPIC_MAP = {
        "priority date forecast EB2 India": "pd_forecast",
        "employer friendliness score INFOSYS": "employer",
        "prevailing wage software developer": "salary",
        "H-1B filings California cities": "geographic",
        "I-485 processing time backlog": "processing",
        "visa bulletin cutoff date EB3": "visa_bulletin",
        "visa demand India China": "visa_demand",
        "PERM labor certification steps": "general",
        "SOC code occupation sponsorship": "occupation",
    }

    def _keyword_search(self, chunks: list, query: str, top_k: int = 5) -> list:
        """Simple keyword-match retrieval (bag of words)."""
        query_words = set(query.lower().split())
        scored = []
        for chunk in chunks:
            text_lower = chunk["text"].lower()
            hits = sum(1 for w in query_words if w in text_lower)
            if hits > 0:
                scored.append((hits, chunk))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored[:top_k]]

    def test_keyword_retrieval_hits_correct_topic(self, all_chunks):
        """For each test query, top retrieved chunks should include the expected topic."""
        failures = []
        for query, expected_topic in self.QUERY_TOPIC_MAP.items():
            results = self._keyword_search(all_chunks, query)
            if not results:
                failures.append(f"No results for query: '{query}'")
                continue
            result_topics = {r["topic"] for r in results}
            if expected_topic not in result_topics:
                failures.append(
                    f"Query '{query}': expected topic '{expected_topic}' "
                    f"not in results {result_topics}"
                )

        assert len(failures) <= 2, \
            f"Retrieval failures ({len(failures)}):\n" + "\n".join(failures)

    def test_every_topic_retrievable(self, all_chunks):
        """Each topic must be retrievable by at least one natural query."""
        topic_keywords = {
            "pd_forecast": "priority date forecast green card",
            "employer": "employer sponsorship score",
            "salary": "prevailing wage salary",
            "geographic": "state city sponsorship",
            "processing": "processing time USCIS",
            "visa_bulletin": "visa bulletin cutoff",
            "visa_demand": "visa demand issuance",
            "general": "immigration PERM H-1B",
            "occupation": "occupation SOC code",
        }
        unreachable = []
        for topic, query in topic_keywords.items():
            results = self._keyword_search(all_chunks, query)
            result_topics = {r["topic"] for r in results}
            if topic not in result_topics:
                unreachable.append(f"Topic '{topic}' not found with query '{query}'")

        assert len(unreachable) == 0, \
            f"Unreachable topics:\n" + "\n".join(unreachable)


# ---------------------------------------------------------------------------
# Test G: QA ↔ Chunk Topic Alignment
# ---------------------------------------------------------------------------

class TestQAChunkAlignment:
    """Every QA topic should have matching chunks and vice versa."""

    def test_every_qa_topic_has_chunks(self, qa_cache, all_chunks):
        """Every topic appearing in QA cache must also have chunks."""
        qa_topics = {q["topic"] for q in qa_cache}
        chunk_topics = {c["topic"] for c in all_chunks}

        orphan_qa_topics = qa_topics - chunk_topics
        assert len(orphan_qa_topics) == 0, \
            f"QA topics with no chunks: {orphan_qa_topics}"

    def test_every_chunk_topic_has_qas(self, qa_cache, all_chunks):
        """Every topic with chunks should ideally have QA pairs."""
        chunk_topics = {c["topic"] for c in all_chunks}
        qa_topics = {q["topic"] for q in qa_cache}

        orphan_chunk_topics = chunk_topics - qa_topics
        assert len(orphan_chunk_topics) == 0, \
            f"Chunk topics with no Q&A pairs: {orphan_chunk_topics}"

    def test_qa_sources_reference_valid_artifacts(self, qa_cache, catalog):
        """QA source references should match artifacts in catalog."""
        catalog_names = {a["name"] for a in catalog.get("artifacts", [])}
        # Also allow general references
        catalog_names.update({"catalog.json", "general", "pd_forecast_model.json"})

        bad_refs = []
        for qa in qa_cache:
            for src in qa.get("sources", []):
                # Allow known artifact names, path stems, or general refs
                stem = Path(src).stem
                if (src not in catalog_names
                    and stem not in {Path(n).stem for n in catalog_names}
                    and not src.endswith(".json")):
                    # Check if file actually exists
                    if not (ARTIFACTS_ROOT / src).exists() and \
                       not (ARTIFACTS_ROOT / src.replace(".parquet", "")).exists():
                        bad_refs.append(f"Q: {qa['question'][:40]} → source: {src}")

        # Allow a few because some sources are model artifacts not parquet
        assert len(bad_refs) <= 5, \
            f"QA pairs referencing non-existent sources:\n" + "\n".join(bad_refs[:10])

    def test_topic_counts_summary(self, qa_cache, all_chunks):
        """Print topic distribution for visibility (always passes)."""
        chunk_topics = {}
        for c in all_chunks:
            t = c["topic"]
            chunk_topics[t] = chunk_topics.get(t, 0) + 1

        qa_topics = {}
        for q in qa_cache:
            t = q["topic"]
            qa_topics[t] = qa_topics.get(t, 0) + 1

        all_topics = sorted(set(chunk_topics) | set(qa_topics))
        report = ["Topic Distribution:"]
        for t in all_topics:
            report.append(f"  {t}: {chunk_topics.get(t, 0)} chunks, {qa_topics.get(t, 0)} QAs")
        report.append(f"  TOTAL: {len(all_chunks)} chunks, {len(qa_cache)} QAs")

        # This test always passes — it's for visibility
        assert True, "\n".join(report)
