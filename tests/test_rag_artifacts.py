"""Tests for RAG artifacts (chunks, Q&A cache, catalog).

Validates that the RAG export from Meridian produces well-formed,
complete artifacts that Compass (P3) can consume directly.
"""

import json
from pathlib import Path

import pytest

RAG_ROOT = Path("artifacts/rag")
CHUNKS_DIR = RAG_ROOT / "chunks"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def catalog():
    """Load catalog.json."""
    path = RAG_ROOT / "catalog.json"
    if not path.exists():
        pytest.skip("catalog.json not found — run rag_builder first")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def all_chunks():
    """Load all_chunks.json."""
    path = RAG_ROOT / "all_chunks.json"
    if not path.exists():
        pytest.skip("all_chunks.json not found — run rag_builder first")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def qa_cache():
    """Load qa_cache.json."""
    path = RAG_ROOT / "qa_cache.json"
    if not path.exists():
        pytest.skip("qa_cache.json not found — run qa_generator first")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Catalog tests
# ---------------------------------------------------------------------------

class TestCatalog:
    """Tests for artifacts/rag/catalog.json."""

    def test_catalog_exists(self):
        assert (RAG_ROOT / "catalog.json").exists()

    def test_catalog_has_program_info(self, catalog):
        assert catalog["program"] == "NorthStar"
        assert "Meridian" in catalog["project"]

    def test_catalog_has_topics(self, catalog):
        assert len(catalog["topics"]) >= 8
        required = {"pd_forecast", "employer", "salary", "visa_bulletin",
                     "geographic", "occupation", "processing"}
        assert required.issubset(set(catalog["topics"]))

    def test_catalog_has_topic_descriptions(self, catalog):
        for topic in catalog["topics"]:
            assert topic in catalog["topic_descriptions"], \
                f"Missing description for topic: {topic}"

    def test_catalog_has_artifacts(self, catalog):
        assert len(catalog["artifacts"]) >= 20, \
            f"Expected ≥20 artifacts in catalog, got {len(catalog['artifacts'])}"

    def test_catalog_artifact_format(self, catalog):
        for art in catalog["artifacts"]:
            assert "name" in art, f"Artifact missing 'name': {art}"


# ---------------------------------------------------------------------------
# Chunk tests
# ---------------------------------------------------------------------------

class TestChunks:
    """Tests for RAG text chunks."""

    def test_chunks_exist(self):
        assert (RAG_ROOT / "all_chunks.json").exists()

    def test_chunks_not_empty(self, all_chunks):
        assert len(all_chunks) >= 20, \
            f"Expected ≥20 chunks, got {len(all_chunks)}"

    def test_chunk_schema(self, all_chunks):
        """Every chunk must have required fields."""
        required_fields = {"chunk_id", "source_artifact", "topic", "label",
                           "text", "metadata", "generated_at"}
        for chunk in all_chunks:
            missing = required_fields - set(chunk.keys())
            assert not missing, \
                f"Chunk {chunk.get('label', '?')} missing fields: {missing}"

    def test_chunk_ids_unique(self, all_chunks):
        ids = [c["chunk_id"] for c in all_chunks]
        assert len(ids) == len(set(ids)), "Duplicate chunk IDs found"

    def test_chunk_text_not_empty(self, all_chunks):
        for chunk in all_chunks:
            assert len(chunk["text"].strip()) > 10, \
                f"Chunk {chunk['label']} has empty/trivial text"

    def test_topic_coverage(self, all_chunks):
        """Must have chunks for key topics."""
        topics = {c["topic"] for c in all_chunks}
        required = {"pd_forecast", "employer", "salary"}
        assert required.issubset(topics), \
            f"Missing required topics: {required - topics}"

    def test_pd_forecast_chunks_present(self, all_chunks):
        """Must have PD forecast detail chunks."""
        pd_chunks = [c for c in all_chunks if c["topic"] == "pd_forecast"]
        assert len(pd_chunks) >= 10, \
            f"Expected ≥10 PD forecast chunks, got {len(pd_chunks)}"

    def test_employer_chunks_present(self, all_chunks):
        """Must have employer summary + top/bottom chunks."""
        emp_chunks = [c for c in all_chunks if c["topic"] == "employer"]
        labels = {c["label"] for c in emp_chunks}
        assert "efs_summary" in labels, "Missing EFS summary chunk"
        assert "efs_top50" in labels, "Missing EFS top-50 chunk"

    def test_topic_files_exist(self, all_chunks):
        """Each topic should have its own JSON file in chunks/."""
        topics = {c["topic"] for c in all_chunks}
        for topic in topics:
            path = CHUNKS_DIR / f"{topic}.json"
            assert path.exists(), f"Missing topic file: {path}"


# ---------------------------------------------------------------------------
# QA Cache tests
# ---------------------------------------------------------------------------

class TestQACache:
    """Tests for artifacts/rag/qa_cache.json."""

    def test_qa_cache_exists(self):
        assert (RAG_ROOT / "qa_cache.json").exists()

    def test_qa_pairs_not_empty(self, qa_cache):
        assert len(qa_cache) >= 50, \
            f"Expected ≥50 Q&A pairs, got {len(qa_cache)}"

    def test_qa_schema(self, qa_cache):
        """Every Q&A pair must have required fields."""
        required = {"question", "answer", "sources", "topic", "confidence",
                    "generated_at"}
        for qa in qa_cache:
            missing = required - set(qa.keys())
            assert not missing, \
                f"Q&A '{qa.get('question', '?')[:50]}' missing: {missing}"

    def test_qa_questions_unique(self, qa_cache):
        """No duplicate questions."""
        questions = [q["question"].lower().strip() for q in qa_cache]
        assert len(questions) == len(set(questions)), \
            "Duplicate questions found in Q&A cache"

    def test_qa_answers_not_empty(self, qa_cache):
        for qa in qa_cache:
            assert len(qa["answer"].strip()) > 20, \
                f"Q&A '{qa['question'][:40]}' has trivial answer"

    def test_qa_topic_coverage(self, qa_cache):
        """Must have Q&A pairs for key topics."""
        topics = {q["topic"] for q in qa_cache}
        required = {"pd_forecast", "employer", "general"}
        assert required.issubset(topics), \
            f"Missing Q&A topics: {required - topics}"

    def test_qa_has_methodology_questions(self, qa_cache):
        """Must answer 'how does the model work?' questions."""
        questions_lower = [q["question"].lower() for q in qa_cache]
        assert any("forecast model" in q for q in questions_lower), \
            "Missing PD forecast methodology Q&A"
        assert any("friendliness score" in q for q in questions_lower), \
            "Missing EFS methodology Q&A"

    def test_qa_has_employer_lookups(self, qa_cache):
        """Must have per-employer lookup Q&As."""
        employer_qas = [q for q in qa_cache
                        if q["question"].startswith("What is the EFS score for")]
        assert len(employer_qas) >= 50, \
            f"Expected ≥50 employer lookup Q&As, got {len(employer_qas)}"

    def test_qa_sources_are_lists(self, qa_cache):
        for qa in qa_cache:
            assert isinstance(qa["sources"], list), \
                f"Q&A '{qa['question'][:40]}' sources should be a list"


# ---------------------------------------------------------------------------
# Integration test: build pipeline
# ---------------------------------------------------------------------------

class TestBuildSummary:
    """Tests for the build_summary.json output."""

    def test_build_summary_exists(self):
        path = RAG_ROOT / "build_summary.json"
        assert path.exists()

    def test_build_summary_format(self):
        path = RAG_ROOT / "build_summary.json"
        if not path.exists():
            pytest.skip("build_summary.json not found")
        data = json.loads(path.read_text())
        assert "total_chunks" in data
        assert "topics" in data
        assert data["total_chunks"] >= 20
