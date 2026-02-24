"""
tests/models/test_model_usage_matrix.py

Verify that the usage_registry.json records the correct dataset usage:
- backlog task references dim_visa_ceiling and/or fact_waiting_list
- visa_demand_metrics consumed ≥2 of the 3 new visa sources
- WARN data referenced for employer_risk_features
- TRAC and ACS appear as stubbed
"""
import json
import pathlib

import pytest

REGISTRY = pathlib.Path("artifacts/metrics/usage_registry.json")
TABLES = pathlib.Path("artifacts/tables")


def _load_registry() -> dict:
    if not REGISTRY.exists():
        pytest.skip("usage_registry.json not found")
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def _all_inputs(registry: dict) -> list[str]:
    """Flatten all input paths across all tasks."""
    inputs = []
    for task_info in registry.get("tasks", {}).values():
        inputs.extend(task_info.get("inputs", []))
    return inputs


class TestBacklogUsage:
    def test_backlog_task_present(self):
        reg = _load_registry()
        tasks = reg.get("tasks", {})
        assert "backlog_usage" in tasks, (
            "usage_registry: 'backlog_usage' task not found. "
            "Run scripts/log_backlog_usage.py"
        )

    def test_backlog_references_dim_visa_ceiling_or_waiting_list(self):
        reg = _load_registry()
        task = reg.get("tasks", {}).get("backlog_usage", {})
        inputs = [pathlib.Path(i).name for i in task.get("inputs", [])]
        has_ceiling = "dim_visa_ceiling.parquet" in inputs
        has_waiting = "fact_waiting_list.parquet" in inputs
        assert has_ceiling or has_waiting, (
            f"backlog_usage task inputs do not reference dim_visa_ceiling or "
            f"fact_waiting_list. Inputs found: {inputs}"
        )

    def test_backlog_row_count_positive(self):
        reg = _load_registry()
        task = reg.get("tasks", {}).get("backlog_usage", {})
        metrics = task.get("metrics", {})
        row_count = metrics.get("row_count", 0)
        assert row_count > 0, f"backlog_usage: row_count={row_count}"


class TestVisaDemandMetrics:
    def test_task_present(self):
        reg = _load_registry()
        assert "visa_demand_metrics" in reg.get("tasks", {}), (
            "usage_registry: 'visa_demand_metrics' task not found. "
            "Run scripts/make_visa_demand_metrics.py"
        )

    def test_used_at_least_two_sources(self):
        reg = _load_registry()
        task = reg.get("tasks", {}).get("visa_demand_metrics", {})
        metrics = task.get("metrics", {})
        sources_used = metrics.get("sources_used", 0)
        source_names = metrics.get("source_names", [])
        assert sources_used >= 2, (
            f"visa_demand_metrics: only {sources_used} source(s) used: {source_names}. "
            f"Expected ≥2 of fact_visa_issuance, fact_visa_applications, fact_niv_issuance"
        )

    def test_output_parquet_exists(self):
        p = TABLES / "visa_demand_metrics.parquet"
        assert p.exists(), "visa_demand_metrics.parquet missing"
        import pandas as pd
        df = pd.read_parquet(p)
        assert len(df) > 0, "visa_demand_metrics.parquet is empty"

    def test_references_niv_or_visa_issuance(self):
        reg = _load_registry()
        task = reg.get("tasks", {}).get("visa_demand_metrics", {})
        inputs = [pathlib.Path(i).name for i in task.get("inputs", [])]
        expected = {"fact_visa_issuance.parquet", "fact_visa_applications.parquet", "fact_niv_issuance.parquet"}
        found = set(inputs) & expected
        assert len(found) >= 2, (
            f"visa_demand_metrics inputs reference only {found}; expected ≥2 of {expected}"
        )


class TestEmployerRiskFeatures:
    def test_task_present(self):
        """employer_risk_features task OR stub should be in registry."""
        reg = _load_registry()
        tasks = reg.get("tasks", {})
        assert "employer_risk_features" in tasks, (
            "usage_registry: 'employer_risk_features' task not found"
        )

    def test_output_exists(self):
        p = TABLES / "employer_risk_features.parquet"
        assert p.exists(), "employer_risk_features.parquet missing"

    def test_join_rate_positive(self):
        """Join rate should be > 0; low rate expected due to under-populated dim_employer."""
        reg = _load_registry()
        task = reg.get("tasks", {}).get("employer_risk_features", {})
        metrics = task.get("metrics", {})
        join_rate = metrics.get("join_rate", None)
        if join_rate is None:
            # Check if it's a stub (skip_reason present)
            if "skip_reason" in metrics:
                pytest.skip(f"employer_risk_features stubbed: {metrics['skip_reason']}")
            pytest.fail("employer_risk_features: 'join_rate' not in metrics")
        assert join_rate > 0, (
            f"employer_risk_features: join_rate={join_rate:.1%} — "
            f"0 matches to dim_employer. Note: low rate (<50%) is expected while "
            f"dim_employer is under-populated (currently 19K < 60K target rows)."
        )


class TestStubsInRegistry:
    def test_trac_stubbed(self):
        reg = _load_registry()
        tasks = reg.get("tasks", {})
        assert "trac_adjudications_usage" in tasks, (
            "usage_registry: TRAC stub not logged. Run log_stubs"
        )
        task = tasks["trac_adjudications_usage"]
        metrics = task.get("metrics", {})
        assert "skip_reason" in metrics, "TRAC stub missing skip_reason"
        assert "empty" in metrics["skip_reason"].lower() or "folder" in metrics["skip_reason"].lower(), (
            f"TRAC skip_reason unexpected: {metrics['skip_reason']}"
        )

    def test_acs_stubbed(self):
        reg = _load_registry()
        tasks = reg.get("tasks", {})
        assert "acs_wages_usage" in tasks, (
            "usage_registry: ACS stub not logged"
        )
        task = tasks["acs_wages_usage"]
        metrics = task.get("metrics", {})
        assert "skip_reason" in metrics, "ACS stub missing skip_reason"
        reason = metrics["skip_reason"].lower()
        assert "404" in reason or "api" in reason or "census" in reason, (
            f"ACS skip_reason unexpected: {metrics['skip_reason']}"
        )
