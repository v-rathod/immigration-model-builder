.PHONY: help audit-input audit-outputs audit-all dry-run curate bundle test clean

help:
	@echo "Available targets:"
	@echo "  dry-run        - Preview files/partitions to be processed (no writes)"
	@echo "  curate         - Run the curation pipeline (builds outputs)"
	@echo "  audit-input    - Run input coverage auditor"
	@echo "  audit-outputs  - Run output validation auditor"
	@echo "  audit-all      - Run both input and output audits"
	@echo "  bundle         - Create audit bundle ZIP for upload"
	@echo "  test           - Run all tests (includes coverage checks)"
	@echo "  clean          - Clean artifacts directory"

dry-run:
	@echo "Running curation pipeline in DRY-RUN mode..."
	@python3 -m src.curate.run_curate --paths configs/paths.yaml --dry-run

curate:
	python3 -m src.curate.run_curate --paths configs/paths.yaml

audit-input:
	@echo "Running input coverage auditor..."
	@python3 scripts/audit_input_coverage.py --paths configs/paths.yaml --report artifacts/metrics/input_coverage_report.md --json artifacts/metrics/input_coverage_report.json
	@echo ""
	@echo "Reports generated:"
	@echo "  MD:   artifacts/metrics/input_coverage_report.md"
	@echo "  JSON: artifacts/metrics/input_coverage_report.json"

audit-outputs:
	@echo "Running output validation auditor..."
	@python3 scripts/audit_outputs.py --paths configs/paths.yaml --schemas configs/schemas.yml --report artifacts/metrics/output_audit_report.md --json artifacts/metrics/output_audit_report.json
	@echo ""
	@echo "Reports generated:"
	@echo "  MD:   artifacts/metrics/output_audit_report.md"
	@echo "  JSON: artifacts/metrics/output_audit_report.json"

audit-all: audit-input audit-outputs
	@echo ""
	@echo "All audits complete. See artifacts/metrics/ for reports."

bundle:
	@echo "Creating audit bundle..."
	@python3 scripts/make_audit_bundle.py --out artifacts/metrics/audit_bundle.zip
	@echo ""
	@echo "Bundle ready for upload."

test:
	python3 -m pytest -q

clean:
	rm -rf artifacts/tables/*
	rm -rf artifacts/metrics/*
	rm -rf artifacts/models/*

