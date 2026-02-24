#!/usr/bin/env python3
"""Run the 4 slow test files one by one and report results."""
import subprocess, sys, time

ROOT = "/Users/vrathod1/dev/NorthStar/immigration-model-builder"
SLOW_FILES = [
    "tests/test_dim_soc.py",
    "tests/test_dim_country.py",
    "tests/test_fact_cutoffs.py",
    "tests/test_fact_perm.py",
]

results = []
for f in SLOW_FILES:
    t0 = time.time()
    r = subprocess.run(
        [sys.executable, "-m", "pytest", f, "--tb=line", "-q"],
        capture_output=True, text=True, cwd=ROOT, timeout=600
    )
    elapsed = time.time() - t0
    # Parse last lines for summary
    lines = r.stdout.strip().split("\n")
    summary = lines[-1] if lines else "NO OUTPUT"
    results.append((f, r.returncode, elapsed, summary))
    print(f"  {f}: rc={r.returncode} ({elapsed:.0f}s) => {summary}")

# Write combined JUnit XML
print("\n--- Running all 4 for combined XML ---")
r2 = subprocess.run(
    [sys.executable, "-m", "pytest"] + SLOW_FILES +
    ["--tb=line", "-q", "--junitxml", f"{ROOT}/artifacts/metrics/slow_tests_final.xml"],
    capture_output=True, text=True, cwd=ROOT, timeout=1800
)
print(r2.stdout[-500:] if r2.stdout else "NO OUTPUT")
print(f"rc={r2.returncode}")
