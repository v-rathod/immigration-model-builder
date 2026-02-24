#!/usr/bin/env python3
"""Run full test suite and produce summary."""
import subprocess, sys, pathlib, xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
XML_PATH = ROOT / "artifacts" / "metrics" / "all_tests.xml"

print("Running full test suite...")
result = subprocess.run(
    [sys.executable, "-m", "pytest", "tests/", "--tb=line", "-q",
     f"--junitxml={XML_PATH}"],
    cwd=str(ROOT),
    capture_output=True, text=True, timeout=600,
)
print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
if result.stderr:
    print(result.stderr[-300:] if len(result.stderr) > 300 else result.stderr)

# Parse XML
tree = ET.parse(str(XML_PATH))
root = tree.getroot()
for suite in root.iter("testsuite"):
    tests = int(suite.attrib.get("tests", 0))
    fails = int(suite.attrib.get("failures", 0))
    errors = int(suite.attrib.get("errors", 0))
    skips = int(suite.attrib.get("skipped", 0))
    passed = tests - fails - errors - skips
    rate = passed / tests if tests else 0
    print(f"\n=== RESULTS ===")
    print(f"  Total:   {tests}")
    print(f"  Passed:  {passed}")
    print(f"  Failed:  {fails}")
    print(f"  Errors:  {errors}")
    print(f"  Skipped: {skips}")
    print(f"  Pass Rate: {rate:.1%}")

failures = []
for tc in root.iter("testcase"):
    fail = tc.find("failure")
    err = tc.find("error")
    skip = tc.find("skipped")
    if fail is not None:
        failures.append(f"FAIL: {tc.attrib.get('classname')}.{tc.attrib.get('name')}")
        failures.append(f"  {fail.attrib.get('message','')[:200]}")
    if err is not None:
        failures.append(f"ERROR: {tc.attrib.get('classname')}.{tc.attrib.get('name')}")
    if skip is not None:
        msg = skip.attrib.get("message", "")
        print(f"  SKIP: {tc.attrib.get('classname')}.{tc.attrib.get('name')}: {msg}")

if failures:
    print("\n=== FAILURES ===")
    for f in failures:
        print(f)
else:
    print("\n  NO FAILURES!")

sys.exit(0 if rate >= 0.95 else 1)
