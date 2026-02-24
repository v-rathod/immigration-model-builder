#!/usr/bin/env python3
"""Parse JUnit XML and print a summary."""
import xml.etree.ElementTree as ET
import sys

xml_path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/metrics/fast_tests.xml"
tree = ET.parse(xml_path)
root = tree.getroot()

for suite in root.iter("testsuite"):
    t = int(suite.attrib.get("tests", 0))
    f = int(suite.attrib.get("failures", 0))
    e = int(suite.attrib.get("errors", 0))
    s = int(suite.attrib.get("skipped", 0))
    p = t - f - e - s
    rate = p / t if t else 0
    print(f"tests={t}  passed={p}  failed={f}  errors={e}  skipped={s}  rate={rate:.1%}")

failures = []
skips = []
for tc in root.iter("testcase"):
    cn = tc.attrib.get("classname", "")
    nm = tc.attrib.get("name", "")
    fail = tc.find("failure")
    err = tc.find("error")
    skip = tc.find("skipped")
    if fail is not None:
        msg = fail.attrib.get("message", "")[:200]
        failures.append(f"  FAIL: {cn}::{nm}")
        failures.append(f"    {msg}")
    if err is not None:
        failures.append(f"  ERROR: {cn}::{nm}")
    if skip is not None:
        msg = skip.attrib.get("message", "")[:100]
        skips.append(f"  SKIP: {cn}::{nm}: {msg}")

if failures:
    print("\nFAILURES:")
    for line in failures:
        print(line)
else:
    print("\nNO FAILURES!")

if skips:
    print("\nSKIPPED:")
    for line in skips:
        print(line)
