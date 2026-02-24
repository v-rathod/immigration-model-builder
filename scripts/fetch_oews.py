#!/usr/bin/env python3
"""
Section A: Attempt to fetch OEWS 2024 from official BLS endpoints.
If the local zip is corrupt or the fetch fails, logs the failure
so make_oews_2024_fallback.py can take over.

Outputs:
  artifacts/metrics/fetch_oews.log  (always)
  returns exit code 0 if data was obtained, 1 if failed (caller should trigger fallback)
"""
import hashlib
import logging
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
METRICS = ROOT / "artifacts" / "metrics"
METRICS.mkdir(parents=True, exist_ok=True)
LOG_PATH = METRICS / "fetch_oews.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

BLS_URLS = [
    "https://www.bls.gov/oes/special.requests/oesm24all.zip",
    "https://www.bls.gov/oes/special.requests/oews_all_data_2024.zip",
    "https://www.bls.gov/oes/2024/may/national/excel/oesm24all.zip",
]


def _valid_zip(path: Path) -> bool:
    import zipfile
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            return len(names) > 0
    except Exception:
        return False


def main() -> int:
    log_lines = [f"=== fetch_oews {datetime.now(timezone.utc).isoformat()} ==="]

    import yaml
    try:
        with open("configs/paths.yaml") as f:
            paths = yaml.safe_load(f)
        data_root = Path(paths["data_root"])
    except Exception:
        data_root = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")

    local_2024 = data_root / "BLS_OEWS" / "2024" / "oews_all_data_2024.zip"

    # Check if local copy is already valid
    if local_2024.exists() and _valid_zip(local_2024):
        log_lines.append(f"PASS: local OEWS 2024 zip is valid at {local_2024}")
        log_lines.append("Status: OFFICIAL_OK")
        LOG_PATH.write_text("\n".join(log_lines) + "\n")
        return 0

    log_lines.append(f"Local OEWS 2024 at {local_2024}: corrupt or missing")
    log_lines.append("Attempting official BLS download...")

    local_2024.parent.mkdir(parents=True, exist_ok=True)
    tmp = local_2024.parent / "oews_all_data_2024.zip.tmp"

    for url in BLS_URLS:
        log_lines.append(f"  Trying: {url}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
            tmp.write_bytes(data)
            if _valid_zip(tmp):
                tmp.rename(local_2024)
                log_lines.append(f"  SUCCESS: downloaded {len(data):,} bytes, valid zip")
                log_lines.append("Status: OFFICIAL_OK")
                LOG_PATH.write_text("\n".join(log_lines) + "\n")
                return 0
            else:
                log_lines.append(f"  FAIL: downloaded but invalid zip ({len(data)} bytes)")
                tmp.unlink(missing_ok=True)
        except Exception as e:
            log_lines.append(f"  FAIL: {type(e).__name__}: {e}")

    log_lines.append("Status: FETCH_FAILED — all BLS endpoints failed or returned corrupt data")
    log_lines.append("Action: make_oews_2024_fallback.py will produce a synthetic ref_year=2024 record")
    LOG_PATH.write_text("\n".join(log_lines) + "\n")

    # Print log path for chaining
    print(f"FETCH_FAILED — see {LOG_PATH}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
