"""Download ISO-NE annual workbooks unchanged and log provenance.

Usage: python step0/download.py <url> [<url> ...]

Each file is saved byte for byte under data/raw/isone_zonal/year=YYYY/ with its
original file name. One row per file is appended to manifest/downloads.csv:
source URL, UTC timestamp, byte size, row count of the system sheet, and a
SHA-256 hash so we can prove later that the raw file was never altered.
"""
import csv
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_zonal"
MANIFEST = ROOT / "manifest" / "downloads.csv"
SYSTEM_SHEET_HINTS = ("ISO NE CA", "ISONE CA", "ISO-NE CA")


def system_sheet_rows(path: Path):
    """Row count of the system-wide sheet, or None if we can't find it."""
    xl = pd.ExcelFile(path)
    for name in xl.sheet_names:
        if name.strip().upper() in SYSTEM_SHEET_HINTS:
            return len(xl.parse(name)), name
    return None, None


def main(urls):
    for url in urls:
        fname = Path(urlparse(url).path).name
        m = re.search(r"(20\d{2})", fname)
        if not m:
            sys.exit(f"Can't find a year in file name {fname}; rename logic needs a look.")
        year = m.group(1)
        dest = RAW / f"year={year}" / fname
        dest.parent.mkdir(parents=True, exist_ok=True)

        print(f"GET {url}")
        r = requests.get(url, timeout=120, headers={"User-Agent": "capstone-research"})
        r.raise_for_status()
        dest.write_bytes(r.content)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        rows, sheet = system_sheet_rows(dest)
        sha = hashlib.sha256(r.content).hexdigest()
        with MANIFEST.open("a", newline="") as f:
            csv.writer(f).writerow(
                [url, str(dest.relative_to(ROOT)), ts, len(r.content), rows, sha,
                 f"rows counted on sheet '{sheet}'"]
            )
        print(f"  saved {dest.relative_to(ROOT)}  {len(r.content):,} bytes  "
              f"{rows} rows on sheet '{sheet}'")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
