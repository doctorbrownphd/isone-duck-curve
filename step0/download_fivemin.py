"""Download ISO-NE Real-Time Five-Minute System Load reports unchanged and log provenance.

Usage: python step0/download_fivemin.py [YYYY-MM [YYYY-MM]]

Defaults to 2024-06 through last month. The public CSV report starts on
2024-05-31 (evening), so 2024-06 is the first complete month.

One CSV per calendar month is saved byte for byte under
data/raw/isone_fivemin/year=YYYY/ and one row per file is appended to
manifest/downloads.csv, same columns as step0/download.py. The row count is
the number of data ("D") lines in the report.

The report page has to be visited first: ISO Express answers 403 to CSV
requests that arrive without its session cookie.
"""
import calendar
import csv
import hashlib
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_fivemin"
MANIFEST = ROOT / "manifest" / "downloads.csv"
PAGE = "https://www.iso-ne.com/isoexpress/web/reports/load-and-demand/-/tree/dmnd-five-minute-sys"
CSV_URL = "https://www.iso-ne.com/transform/csv/fiveminutesystemload?start={start}&end={end}"
FIRST = (2024, 6)


def months(first, last):
    y, m = first
    while (y, m) <= last:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def main(args):
    today = date.today()
    last = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    first = FIRST
    if args:
        first = tuple(int(x) for x in args[0].split("-"))
        last = tuple(int(x) for x in args[-1].split("-"))

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (capstone-research)"
    s.get(PAGE, timeout=120).raise_for_status()

    for y, m in months(first, last):
        start = f"{y}{m:02d}01"
        end = f"{y}{m:02d}{calendar.monthrange(y, m)[1]:02d}"
        url = CSV_URL.format(start=start, end=end)
        dest = RAW / f"year={y}" / f"rt_fiveminsysload_{start}_{end}.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)

        print(f"GET {url}")
        r = s.get(url, timeout=300)
        r.raise_for_status()
        if not r.content.startswith(b'"C"'):
            sys.exit(f"{url}: response is not the CSV report, stopping.")
        dest.write_bytes(r.content)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        rows = sum(1 for line in r.content.splitlines() if line.startswith(b'"D"'))
        sha = hashlib.sha256(r.content).hexdigest()
        with MANIFEST.open("a", newline="") as f:
            csv.writer(f).writerow(
                [url, str(dest.relative_to(ROOT)), ts, len(r.content), rows, sha,
                 "rows counted as 'D' lines in the report"]
            )
        print(f"  saved {dest.relative_to(ROOT)}  {len(r.content):,} bytes  {rows} rows")
        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv[1:])
