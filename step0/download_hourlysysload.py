"""Download ISO-NE hourly system load (with native load) from the web services API.

Usage: python step0/download_hourlysysload.py [YYYY-MM-DD [YYYY-MM-DD]]

Defaults to 2019-06-01 through yesterday. The API has no data before the
evening of 2019-05-31, so 2019-06-01 is the first complete day.

Each day's JSON response is saved byte for byte under
data/raw/isone_hourlysysload/year=YYYY/ and one row per file is appended to
manifest/downloads.csv, same columns as step0/download.py. The row count is
the number of hourly records in the response. Days already on disk are
skipped, so an interrupted run can be restarted.

Needs a free ISO Express account. Put ISONE_USER and ISONE_PASS in .env at the
repo root (git-ignored).
"""
import csv
import hashlib
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_hourlysysload"
MANIFEST = ROOT / "manifest" / "downloads.csv"
URL = "https://webservices.iso-ne.com/api/v1.1/hourlysysload/day/{day}.json"
FIRST = date(2019, 6, 1)


def credentials():
    env = ROOT / ".env"
    if not env.exists():
        sys.exit("No .env at the repo root. It needs ISONE_USER and ISONE_PASS.")
    kv = dict(line.strip().split("=", 1) for line in env.read_text().splitlines() if "=" in line)
    return kv["ISONE_USER"], kv["ISONE_PASS"]


def main(args):
    first = date.fromisoformat(args[0]) if args else FIRST
    last = date.fromisoformat(args[-1]) if args else date.today() - timedelta(days=1)

    s = requests.Session()
    s.auth = credentials()
    s.headers["Accept"] = "application/json"

    saved = skipped = 0
    d = first
    while d <= last:
        day = d.strftime("%Y%m%d")
        dest = RAW / f"year={d.year}" / f"hourlysysload_{day}.json"
        d += timedelta(days=1)
        if dest.exists():
            skipped += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = URL.format(day=day)
        for attempt in range(4):
            try:
                r = s.get(url, timeout=120)
                if r.status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(5 * (attempt + 1))
        else:
            sys.exit(f"{url}: no good response after 4 tries, stopping. Rerun to resume.")

        body = json.loads(r.content).get("HourlySystemLoads") or {}
        rows = len(body.get("HourlySystemLoad", []))
        dest.write_bytes(r.content)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sha = hashlib.sha256(r.content).hexdigest()
        with MANIFEST.open("a", newline="") as f:
            csv.writer(f).writerow(
                [url, str(dest.relative_to(ROOT)), ts, len(r.content), rows, sha,
                 "rows counted as hourly records in the response"]
            )
        saved += 1
        if saved % 100 == 0:
            print(f"  {saved} saved, at {day}", flush=True)
        time.sleep(0.25)
    print(f"done: {saved} saved, {skipped} already on disk")


if __name__ == "__main__":
    main(sys.argv[1:])
