"""Extract: download NOAA Local Climatological Data (LCD v2) station-year files.

Usage:
  python pipeline/extract_noaa_lcd.py --dry-run    list what would be downloaded
  python pipeline/extract_noaa_lcd.py              download

What it does, in order:
  1. Builds the list of files: six airports (one per New England state) for
     each year from 2019 to 2026. That is 48 files of about 10 MB each.
  2. Skips any file already on disk, so a stopped run can be restarted.
  3. Downloads each file and saves it byte for byte, with NOAA's own file
     name, under data/raw/noaa_lcd/station=<id>/year=YYYY/. This mirrors the
     S3 layout raw/noaa_lcd/station=<id>/year=YYYY/ so the folder can be
     copied to the bucket unchanged.
  4. Appends one row per file to manifest/downloads.csv: source URL, local
     path, download time (UTC), byte size, row count, and a SHA-256 hash that
     proves later that the raw file was never altered.

Nothing is parsed or cleaned here. LCD files mix hourly, daily and monthly
rows, report times in local standard time, and give temperature in degrees
Celsius. All of that is handled downstream in the cleaning step.

Dataset citation: Kantor et al. (2023), https://doi.org/10.25921/96dw-mb77
"""
import csv
import hashlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "noaa_lcd"
MANIFEST = ROOT / "manifest" / "downloads.csv"
URL = "https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access/{year}/LCD_{station}_{year}.csv"

YEARS = range(2019, 2027)
# One major airport per state. Station ids are NOAA's GHCN-style ids.
STATIONS = {
    "USW00014739": "Boston Logan, MA",
    "USW00014740": "Hartford Bradley, CT",
    "USW00014765": "Providence T.F. Green, RI",
    "USW00014764": "Portland Jetport, ME",
    "USW00014745": "Concord Municipal, NH",
    "USW00014742": "Burlington, VT",
}


def download(session, url: str, dest: Path):
    """Save the file unchanged. Returns (bytes, rows, sha256)."""
    for attempt in range(4):
        try:
            r = session.get(url, timeout=600)
            if r.status_code == 200 and r.content.startswith(b'"STATION"'):
                break
        except requests.RequestException:
            pass
        time.sleep(10 * (attempt + 1))
    else:
        sys.exit(f"{url}: no good response after 4 tries, stopping. Run again to resume.")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    rows = max(r.content.count(b"\n") - 1, 0)      # lines after the header
    return len(r.content), rows, hashlib.sha256(r.content).hexdigest()


def main(dry_run: bool):
    session = requests.Session()
    session.headers["User-Agent"] = "capstone-research"
    saved = have = 0
    total_bytes = total_rows = 0
    for station, name in STATIONS.items():
        for year in YEARS:
            url = URL.format(station=station, year=year)
            dest = RAW / f"station={station}" / f"year={year}" / f"LCD_{station}_{year}.csv"
            rel = dest.relative_to(ROOT)
            if dest.exists():
                have += 1
                print(f"have   {rel}")
                continue
            if dry_run:
                print(f"would get  {url}\n      ->   {rel}")
                continue
            size, rows, sha = download(session, url, dest)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            with MANIFEST.open("a", newline="") as f:
                csv.writer(f).writerow([url, str(rel), ts, size, rows, sha,
                                        f"{name}; rows counted as CSV lines after the header"])
            saved += 1
            total_bytes += size
            total_rows += rows
            print(f"saved  {rel}  {name}  {size:,} bytes  {rows:,} rows")
            time.sleep(1)
    if dry_run:
        print(f"\ndry run: {len(STATIONS) * len(YEARS) - have} files to download, {have} already on disk")
    else:
        print(f"\ndone: {saved} files saved ({total_bytes:,} bytes, {total_rows:,} rows), {have} already on disk")
        print(f"manifest: {MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main("--dry-run" in sys.argv[1:])
