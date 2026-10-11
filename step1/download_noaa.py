"""Download NOAA NCEI weather for ISO-NE's eight weather stations and log provenance.

Usage: python step1/download_noaa.py

The stations are the ones ISO-NE uses for its own system weather values
(Notes sheet of the SMD hourly workbook): Boston, Bridgeport, Burlington,
Concord, Portland, Providence, Windsor Locks, Worcester.

Two NCEI Access Data Service datasets, saved byte for byte as returned:
  daily-summaries   one CSV per station for the whole study period, limited
                    to temperature, wind, precipitation and snow fields (the
                    all-fields request takes many minutes per station and
                    stalls)
                    data/raw/noaa_daily/<code>_<ghcnd id>_core.csv
  global-hourly     one CSV per station per calendar year (times are UTC),
                    limited to sky cover, temperature, dew point, visibility
                    and precipitation fields
                    data/raw/noaa_hourly/year=YYYY/<code>_<isd id>_YYYY.csv

The global-hourly service stops at 2025-08-27. NOAA's replacement, GHCNh, is
downloaded for 2025 and 2026 as whole station-year files (all fields):
  GHCNh             data/raw/noaa_ghcnh/year=YYYY/GHCNh_<ghcnd id>_YYYY.psv
2025 is in both sources so the two can be compared over Jan to Aug 2025.

One row per file is appended to manifest/downloads.csv, same columns as
step0/download.py. The row count is the number of data rows in the CSV.
Files already on disk are skipped.
"""
import csv
import hashlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
MANIFEST = ROOT / "manifest" / "downloads.csv"
API = "https://www.ncei.noaa.gov/access/services/data/v1"

START, END = "2019-06-01", "2026-09-30"
# Hourly data is in UTC, so one extra day covers the last local evening.
HOURLY_END = "2026-10-01"
DAILY_TYPES = "TMAX,TMIN,TAVG,AWND,PRCP,SNOW,SNWD"
GHCNH = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{year}/psv/{name}"
GHCNH_YEARS = (2025, 2026)
HOURLY_TYPES = "GF1,GA1,GA2,GA3,GA4,GD1,GD2,GD3,GD4,TMP,DEW,VIS,AA1"

# code: (GHCN-Daily id, ISD id = USAF + WBAN)
STATIONS = {
    "BOS": ("USW00014739", "72509014739"),
    "BDR": ("USW00094702", "72504094702"),
    "BTV": ("USW00014742", "72617014742"),
    "CON": ("USW00014745", "72605014745"),
    "PWM": ("USW00014764", "72606014764"),
    "PVD": ("USW00014765", "72507014765"),
    "BDL": ("USW00014740", "72508014740"),
    "ORH": ("USW00094746", "72510094746"),
}


def fetch(session, params, dest: Path, url=API, starts=b'"STATION"'):
    if dest.exists():
        print(f"  have {dest.relative_to(ROOT)}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = requests.Request("GET", url, params=params).prepare()
    for attempt in range(4):
        try:
            r = session.send(req, timeout=600)
            if r.status_code == 200 and r.content.startswith(starts):
                break
        except requests.RequestException:
            pass
        time.sleep(10 * (attempt + 1))
    else:
        sys.exit(f"{req.url}: no good response after 4 tries, stopping. Rerun to resume.")
    dest.write_bytes(r.content)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = max(r.content.count(b"\n") - 1, 0)
    sha = hashlib.sha256(r.content).hexdigest()
    with MANIFEST.open("a", newline="") as f:
        csv.writer(f).writerow(
            [req.url, str(dest.relative_to(ROOT)), ts, len(r.content), rows, sha,
             "rows counted as CSV lines after the header"]
        )
    print(f"  saved {dest.relative_to(ROOT)}  {len(r.content):,} bytes  {rows} rows")
    time.sleep(1)


def main():
    s = requests.Session()
    s.headers["User-Agent"] = "capstone-research"
    for year in range(int(START[:4]), int(HOURLY_END[:4]) + 1):
        a = max(f"{year}-01-01", START)
        b = min(f"{year}-12-31", HOURLY_END)
        for code, (ghcnd, isd) in STATIONS.items():
            fetch(s, {"dataset": "global-hourly", "stations": isd, "startDate": a, "endDate": b,
                      "dataTypes": HOURLY_TYPES, "format": "csv", "includeStationName": "true"},
                  RAW / "noaa_hourly" / f"year={year}" / f"{code}_{isd}_{year}.csv")
    for year in GHCNH_YEARS:
        for code, (ghcnd, isd) in STATIONS.items():
            name = f"GHCNh_{ghcnd}_{year}.psv"
            fetch(s, None, RAW / "noaa_ghcnh" / f"year={year}" / name,
                  url=GHCNH.format(year=year, name=name), starts=b"STATION|")
    for code, (ghcnd, isd) in STATIONS.items():
        fetch(s, {"dataset": "daily-summaries", "stations": ghcnd, "startDate": START, "endDate": END,
                  "dataTypes": DAILY_TYPES, "format": "csv", "includeStationName": "true", "units": "standard"},
              RAW / "noaa_daily" / f"{code}_{ghcnd}_core.csv")


if __name__ == "__main__":
    main()
