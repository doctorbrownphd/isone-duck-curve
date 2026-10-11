"""Step 0c: duck day counts from ISO-NE's own hourly native load, 2019-06 onward.

Step 0b (duck_day_check_native.py) showed that hourly native load built from
five-minute samples reproduces ISO-NE's 2025 counts, but that report only
starts 2024-05-31. The web services API serves the ISO's hourly system load
with a NativeLoad field back to 2019-06-01. This script runs the same
definition on that series and compares it to every published figure.

Definition under test: a date is a duck day when the hour with the lowest
hourly native load is a daytime hour. Native load is Load minus ArdDemand
(asset related demand: pumped storage pumping and storage charging).

Three daytime windows are counted (hour ending):
  W1  HE  9-17  (8:00 to 17:00)   primary
  W2  HE 10-16  (9:00 to 16:00)   the original proposal wording
  W3  HE  8-18

Hour ending is the local clock hour of BeginDate plus one. On the fall
daylight saving day the repeated hour appears twice as HE 2 (two rows, 25 in
the day); the spring day has 23 rows. Both are overnight hours.

Outputs in step0/output/:
  hourly_native_load.csv      the parsed hourly series
  hourly_daily_min.csv        one row per date: min hour, min MW, overnight and
                              daytime minimum, margin, flag per window
  hourly_comparison.csv       published ISO-NE counts vs ours
  hourly_duck_days_by_year.csv
"""
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_hourlysysload"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

WINDOWS = {"W1": (9, 17), "W2": (10, 16), "W3": (8, 18)}
PRIMARY = "W1"
NIGHT = (1, 7)   # overnight hours used for the margin column only
NEAR_TIE_MW = 25

N20 = "ISO Newswire 2020-02-25"
N24A = "ISO Newswire 2024-02-28"
N24B = "ISO Newswire 2024-11-26"
N25A = "ISO Newswire 2025-06-18"
N25B = "ISO Newswire 2025-09-17"
N26A = "ISO Newswire 2026-01-13"
N26B = "ISO Newswire 2026-09-21"

# (label, start, end, ISO-NE count, source). Only periods the data covers in full.
PUBLISHED = [
    ("2022 full year", "2022-01-01", "2022-12-31", 45, N24A),
    ("2023 full year", "2023-01-01", "2023-12-31", 73, N24A),
    ("2023 Jan 1 to Jan 7", "2023-01-01", "2023-01-07", 0, f"first of 2023 on Jan 8, {N24A}"),
    ("2023 Jan 1 to Feb 20", "2023-01-01", "2023-02-20", 9, N24A),
    ("2023 Mar", "2023-03-01", "2023-03-31", 18, N24A),
    ("2023 May 26 to 29", "2023-05-26", "2023-05-29", 4, f"four in a row, {N24A}"),
    ("2023 Jun", "2023-06-01", "2023-06-30", 1, N24A),
    ("2023 Jul", "2023-07-01", "2023-07-31", 1, N24A),
    ("2023 Aug", "2023-08-01", "2023-08-31", 1, N24A),
    ("2023 Dec 23 to 31", "2023-12-23", "2023-12-31", 0, f"last of 2023 on Dec 22, {N24A}"),
    ("2024 Jan 1 to Feb 20", "2024-01-01", "2024-02-20", 14, N24A),
    ("2024 Jan 1 to May 31", "2024-01-01", "2024-05-31", 64, N25A),
    ("2024 Feb", "2024-02-01", "2024-02-29", 19, N24B),
    ("2024 Mar", "2024-03-01", "2024-03-31", 16, N24B),
    ("2024 Apr", "2024-04-01", "2024-04-30", 16, N24B),
    ("2024 Jul", "2024-07-01", "2024-07-31", 0, N24B),
    ("2024 Aug", "2024-08-01", "2024-08-31", 1, N24B),
    ("2024 Nov 1 to 25", "2024-11-01", "2024-11-25", 10, N24B),
    ("2024 Jan 1 to Nov 30", "2024-01-01", "2024-11-30", 100, f"100th of 2024 on Nov 30, {N25B}"),
    ("2024 full year", "2024-01-01", "2024-12-31", 107, N25A),
    ("2025 Jan 1 to May 31", "2025-01-01", "2025-05-31", 69, N25A),
    ("2025 Mar", "2025-03-01", "2025-03-31", 20, N25A),
    ("2025 Apr", "2025-04-01", "2025-04-30", 18, N25A),
    ("2025 May", "2025-05-01", "2025-05-31", 12, N25A),
    ("2025 Jul", "2025-07-01", "2025-07-31", 6, N25B),
    ("2025 Jan 1 to Sep 13", "2025-01-01", "2025-09-13", 100, f"100th of 2025 on Sep 13, {N25B}"),
    ("2025 full year", "2025-01-01", "2025-12-31", 134, N26A),
    ("2026 Jan 1 to Jan 8", "2026-01-01", "2026-01-08", 1, f"one so far, on Jan 8, {N26A}"),
    ("2026 Jan 1 to Sep 6", "2026-01-01", "2026-09-06", 100, f"100th of 2026 on Sep 6, {N26B}"),
    ("2026 Jan 1 to Sep 21", "2026-01-01", "2026-09-21", 107, f"107th of 2026 on Sep 21, {N26B}"),
]

# Dates ISO-NE has named as duck days.
NAMED_DUCK = [
    ("2020-02-15", N20), ("2020-02-22", N20), ("2020-02-23", N20),
    ("2023-01-08", N24A), ("2023-04-09", N24A), ("2023-12-22", N24A),
    ("2024-03-30", N24B), ("2024-04-27", N24B), ("2024-11-30", N25B),
    ("2025-04-20", N25A), ("2025-09-13", N25B),
    ("2026-01-08", N26A), ("2026-03-29", N26B), ("2026-09-06", N26B), ("2026-09-21", N26B),
]

# Published values for single days: (date, column, ISO-NE MW, source).
SPOT = [
    ("2023-04-09", "day_min_mw", 6814, N24A),
    ("2024-04-27", "day_min_mw", 6596, N24B),
    ("2024-03-30", "gap_mw", 3134, N24B),
    ("2025-04-20", "day_min_mw", 5318, N25A),
    ("2025-09-13", "day_min_mw", 8637, N25B),
    ("2025-09-13", "night_min_mw", 9321, N25B),
    ("2026-03-29", "night_min_mw", 11667, N26B),
    ("2026-03-29", "day_min_mw", 7261, N26B),
    ("2026-03-29", "gap_mw", 4406, N26B),
]


def load_hourly() -> pd.DataFrame:
    files = sorted(RAW.glob("year=*/*.json"))
    if not files:
        raise SystemExit("No files in data/raw/isone_hourlysysload/. Run step0/download_hourlysysload.py first.")
    recs = []
    for p in files:
        body = json.loads(p.read_text()).get("HourlySystemLoads") or {}
        for r in body.get("HourlySystemLoad", []):
            recs.append((r["BeginDate"], r.get("Load"), r.get("NativeLoad"), r.get("ArdDemand")))
    df = pd.DataFrame(recs, columns=["begin", "load_mw", "native_mw", "ard_mw"])
    local = pd.to_datetime(df["begin"].str[:19])      # local clock time, offset dropped
    df["date"] = local.dt.normalize()
    df["he"] = local.dt.hour + 1
    df["utc_offset"] = df["begin"].str[-6:]
    return df.sort_values(["date", "he", "begin"]).reset_index(drop=True)


def main():
    hourly = load_hourly()
    bad = hourly["native_mw"].isna()
    print(f"hourly rows: {len(hourly):,}   {hourly['date'].min().date()} to {hourly['date'].max().date()}   "
          f"rows with missing native load dropped: {int(bad.sum())}")
    hourly = hourly[~bad]
    hourly.drop(columns=["begin"]).to_csv(OUT / "hourly_native_load.csv", index=False)

    per_day = hourly.groupby("date").size()
    span = pd.date_range(hourly["date"].min(), hourly["date"].max())
    print(f"days: {len(per_day):,} of {len(span):,} in span   missing dates: "
          f"{[d.date().isoformat() for d in span.difference(per_day.index)]}")
    print(f"days without 24 rows: { {d.date().isoformat(): int(n) for d, n in per_day[per_day != 24].items()} }")
    resid = (hourly["load_mw"] - hourly["ard_mw"] - hourly["native_mw"]).abs()
    print(f"NativeLoad = Load - ArdDemand check: max abs residual {resid.max():.3f} MW")

    # Lowest native load hour per date. Ties keep the earliest hour.
    idx = hourly.groupby("date")["native_mw"].idxmin()
    daily = hourly.loc[idx, ["date", "he", "native_mw"]].rename(
        columns={"he": "min_he", "native_mw": "min_mw"}).set_index("date")
    ties = hourly.merge(daily[["min_mw"]], left_on="date", right_index=True)
    ties = ties[ties["native_mw"] == ties["min_mw"]].groupby("date").size()
    daily["tied_min"] = ties.reindex(daily.index).gt(1)
    lo, hi = WINDOWS[PRIMARY]
    daily["night_min_mw"] = hourly[hourly["he"].between(*NIGHT)].groupby("date")["native_mw"].min()
    daily["day_min_mw"] = hourly[hourly["he"].between(lo, hi)].groupby("date")["native_mw"].min()
    daily["gap_mw"] = daily["night_min_mw"] - daily["day_min_mw"]     # positive means daytime is lower
    daily["near_tie"] = daily["gap_mw"].abs() < NEAR_TIE_MW
    daily["hours_in_day"] = per_day
    for k, (a, b) in WINDOWS.items():
        daily[f"duck_{k}"] = daily["min_he"].between(a, b)
    daily = daily[daily["hours_in_day"] >= 23]       # drops a partial first day if present
    daily.round(1).to_csv(OUT / "hourly_daily_min.csv")
    print(f"days with tied minimum: {int(daily['tied_min'].sum())}")

    rows = []
    for label, start, end, iso, src in PUBLISHED:
        sub = daily.loc[start:end]
        row = {"period": label, "days": len(sub), "iso_ne": iso}
        for k in WINDOWS:
            row[f"ours_{k}"] = int(sub[f"duck_{k}"].sum())
        row["diff_primary"] = row[f"ours_{PRIMARY}"] - iso
        row["near_ties_in_period"] = int(sub["near_tie"].sum())
        row["source"] = src
        rows.append(row)
    comp = pd.DataFrame(rows)
    comp.to_csv(OUT / "hourly_comparison.csv", index=False)
    print(f"\nComparison (primary window {PRIMARY} = HE {lo}-{hi}):")
    print(comp.drop(columns=["source"]).to_string(index=False))
    exact = int((comp["diff_primary"] == 0).sum())
    print(f"exact matches: {exact} of {len(comp)}   largest miss: {int(comp['diff_primary'].abs().max())}")

    duck = daily[f"duck_{PRIMARY}"]
    print("\nDates ISO-NE has named as duck days:")
    for d, src in NAMED_DUCK:
        r = daily.loc[d]
        print(f"  {d}  ours: {'duck' if duck.loc[d] else 'NOT duck'}   min HE {int(r['min_he']):2d}   "
              f"gap {r['gap_mw']:8.1f} MW   ({src})")

    print("\nSingle-day values, ours vs ISO-NE:")
    for d, col, iso, src in SPOT:
        ours = daily.loc[d, col]
        print(f"  {d}  {col:13}  ours {ours:8.0f}   ISO-NE {iso:6d}   diff {ours - iso:+5.0f}   ({src})")

    print(f"\nDays within {NEAR_TIE_MW} MW of a tie between daytime and overnight minimum:")
    print(daily[daily["near_tie"]][["min_he", "night_min_mw", "day_min_mw", "gap_mw", f"duck_{PRIMARY}"]]
          .round(1).to_string())

    for year, n, iso_date in [(2024, 100, "2024-11-30"), (2025, 100, "2025-09-13"), (2026, 100, "2026-09-06")]:
        c = duck.loc[f"{year}-01-01":f"{year}-12-31"].cumsum()
        hit = c[c == n]
        print(f"{n}th duck day of {year}: ours {hit.index[0].date() if len(hit) else 'not reached'}   ISO-NE {iso_date}")

    by_year = pd.DataFrame({k: daily[f"duck_{k}"].groupby(daily.index.year).sum() for k in WINDOWS})
    by_year.insert(0, "days", daily.groupby(daily.index.year).size())
    by_year.index.name = "year"
    by_year.to_csv(OUT / "hourly_duck_days_by_year.csv")
    print("\nDuck days by year (2019 starts Jun 1):")
    print(by_year.to_string())
    pre22 = int(duck.loc[:"2021-12-31"].sum())
    print(f"\n2019-06-01 to 2021-12-31: ours {pre22}. ISO-NE reports 34 from 2018-04-21 through 2021, "
          f"which includes Apr 21, 2018 and any in Jan to May 2019, so ours should be at most 33.")

    print("\nMinimum hour of day distribution (hour ending):")
    print(daily["min_he"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
