"""Step 0b: duck day counts from hourly native load vs ISO-NE's published counts.

duck_day_check.py showed that the settlement workbook (SMD RT_Demand or
System_Load) cannot reproduce ISO-NE's counts. This script tests the series
that does: native load from the Real-Time Five-Minute System Load report.

  Total Load   demand seen by the control room. Output from solar that the ISO
               does not monitor in real time (including Settlement Only
               Generators) shows up as lower demand, not as generation.
  Native Load  Total Load minus Asset Related Load (pumped storage pumping and
               other storage charging).

Definition under test: a date is a duck day when the hour with the lowest
hourly native load is a daytime hour. Hourly native load is the mean of the
twelve five-minute samples stamped hh:00 to hh:55, labelled hour ending hh+1.
This reproduces the ISO's Real-Time Hourly System Load report for Total Load.

Three daytime windows are counted (hour ending):
  W1  HE  9-17  (8:00 to 17:00)   primary
  W2  HE 10-16  (9:00 to 16:00)   the original proposal wording
  W3  HE  8-18

Daylight saving: the spring day has 23 hours of samples and the fall day 25.
The repeated fall hour shares its clock label, so its 24 samples are averaged
into one HE 2 value. Both are overnight hours.

Outputs in step0/output/:
  native_daily_min.csv    one row per date: min hour, min MW, overnight and
                          daytime minimum, margin, flag per window
  native_comparison.csv   every published ISO-NE figure we can test, our count
                          per window, and the difference
"""
import io
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_fivemin"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

WINDOWS = {"W1": (9, 17), "W2": (10, 16), "W3": (8, 18)}
PRIMARY = "W1"
NIGHT = (1, 7)   # overnight hours used for the margin column only
NEAR_TIE_MW = 25

N24 = "ISO Newswire 2024-11-26"
N25A = "ISO Newswire 2025-06-18"
N25B = "ISO Newswire 2025-09-17"
N26A = "ISO Newswire 2026-01-13"
N26B = "ISO Newswire 2026-09-21"

# (label, start, end, ISO-NE count, source). Only periods the data covers in full.
PUBLISHED = [
    ("2024 Jun 1 to Dec 31", "2024-06-01", "2024-12-31", 43, f"107 for the year less 64 before June, {N25A}"),
    ("2024 Jun 1 to Nov 30", "2024-06-01", "2024-11-30", 36, f"100th of 2024 on Nov 30 less 64 before June, {N25B}"),
    ("2024 Jul", "2024-07-01", "2024-07-31", 0, N24),
    ("2024 Aug", "2024-08-01", "2024-08-31", 1, N24),
    ("2024 Nov 1 to 25", "2024-11-01", "2024-11-25", 10, N24),
    ("2025 Jan 1 to May 31", "2025-01-01", "2025-05-31", 69, N25A),
    ("2025 Mar", "2025-03-01", "2025-03-31", 20, N25A),
    ("2025 Apr", "2025-04-01", "2025-04-30", 18, N25A),
    ("2025 May", "2025-05-01", "2025-05-31", 12, N25A),
    ("2025 Jul", "2025-07-01", "2025-07-31", 6, N25B),
    ("2025 Jan 1 to Sep 13", "2025-01-01", "2025-09-13", 100, f"100th of 2025 on Sep 13, {N25B}"),
    ("2025 full year", "2025-01-01", "2025-12-31", 134, N26A),
    ("2026 Jan 1 to Jan 8", "2026-01-01", "2026-01-08", 1, f"one duck day so far, on Jan 8, {N26A}"),
    ("2026 Jan 1 to Sep 6", "2026-01-01", "2026-09-06", 100, f"100th of 2026 on Sep 6, {N26B}"),
    ("2026 Jan 1 to Sep 21", "2026-01-01", "2026-09-21", 107, f"107th of 2026 on Sep 21, {N26B}"),
]

# Published values for single days: (date, what, ISO-NE MW, source).
SPOT = [
    ("2025-04-20", "day_min", 5318, N25A),
    ("2025-09-13", "day_min", 8637, N25B),
    ("2025-09-13", "night_min", 9321, N25B),
    ("2026-03-29", "night_min", 11667, N26B),
    ("2026-03-29", "day_min", 7261, N26B),
]


def load_fivemin() -> pd.DataFrame:
    files = sorted(RAW.glob("year=*/*.csv"))
    if not files:
        raise SystemExit("No files in data/raw/isone_fivemin/. Run step0/download_fivemin.py first.")
    lines = []
    for p in files:
        lines += [ln for ln in p.read_text().splitlines() if ln.startswith('"D"')]
    df = pd.read_csv(
        io.StringIO("\n".join(lines)), header=None,
        names=["rec", "ts", "total_mw", "native_mw", "asset_related_mw", "total_with_solar", "native_with_solar"],
    )
    df["ts"] = pd.to_datetime(df["ts"], format="%m/%d/%Y %H:%M:%S")
    return df


def main():
    five = load_fivemin()
    bad = five[["total_mw", "native_mw"]].isna().any(axis=1)
    print(f"five-minute rows: {len(five):,}   {five['ts'].min()} to {five['ts'].max()}   "
          f"rows with missing load dropped: {int(bad.sum())}")
    five = five[~bad]
    five["date"] = five["ts"].dt.normalize()
    five["he"] = five["ts"].dt.hour + 1

    per_day = five.groupby("date").size()
    print(f"days: {len(per_day):,}   days without 288 samples: "
          f"{ {d.date().isoformat(): int(n) for d, n in per_day[per_day != 288].items()} }")

    hourly = five.groupby(["date", "he"], as_index=False)[["total_mw", "native_mw"]].mean()

    # Lowest native load hour per date. Ties keep the earliest hour.
    idx = hourly.groupby("date")["native_mw"].idxmin()
    daily = hourly.loc[idx, ["date", "he", "native_mw"]].rename(
        columns={"he": "min_he", "native_mw": "min_mw"}).set_index("date")
    night = hourly[hourly["he"].between(*NIGHT)].groupby("date")["native_mw"].min()
    lo, hi = WINDOWS[PRIMARY]
    day = hourly[hourly["he"].between(lo, hi)].groupby("date")["native_mw"].min()
    daily["night_min_mw"] = night
    daily["day_min_mw"] = day
    daily["margin_mw"] = day - night          # negative means daytime is lower
    daily["near_tie"] = daily["margin_mw"].abs() < NEAR_TIE_MW
    for k, (a, b) in WINDOWS.items():
        daily[f"duck_{k}"] = daily["min_he"].between(a, b)
    daily.round(1).to_csv(OUT / "native_daily_min.csv")

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
    comp.to_csv(OUT / "native_comparison.csv", index=False)
    print(f"\nComparison (primary window {PRIMARY} = HE {lo}-{hi}):")
    print(comp.drop(columns=["source"]).to_string(index=False))

    print("\nSingle-day values, ours vs ISO-NE:")
    for d, what, iso, src in SPOT:
        ours = daily.loc[d, f"{what}_mw"]
        print(f"  {d}  {what:9}  ours {ours:9.0f}   ISO-NE {iso:6d}   diff {ours - iso:+6.0f}   ({src})")

    print(f"\nDays within {NEAR_TIE_MW} MW of a tie between daytime and overnight minimum:")
    print(daily[daily["near_tie"]][["min_he", "night_min_mw", "day_min_mw", "margin_mw", f"duck_{PRIMARY}"]]
          .round(1).to_string())

    duck = daily[f"duck_{PRIMARY}"]
    c25 = duck.loc["2025-01-01":"2025-12-31"].cumsum()
    print(f"\n100th duck day of 2025 falls on {c25[c25 == 100].index[0].date()} (ISO-NE: 2025-09-13)")
    for a, b in [("2025-03-07", "2025-03-15"), ("2025-04-17", "2025-04-25")]:
        print(f"nine-day run {a} to {b}: all duck days = {bool(duck.loc[a:b].all())}, "
              f"day before = {bool(duck.loc[pd.Timestamp(a) - pd.Timedelta(days=1)])}, "
              f"day after = {bool(duck.loc[pd.Timestamp(b) + pd.Timedelta(days=1)])}")

    print("\nMinimum hour of day distribution (hour ending):")
    print(daily["min_he"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
