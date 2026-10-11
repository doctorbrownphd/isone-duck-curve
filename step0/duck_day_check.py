"""Step 0: does our duck day definition reproduce ISO-NE's published counts?

Definition under test: a date is a duck day when the hour with the lowest
real-time system demand (ISO NE control area sheet) falls between 9:00 a.m.
and 4:00 p.m. local prevailing time.

ISO-NE reports in "hour ending" (HE) form. HE 10 is the hour from 9:00 to
10:00. That makes "between 9:00 and 4:00" ambiguous at the edges, so we
count four readings of the window and report all of them:

  A  HE 10-16  hour must sit fully inside 9:00-16:00         (strict, primary)
  B  HE  9-16  treat the HE label itself as the clock hour
  C  HE 10-17  any hour that starts between 9:00 and 16:00
  D  HE  9-17  widest reading, union of B and C

Daylight saving: in spring one hour is missing (23 rows). In fall ISO-NE
repeats hour 2 and labels the second one "02X" (25 rows). Both are overnight
hours, so they compete for the daily minimum like any other hour; we keep
them and record them. They can't land inside the midday window.

Outputs in step0/output/:
  daily_min.csv        one row per date: min hour, min MW, flag per reading
  comparison.csv       year, period, our count per reading, ISO-NE count, diff
  duck_days_2018.csv   every 2018 date flagged under any reading
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_zonal"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

WINDOWS = {"A": (10, 16), "B": (9, 16), "C": (10, 17), "D": (9, 17)}
PRIMARY = "A"

# Published ISO-NE counts and the period each one covers.
# Sources: ISO Newswire 2018-05-03, 2024-11-26, 2025-06-18.
PUBLISHED = [
    (2018, "2018-01-01", "2018-12-31", 1, "Apr 21, 2018 was the first and only 2018 duck day"),
    (2022, "2022-01-01", "2022-12-31", 45, "ISO Newswire 2024-11-26"),
    (2023, "2023-01-01", "2023-12-31", 73, "ISO Newswire 2024-11-26"),
    (2024, "2024-01-01", "2024-11-26", 100, "100th day announced 2024-11-26; cutoff is approximate"),
    (2025, "2025-01-01", "2025-05-31", 69, "69 of first 151 days, ISO Newswire 2025-06-18"),
]


def find_col(df, *candidates):
    norm = {str(c).strip().lower().replace(" ", "_"): c for c in df.columns}
    for cand in candidates:
        if cand in norm:
            return norm[cand]
    return None


def load_year(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    sheet = next(s for s in xl.sheet_names if "ISO" in s.upper() and "CA" in s.upper())
    df = xl.parse(sheet)

    date_col = find_col(df, "date")
    hr_col = find_col(df, "hr_end", "hour_ending", "hour")
    # Real-time demand. Older workbooks may call it DEMAND instead of RT_Demand.
    rt_col = find_col(df, "rt_demand", "demand", "rt_demd")
    if None in (date_col, hr_col, rt_col):
        raise SystemExit(f"{path.name}: missing a needed column. Columns: {list(df.columns)}")

    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col]).dt.date,
        "hr_label": df[hr_col].astype(str).str.strip(),
        "rt_demand_mw": pd.to_numeric(df[rt_col], errors="coerce"),
    })
    out["repeated_hour"] = out["hr_label"].str.upper().str.endswith("X")
    out["he"] = pd.to_numeric(out["hr_label"].str.upper().str.rstrip("X"), errors="coerce").astype("Int64")
    out["source_file"] = path.name
    out["demand_col_used"] = rt_col
    return out


def main():
    files = sorted(RAW.glob("year=*/*.xls*"))
    if not files:
        raise SystemExit("No workbooks in data/raw/isone_zonal/. Run step0/download.py first.")

    hourly = pd.concat([load_year(p) for p in files], ignore_index=True)
    bad = hourly["rt_demand_mw"].isna() | hourly["he"].isna()
    print(f"hourly rows: {len(hourly):,}   non-numeric rows dropped: {int(bad.sum())}")
    print("demand column used per file:")
    print(hourly.groupby("source_file")["demand_col_used"].first().to_string())
    hourly = hourly[~bad]

    # Lowest demand hour per date. Ties keep the earliest hour; we count ties.
    idx = hourly.groupby("date")["rt_demand_mw"].idxmin()
    daily = hourly.loc[idx, ["date", "he", "hr_label", "rt_demand_mw"]].rename(
        columns={"he": "min_he", "hr_label": "min_hr_label", "rt_demand_mw": "min_mw"})
    ties = hourly.merge(daily[["date", "min_mw"]], on="date")
    ties = ties[ties["rt_demand_mw"] == ties["min_mw"]].groupby("date").size()
    daily["tied_min"] = daily["date"].map(ties).gt(1)
    daily["hours_in_day"] = daily["date"].map(hourly.groupby("date").size())
    for k, (lo, hi) in WINDOWS.items():
        daily[f"duck_{k}"] = daily["min_he"].between(lo, hi)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date")
    daily.to_csv(OUT / "daily_min.csv", index=False)

    print(f"days: {len(daily):,}   days with tied minimum: {int(daily['tied_min'].sum())}")
    print(f"days with 23 rows: {int((daily['hours_in_day'] == 23).sum())}   "
          f"25 rows: {int((daily['hours_in_day'] == 25).sum())}   "
          f"other != 24: {int((~daily['hours_in_day'].isin([23, 24, 25])).sum())}")

    rows = []
    for year, start, end, iso, note in PUBLISHED:
        sub = daily[(daily["date"] >= start) & (daily["date"] <= end)]
        if sub.empty:
            continue
        row = {"year": year, "period": f"{start} to {end}", "days_in_data": len(sub), "iso_ne_count": iso}
        for k in WINDOWS:
            row[f"ours_{k}"] = int(sub[f"duck_{k}"].sum())
        row["diff_primary"] = row[f"ours_{PRIMARY}"] - iso
        row["pct_diff_primary"] = round(100 * row["diff_primary"] / iso, 1)
        row["note"] = note
        rows.append(row)
    comp = pd.DataFrame(rows)
    comp.to_csv(OUT / "comparison.csv", index=False)

    print("\nComparison (primary reading A = HE 10-16):")
    print(comp.drop(columns=["note", "period"]).to_string(index=False))

    d18 = daily[(daily["date"].dt.year == 2018) & daily[[f"duck_{k}" for k in WINDOWS]].any(axis=1)]
    d18.to_csv(OUT / "duck_days_2018.csv", index=False)
    if not d18.empty or (daily["date"].dt.year == 2018).any():
        print("\n2018 dates flagged under any reading:")
        print(d18[["date", "min_he", "min_mw"] + [f"duck_{k}" for k in WINDOWS]].to_string(index=False)
              if not d18.empty else "  none")

    print("\nMinimum hour of day distribution (all days, hour ending):")
    print(daily["min_he"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
