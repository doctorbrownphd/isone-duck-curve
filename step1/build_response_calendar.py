"""Build the daily response table with calendar features.

Input:  step0/output/hourly_daily_min.csv (from step0/duck_day_check_hourly.py)
Output: step1/output/daily_response_calendar.csv, one row per date in the
        study period 2019-06-01 to 2026-09-29.

Response: duck_day, the decided definition (lowest hourly native load falls in
hour ending 9 to 17). See results/decisions.md.

Calendar features:
  dow, is_weekend       day of week (0 = Monday)
  is_holiday, holiday   US federal holidays from the `holidays` package,
                        observed dates included
  is_easter             Easter Sunday (not a federal holiday, kept as its own
                        flag because ISO-NE singles it out for low demand)
  month, day_of_year
  day_length_hours      sunrise to sunset at 42.3 N (central Massachusetts),
                        from solar declination; no weather in it
"""
from pathlib import Path

import holidays
from dateutil.easter import easter
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "step0" / "output" / "hourly_daily_min.csv"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

START, END = "2019-06-01", "2026-09-29"
LATITUDE = 42.3


def day_length_hours(doy: pd.Series, lat_deg: float) -> pd.Series:
    decl = np.radians(23.44) * np.sin(2 * np.pi * (284 + doy) / 365)
    cos_h = -np.tan(np.radians(lat_deg)) * np.tan(decl)
    return 24 / np.pi * np.arccos(np.clip(cos_h, -1, 1))


def main():
    if not SRC.exists():
        raise SystemExit("Run step0/duck_day_check_hourly.py first.")
    d = pd.read_csv(SRC, parse_dates=["date"]).set_index("date").loc[START:END]
    expected = pd.date_range(START, END)
    missing = expected.difference(d.index)
    if len(missing):
        raise SystemExit(f"{len(missing)} dates missing from the response, first {missing[0].date()}")

    out = pd.DataFrame(index=d.index)
    out["duck_day"] = d["duck_W1"].astype(int)
    out["min_he"] = d["min_he"]
    out["min_mw"] = d["min_mw"]
    out["night_min_mw"] = d["night_min_mw"]
    out["day_min_mw"] = d["day_min_mw"]
    out["gap_mw"] = d["gap_mw"]
    out["near_tie"] = d["near_tie"].astype(int)

    out["dow"] = out.index.dayofweek
    out["is_weekend"] = (out["dow"] >= 5).astype(int)
    us = holidays.US(years=range(out.index.year.min(), out.index.year.max() + 1), observed=True)
    out["holiday"] = [us.get(x.date(), "") for x in out.index]
    out["is_holiday"] = (out["holiday"] != "").astype(int)
    easters = {pd.Timestamp(easter(y)) for y in range(out.index.year.min(), out.index.year.max() + 1)}
    out["is_easter"] = out.index.isin(easters).astype(int)
    out["month"] = out.index.month
    out["day_of_year"] = out.index.dayofyear
    out["day_length_hours"] = day_length_hours(out["day_of_year"], LATITUDE).round(3)

    out.to_csv(OUT / "daily_response_calendar.csv")
    print(f"rows: {len(out):,}   duck days: {int(out['duck_day'].sum())}   holidays: {int(out['is_holiday'].sum())}")
    print("\nduck day rate by day of week (0 = Monday):")
    print(out.groupby("dow")["duck_day"].agg(["sum", "mean"]).round(3).to_string())
    print("\nduck day rate by month:")
    print(out.groupby("month")["duck_day"].agg(["sum", "mean"]).round(3).to_string())
    print("\nduck day rate on holidays vs other days:")
    print(out.groupby("is_holiday")["duck_day"].agg(["count", "sum", "mean"]).round(3).to_string())
    print("\nEaster Sundays:")
    print(out.loc[out["is_easter"] == 1, ["duck_day", "min_he", "gap_mw"]].to_string())
    print("\nholidays found:", sorted(set(out.loc[out['is_holiday'] == 1, 'holiday'])))


if __name__ == "__main__":
    main()
