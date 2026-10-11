"""Join the response, calendar, weather and installed solar into one daily table.

Inputs:
  step1/output/daily_response_calendar.csv   (build_response_calendar.py)
  step1/output/weather_daily.csv             (build_weather_daily.py)
  reference/pv_installed_by_year.csv         ISO-NE year-end installed PV
  reference/pv_forecast_year_end.csv         ISO-NE forecast for year-end 2026

Output: step1/output/model_table.csv, one row per date, 2019-06-01 to 2026-09-29.

Installed solar per day
  ISO-NE publishes installed PV nameplate once a year, as of December 31.
  pv_installed_mw is a straight line between consecutive year-end values.
  2026 has no year-end survey yet, so the line runs from the year-end 2025
  value to ISO-NE's own forecast for year-end 2026; pv_is_forecast marks
  those rows. Real installations do not arrive evenly through the year, so
  this is a trend measure, not a daily measurement.

complete_case is 1 when no predictor is missing.
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / "output"

PREDICTORS = ["dow", "is_weekend", "is_holiday", "is_easter", "month", "day_of_year", "day_length_hours",
              "tmax_f", "tmin_f", "tmean_f", "hdd65", "cdd65", "wind_mph", "prcp_in", "snow_in",
              "sky_cover_day", "pv_installed_mw"]


def daily_pv(index: pd.DatetimeIndex) -> pd.DataFrame:
    inst = pd.read_csv(ROOT / "reference" / "pv_installed_by_year.csv", parse_dates=["as_of"])
    fc = pd.read_csv(ROOT / "reference" / "pv_forecast_year_end.csv", parse_dates=["as_of"])
    pts = pd.concat([inst[["as_of", "new_england_mwac"]], fc[["as_of", "new_england_mwac"]]])
    pts = pts.set_index("as_of")["new_england_mwac"].sort_index()
    if index.min() < pts.index.min() or index.max() > pts.index.max():
        raise SystemExit("Study period runs outside the year-end PV values.")
    line = pts.reindex(pts.index.union(index)).interpolate(method="time").reindex(index)
    last_installed = inst["as_of"].max()
    return pd.DataFrame({"pv_installed_mw": line.round(1), "pv_is_forecast": (index > last_installed).astype(int)},
                        index=index)


def main():
    resp = pd.read_csv(OUT / "daily_response_calendar.csv", parse_dates=["date"]).set_index("date")
    wx = pd.read_csv(OUT / "weather_daily.csv", parse_dates=["date"]).set_index("date")
    table = resp.join(wx, how="left").join(daily_pv(resp.index))
    table["complete_case"] = table[PREDICTORS].notna().all(axis=1).astype(int)
    table.to_csv(OUT / "model_table.csv")

    print(f"rows: {len(table):,}   columns: {table.shape[1]}   duck days: {int(table['duck_day'].sum())}")
    print(f"complete cases: {int(table['complete_case'].sum()):,}   "
          f"duck days among them: {int(table.loc[table['complete_case'] == 1, 'duck_day'].sum())}")
    miss = table[PREDICTORS].isna().sum()
    print("missing values by predictor:", miss[miss > 0].to_dict())
    print("dates with a missing predictor:",
          [d.date().isoformat() for d in table.index[table["complete_case"] == 0]])
    print("\ninstalled PV on Jan 1 and Jul 1 of each year:")
    print(table.loc[(table.index.day == 1) & table.index.month.isin([1, 7]), ["pv_installed_mw", "pv_is_forecast"]].to_string())
    print("\nmeans by duck day:")
    cols = ["sky_cover_day", "tmean_f", "hdd65", "cdd65", "wind_mph", "prcp_in", "pv_installed_mw",
            "is_weekend", "is_holiday", "day_length_hours"]
    print(table[table["complete_case"] == 1].groupby("duck_day")[cols].mean().round(3).T.to_string())


if __name__ == "__main__":
    main()
