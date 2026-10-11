"""Build daily weather features for New England from the eight ISO-NE stations.

Inputs:
  data/raw/noaa_daily/<code>_<id>_core.csv         NOAA daily summaries
  data/raw/noaa_hourly/year=YYYY/<code>_*.csv      NOAA global hourly (UTC), used through 2024
  data/raw/noaa_ghcnh/year=YYYY/GHCNh_*.psv        NOAA GHCNh (UTC), used from 2025
  reference/isone_weather_station_weights.csv      ISO-NE's station weights

Outputs in step1/output/:
  weather_station_daily.csv   one row per station per date
  weather_daily.csv           one row per date, weighted across stations

Daytime sky cover
  Only routine hourly reports (FM-15) are used. Special reports (FM-16) are
  issued when conditions change, so including them would over-weight bad
  weather. Each report lists up to four cloud layers (GA1 to GA4) with
  coverage in oktas. The report's sky cover is the highest layer coverage
  divided by 8: clear 0, few 2/8, scattered 4/8, broken 7/8, overcast 8/8.
  Obscured sky (code 09) counts as 8/8. Partial obscuration (10) and missing
  (99) are dropped. A station's daily value is the mean over reports with
  local time from 8:00 to 16:59, the same hours as the duck day window.
  The global-hourly service ends on 2025-08-27, so GHCNh, NOAA's replacement,
  supplies 2025 onward (its sky_cover_summation fields carry the same oktas).
  The script prints how the two sources compare on the 2025 days both cover.
  Airport ceilometers only detect cloud up to about 12,000 ft, so high thin
  cloud reads as clear.

Regional value
  Stations are combined with ISO-NE's weights: summer weights for June to
  September, winter weights for October to May (Notes sheet of the SMD hourly
  workbook). When a station has no value for a date, the remaining weights are
  rescaled to sum to 1 and the number of stations used is recorded.

Temperature is in degrees F, wind in mph, precipitation and snow in inches.
HDD and CDD use the regional mean temperature against a 65 F base.
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

START, END = "2019-06-01", "2026-09-29"
DAY_HOURS = (8, 16)        # local clock hours 8:00 to 16:59
MIN_REPORTS = 6            # of 9 expected daytime reports
SPLICE = "2025-01-01"      # sky cover source changes here
SUMMER_MONTHS = (6, 7, 8, 9)
DAILY_COLS = {"TMAX": "tmax_f", "TMIN": "tmin_f", "AWND": "wind_mph", "PRCP": "prcp_in",
              "SNOW": "snow_in", "SNWD": "snow_depth_in"}


def load_daily(weights: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for code, ghcnd in zip(weights["code"], weights["ghcnd_id"]):
        p = RAW / "noaa_daily" / f"{code}_{ghcnd}_core.csv"
        if not p.exists():
            raise SystemExit(f"Missing {p.relative_to(ROOT)}. Run step1/download_noaa.py first.")
        df = pd.read_csv(p, parse_dates=["DATE"])
        df = df.rename(columns={"DATE": "date", **DAILY_COLS})[["date", *DAILY_COLS.values()]]
        df.insert(0, "code", code)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    out["tmean_f"] = (out["tmax_f"] + out["tmin_f"]) / 2
    return out


def daytime_sky(code: str, utc: pd.Series, oktas: pd.DataFrame) -> pd.DataFrame:
    """Station daily mean of report sky cover over the daytime window."""
    oktas = oktas.replace({9: 8}).where(lambda x: x <= 8)
    local = pd.to_datetime(utc, utc=True).dt.tz_convert("America/New_York")
    rep = pd.DataFrame({"date": local.dt.tz_localize(None).dt.normalize(), "hour": local.dt.hour,
                        "sky": oktas.max(axis=1) / 8}).dropna(subset=["sky"])
    rep = rep[rep["hour"].between(*DAY_HOURS)]
    day = rep.groupby("date")["sky"].agg(sky_cover_day="mean", sky_reports="size").reset_index()
    day.loc[day["sky_reports"] < MIN_REPORTS, "sky_cover_day"] = float("nan")
    day.insert(0, "code", code)
    return day


def sky_from_isd(code: str, isd: str) -> pd.DataFrame:
    files = sorted((RAW / "noaa_hourly").glob(f"year=*/{code}_{isd}_*.csv"))
    if not files:
        raise SystemExit(f"No global-hourly files for {code}. Run step1/download_noaa.py first.")
    df = pd.concat([pd.read_csv(p, dtype=str, usecols=["DATE", "REPORT_TYPE", "GA1", "GA2", "GA3", "GA4"])
                    for p in files], ignore_index=True)
    df = df[df["REPORT_TYPE"].str.strip() == "FM-15"]
    oktas = pd.DataFrame({c: pd.to_numeric(df[c].str[:2], errors="coerce") for c in ["GA1", "GA2", "GA3", "GA4"]})
    return daytime_sky(code, df["DATE"], oktas)


def sky_from_ghcnh(code: str, ghcnd: str) -> pd.DataFrame:
    files = sorted((RAW / "noaa_ghcnh").glob(f"year=*/GHCNh_{ghcnd}_*.psv"))
    if not files:
        raise SystemExit(f"No GHCNh files for {code}. Run step1/download_noaa.py first.")
    layers = [f"sky_cover_summation_{i}" for i in (1, 2, 3, 4)]
    df = pd.concat([pd.read_csv(p, sep="|", dtype=str, usecols=["DATE", "temperature_Report_Type", *layers])
                    for p in files], ignore_index=True)
    df = df[df["temperature_Report_Type"] == "FM15"]
    # Values look like "BKN:07;". The two digits are oktas.
    oktas = pd.DataFrame({c: pd.to_numeric(df[c].str.extract(r":(\d\d)")[0], errors="coerce") for c in layers})
    return daytime_sky(code, df["DATE"], oktas)


def load_sky(weights: pd.DataFrame) -> pd.DataFrame:
    """global-hourly through 2024, GHCNh from 2025. Prints how they compare in 2025."""
    frames, overlap = [], []
    for code, isd, ghcnd in zip(weights["code"], weights["isd_id"].astype(str), weights["ghcnd_id"]):
        old = sky_from_isd(code, isd)
        new = sky_from_ghcnh(code, ghcnd)
        frames.append(pd.concat([old[old["date"] < SPLICE], new[new["date"] >= SPLICE]]))
        both = old.merge(new, on=["code", "date"], suffixes=("_isd", "_ghcnh")).dropna()
        both = both[both["date"] >= SPLICE]
        diff = both["sky_cover_day_ghcnh"] - both["sky_cover_day_isd"]
        overlap.append({"code": code, "days": len(both), "mean_diff": diff.mean(), "max_abs_diff": diff.abs().max(),
                        "share_within_0.05": (diff.abs() <= 0.05).mean(),
                        "corr": both["sky_cover_day_isd"].corr(both["sky_cover_day_ghcnh"])})
    print(f"\nsky cover, GHCNh minus global-hourly on days both cover (from {SPLICE}):")
    print(pd.DataFrame(overlap).round(4).to_string(index=False))
    return pd.concat(frames, ignore_index=True)


def weighted(station: pd.DataFrame, weights: pd.DataFrame, col: str) -> pd.DataFrame:
    wide = station.pivot(index="date", columns="code", values=col).reindex(pd.date_range(START, END))
    w = weights.set_index("code")
    summer = wide.index.month.isin(SUMMER_MONTHS)
    wmat = pd.DataFrame([w["summer_weight"] if s else w["winter_weight"] for s in summer],
                        index=wide.index)[wide.columns]
    wmat = wmat.where(wide.notna())
    value = (wide * wmat).sum(axis=1, min_count=1) / wmat.sum(axis=1, min_count=1)
    return pd.DataFrame({col: value, f"{col}_stations": wide.notna().sum(axis=1)})


def main():
    weights = pd.read_csv(ROOT / "reference" / "isone_weather_station_weights.csv")
    print("weight sums: summer", round(weights["summer_weight"].sum(), 3),
          " winter", round(weights["winter_weight"].sum(), 3))

    daily = load_daily(weights)
    sky = load_sky(weights)
    station = daily.merge(sky, on=["code", "date"], how="outer")
    station = station[(station["date"] >= START) & (station["date"] <= END)].sort_values(["code", "date"])
    station.round(3).to_csv(OUT / "weather_station_daily.csv", index=False)

    n_days = len(pd.date_range(START, END))
    cols = ["tmax_f", "tmin_f", "tmean_f", "wind_mph", "prcp_in", "snow_in", "snow_depth_in", "sky_cover_day"]
    print(f"\nshare of the {n_days:,} dates with a value, by station:")
    print((station.groupby("code")[cols].count() / n_days).round(3).to_string())

    region = pd.concat([weighted(station, weights, c) for c in cols], axis=1)
    region.index.name = "date"
    region["hdd65"] = (65 - region["tmean_f"]).clip(lower=0)
    region["cdd65"] = (region["tmean_f"] - 65).clip(lower=0)
    region.round(3).to_csv(OUT / "weather_daily.csv")

    print("\nregional values: dates missing, and dates built from fewer than 8 stations:")
    for c in cols:
        print(f"  {c:15} missing {int(region[c].isna().sum()):4d}   fewer than 8 stations "
              f"{int((region[f'{c}_stations'] < 8).sum()):4d}   fewest {int(region[f'{c}_stations'].min())}")
    print("\nregional summary:")
    print(region[cols + ["hdd65", "cdd65"]].describe().round(2).T[["count", "mean", "min", "max"]].to_string())


if __name__ == "__main__":
    main()
