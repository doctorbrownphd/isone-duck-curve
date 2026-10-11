"""Build the hand-back files: figures, numbers.csv and the daily table sample.

Usage: python pipeline/make_results.py     (run after fit_model.py)

Reads the curated tables in data/curated/, the model outputs in results/, the
download manifest and the Step 0 comparison. Writes:
  results/figures/f1 to f6 (PNG, 1600 x 900)
  results/numbers.csv              name,value: every number for the report
  results/daily_model_sample.csv   first 50 rows of the daily table

Figure f1 uses two different demand series, and says so on the figure. Native
load does not exist before June 2019, so the 2014 line is settlement demand
(DEMAND) from ISO-NE's 2014 SMD hourly workbook.
"""
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from common import ROOT, STUDY_END, STUDY_START, parse_args, spark_session

RESULTS_DIR = ROOT / "results"
FIGS = RESULTS_DIR / "figures"
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
ISO_COUNTS = {2022: 45, 2023: 73, 2024: 107, 2025: 134}       # ISO Newswire, full calendar years
BLUE, ORANGE, GREY = "#1f5fa8", "#d9730d", "#8a8a8a"
TERM_LABELS = {"is_holiday": "Federal holiday", "daily_high_temp_f": "Daily high, per 1 F warmer",
               "daytime_sky_cover_oktas": "Sky cover, per okta cloudier", "btm_solar_gw": "Installed solar, per 1,000 MW"}

plt.rcParams.update({"font.size": 18, "axes.titlesize": 24, "axes.titleweight": "bold", "axes.spines.top": False,
                     "axes.spines.right": False, "figure.facecolor": "white", "axes.facecolor": "white"})


def save(fig, name):
    FIGS.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGS / name, dpi=100)
    plt.close(fig)
    print(f"wrote results/figures/{name}")


def new_fig():
    return plt.subplots(figsize=(16, 9))


def f1(load):
    day25 = load[load["date"].astype(str) == "2025-04-20"].sort_values("utc_begin")
    wb = pd.read_excel(ROOT / "data/raw/isone_zonal/year=2014/2014_smd_hourly.xls", sheet_name="ISONE CA")
    day14 = wb[pd.to_datetime(wb["Date"]) == "2014-04-20"].sort_values("Hour")
    fig, ax = new_fig()
    ax.plot(day14["Hour"], day14["DEMAND"], color=GREY, lw=4, marker="o",
            label="Easter Sunday, April 20, 2014 (settlement demand)")
    ax.plot(day25["hour_ending"], day25["native_load_mw"], color=BLUE, lw=4, marker="o",
            label="Easter Sunday, April 20, 2025 (native load)")
    lo = day25.loc[day25["native_load_mw"].idxmin()]
    ax.annotate(f"Low of {lo['native_load_mw']:,.0f} MW\nin the hour ending {int(lo['hour_ending']) - 12} p.m.",
                (lo["hour_ending"], lo["native_load_mw"]), xytext=(15.5, 3600), color=BLUE,
                arrowprops={"arrowstyle": "->", "color": BLUE})
    ax.set_title("In 2025 the lowest demand of the day came in mid-afternoon, not overnight")
    ax.set_xlabel("Hour ending (local time)")
    ax.set_ylabel("Grid demand (MW)")
    ax.set_xticks(range(1, 25))
    ax.set_ylim(3000, 14000)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}")
    ax.legend(loc="upper left", frameon=False)
    fig.text(0.01, 0.01, "The two lines are different ISO-NE series. Native load is not published before June 2019, "
             "so 2014 shows settlement demand from the SMD hourly workbook.", fontsize=14, color="#444444")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "f1_april20_2025_vs_2014.png")
    return {"f1_low_2025_mw": round(lo["native_load_mw"], 0), "f1_low_2025_hour_ending": int(lo["hour_ending"]),
            "f1_low_2014_mw": int(day14["DEMAND"].min()),
            "f1_low_2014_hour_ending": int(day14.loc[day14["DEMAND"].idxmin(), "Hour"])}


def f2(daily):
    by = daily[daily["year"].between(2019, 2025)].groupby("year")["is_duck_day"].sum()
    fig, ax = new_fig()
    bars = ax.bar(by.index, by.values, color=BLUE, width=0.5, label="This project (hourly native load)")
    ax.bar_label(bars, padding=4)
    # ISO-NE's count sits just right of each bar so the two numbers do not overlap.
    ax.scatter([y + 0.42 for y in ISO_COUNTS], list(ISO_COUNTS.values()), color=ORANGE, s=260, marker="D", zorder=3,
               label="ISO New England published count")
    for y, v in ISO_COUNTS.items():
        ax.annotate(str(v), (y + 0.42, v), xytext=(0, 14), textcoords="offset points", color=ORANGE,
                    fontweight="bold", ha="center")
    ax.set_title("Duck curve days rose every year and track ISO-NE's published counts")
    ax.set_ylabel("Duck curve days in the year")
    ax.set_xticks(by.index)
    ax.set_xticklabels([f"{y}\n(Jun to Dec)" if y == 2019 else str(y) for y in by.index])
    ax.set_ylim(0, 155)
    ax.legend(loc="upper left", frameon=False)
    fig.tight_layout()
    save(fig, "f2_duck_days_per_year.png")


def f3():
    c = pd.read_csv(RESULTS_DIR / "coefficients.csv")
    c = c[c["term"] != "intercept"].copy()

    def label(t):
        if t.startswith("month_"):
            return f"{t[6:]} (vs Jan)"
        if t.startswith("dow_"):
            return f"{t[4:]} (vs Mon)"
        return TERM_LABELS[t]
    c["label"] = c["term"].map(label)
    c = c.sort_values("odds_ratio")
    sig = c["p_value"] < 0.05
    fig, ax = plt.subplots(figsize=(16, 9))
    y = range(len(c))
    ax.hlines(y, c["or_ci_low"], c["or_ci_high"], color=[BLUE if s else GREY for s in sig], lw=4)
    ax.scatter(c["odds_ratio"], y, color=[BLUE if s else GREY for s in sig], s=110, zorder=3)
    ax.axvline(1, color="black", lw=1.5, ls="--")
    ax.set_xscale("log")
    ax.set_yticks(list(y))
    ax.set_yticklabels(c["label"], fontsize=16)
    ax.set_xticks([0.01, 0.1, 1, 10, 100, 1000])
    ax.set_xticklabels(["0.01", "0.1", "1", "10", "100", "1,000"])
    ax.set_xlabel("Odds ratio with 95% interval (log scale). Right of 1 = duck day more likely")
    ax.set_title("Spring, weekends, clear skies and more solar raise the odds of a duck day", fontsize=21)
    fig.text(0.01, 0.01, "Blue: p < 0.05. Grey: not significant.", fontsize=14, color="#444444")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    save(fig, "f3_odds_ratios.png")


def f4(nums):
    tp, fn, fp, tn = (int(nums[f"test_{k}"]) for k in ("TP", "FN", "FP", "TN"))
    labels = [f"Caught\n(duck day, predicted)", "Missed\n(duck day, not predicted)",
              "False alarm\n(predicted, not a duck day)", "Correctly ruled out\n(not a duck day)"]
    vals = [tp, fn, fp, tn]
    fig, ax = new_fig()
    bars = ax.bar(labels, vals, color=[BLUE, ORANGE, ORANGE, GREY], width=0.6)
    ax.bar_label(bars, padding=4, fontsize=24, fontweight="bold")
    ax.set_title(f"In 2025 the model caught {tp} of {tp + fn} duck days and raised {fp} false alarms")
    ax.set_ylabel(f"Days in 2025 ({tp + fn + fp + tn} scored)")
    ax.set_ylim(0, max(vals) * 1.15)
    fig.tight_layout()
    save(fig, "f4_confusion_2025.png")


def f5(daily):
    by = daily.groupby("month")["is_duck_day"].sum().reindex(range(1, 13), fill_value=0)
    fig, ax = new_fig()
    bars = ax.bar(MONTHS, by.values, color=BLUE, width=0.7)
    ax.bar_label(bars, padding=4)
    ax.set_title("Duck curve days cluster in March and April")
    ax.set_ylabel("Duck curve days, June 2019 to September 2026")
    ax.set_ylim(0, by.max() * 1.12)
    fig.tight_layout()
    save(fig, "f5_duck_days_by_month.png")


def f6():
    """Expected (sum of fitted probabilities) against actual duck days, by month."""
    pred = pd.read_csv(RESULTS_DIR / "predictions.csv", parse_dates=["date"])
    pred["month"] = pred["date"].dt.month
    panels = [("test_2025", "2025 test year"), ("holdout_2026", "2026 holdout (to Sep 29)")]
    fig, axes = plt.subplots(1, 2, figsize=(16, 9), sharey=True)
    out = []
    for ax, (key, label) in zip(axes, panels):
        g = pred[pred["set"] == key].groupby("month").agg(expected=("probability", "sum"),
                                                          actual=("is_duck_day", "sum"))
        x = range(len(g))
        ax.bar([i - 0.2 for i in x], g["expected"], width=0.4, color=ORANGE, label="Expected by the model")
        ax.bar([i + 0.2 for i in x], g["actual"], width=0.4, color=BLUE, label="Actual duck days")
        ax.set_xticks(list(x))
        ax.set_xticklabels([MONTHS[m - 1] for m in g.index])
        ax.set_title(f"{label}: {g['expected'].sum():.0f} expected, {int(g['actual'].sum())} actual",
                     fontsize=20, fontweight="normal")
        for m, r in g.iterrows():
            out += [(f"calib_{key}_{MONTHS[m - 1]}_expected", round(r["expected"], 1)),
                    (f"calib_{key}_{MONTHS[m - 1]}_actual", int(r["actual"]))]
    axes[0].set_ylabel("Duck curve days in the month")
    axes[0].legend(loc="upper right", frameon=False)
    fig.suptitle("Outside the training years the model expects more duck days than occurred",
                 fontsize=24, fontweight="bold")
    fig.text(0.01, 0.01, "Expected = sum of the model's daily probabilities. Days without weather data are not scored.",
             fontsize=14, color="#444444")
    fig.tight_layout(rect=(0, 0.03, 1, 0.95))
    save(fig, "f6_calibration_by_month.png")
    return out


def main():
    args = parse_args(__doc__)
    spark = spark_session("make_results")
    daily = spark.read.parquet(f"{args.base}/curated/daily_model").orderBy("date").toPandas()
    load = spark.read.parquet(f"{args.base}/curated/hourly_load").filter("date = '2025-04-20'").toPandas()
    n_load = spark.read.parquet(f"{args.base}/curated/hourly_load").count()
    n_wx = spark.read.parquet(f"{args.base}/curated/hourly_weather").count()
    spark.stop()

    model = dict(csv.reader((RESULTS_DIR / "model_numbers.csv").open()))
    model.pop("name", None)

    nums = []
    man = pd.read_csv(ROOT / "manifest" / "downloads.csv")
    man["source"] = man["local_path"].str.split("/").str[2]
    for src, g in man.drop_duplicates("local_path", keep="last").groupby("source"):
        nums += [(f"raw_files_{src}", len(g)), (f"raw_rows_{src}", int(g["rows"].fillna(0).sum())),
                 (f"raw_bytes_{src}", int(g["bytes"].sum()))]
    nums += [("curated_hourly_load_rows", n_load), ("curated_hourly_weather_rows", n_wx),
             ("study_period_start", str(daily["date"].min())), ("study_period_end", str(daily["date"].max())),
             ("days_in_daily_table", len(daily)),
             ("days_dropped_missing_load_hours", len(pd.date_range(STUDY_START, STUDY_END)) - len(daily)),
             ("duck_days_total", int(daily["is_duck_day"].sum())), ("near_tie_days", int(daily["near_tie"].sum()))]
    for y, g in daily.groupby("year"):
        nums += [(f"days_{y}", len(g)), (f"duck_days_{y}", int(g["is_duck_day"].sum()))]
    for m, g in daily.groupby("month"):
        nums.append((f"duck_days_month_{MONTHS[m - 1]}", int(g["is_duck_day"].sum())))
    comp = pd.read_csv(ROOT / "step0" / "output" / "hourly_comparison.csv")
    for _, r in comp.iterrows():
        key = r["period"].replace(" ", "_")
        nums += [(f"step0_iso_ne_{key}", int(r["iso_ne"])), (f"step0_ours_{key}", int(r["ours_W1"])),
                 (f"step0_diff_{key}", int(r["diff_primary"]))]
    nums += [(k, v) for k, v in model.items() if k != "days_in_daily_table"]

    nums += list(f1(load).items())
    f2(daily)
    f3()
    f4(model)
    f5(daily)

    # Calibration outside the training years: expected duck days (sum of fitted
    # probabilities) against actual, and the solar value each year was scored at.
    pred = pd.read_csv(RESULTS_DIR / "predictions.csv")
    for key, label in [("test_2025", "test_2025"), ("holdout_2026", "holdout_2026")]:
        g = pred[pred["set"] == key]
        expected, actual = g["probability"].sum(), int(g["is_duck_day"].sum())
        nums += [(f"{label}_expected_duck_days", round(expected, 1)),
                 (f"{label}_actual_duck_days_scored", actual),
                 (f"{label}_overprediction_pct", round(100 * (expected / actual - 1)))]
    solar = daily.groupby("year")["btm_solar_mw"].first()
    nums += [("train_solar_max_mw", float(solar[solar.index <= 2024].max())),
             ("test_2025_solar_mw", float(solar[2025])), ("holdout_2026_solar_mw", float(solar[2026]))]

    nums += f6()

    # Fixed settings quoted in the write-up.
    nums += [("alpha", 0.05), ("classification_threshold", 0.5), ("confidence_level_pct", 95),
             ("near_tie_threshold_mw", 25), ("solar_odds_ratio_unit_mw", 1000)]

    nums.append(("aws_total_cost_usd", "not yet run in AWS"))
    with (RESULTS_DIR / "numbers.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name", "value"])
        w.writerows(nums)
    print(f"wrote results/numbers.csv ({len(nums)} numbers)")

    cols = ["date", "year", "month", "day_of_week", "is_holiday", "daily_high_temp_f", "daytime_sky_cover_oktas",
            "btm_solar_mw", "min_load_hour", "min_load_mw", "is_duck_day"]
    daily[cols].head(50).to_csv(RESULTS_DIR / "daily_model_sample.csv", index=False)
    print("wrote results/daily_model_sample.csv")


if __name__ == "__main__":
    main()
