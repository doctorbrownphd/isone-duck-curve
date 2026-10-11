"""Join and aggregate to daily: curated hourly tables -> curated/daily_model (Parquet).

Usage: spark-submit pipeline/build_daily_model.py [--base <folder or s3://bucket>]

Inputs:  <base>/curated/hourly_load, <base>/curated/hourly_weather,
         btm_solar_by_year.csv (hand-entered ISO-NE year-end installed solar)
Output:  <base>/curated/daily_model/, Parquet, partitioned by year.
         One row per local calendar date, 2019-06-01 to 2026-09-29.

Load, per date
  - Days with more than 2 missing hourly rows are dropped and counted.
  - min_load_hour is the hour ending with the lowest native load. A tie keeps
    the earlier hour.
  - is_duck_day is 1 when min_load_hour is 9 to 17, meaning the lowest hour
    started between 8:00 a.m. and 4:00 p.m. local time. This is the reading
    that reproduces ISO-NE's published counts (Step 0).
  - gap_mw is the overnight minimum (hour ending 1 to 7) minus the daytime
    minimum (hour ending 9 to 17). near_tie marks days where the two are
    within 25 MW, used for one sensitivity fit.

Weather, per station per date, then across stations
  - daily_high_temp_f: the highest hourly temperature of the local day. A
    station needs at least 18 hourly readings that day to count.
  - daytime_sky_cover_oktas: mean sky cover over the hourly reports from
    9:00 to 16:00 local time. A station needs at least 5 of the 7 reports.
  - The regional value is the unweighted mean across the stations that have a
    value that day. If a station is missing, the others fill in, and the
    number of stations used is kept in its own column.

Solar
  - btm_solar_mw for a calendar year is the installed capacity ISO-NE reported
    for the previous December 31. It is capacity known to be in place for the
    whole year.

Calendar
  - day_of_week: 1 = Monday to 7 = Sunday.
  - is_holiday: US federal holidays from the `holidays` package, observed
    dates included.
  - day_length_hours: sunrise to sunset at 42.3 N from the solar declination.
    Not a model predictor. It is kept in case month has to be replaced
    (decision gate 2).
"""
import holidays
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import STUDY_END, STUDY_START, parse_args, reference_dir, show, spark_session

DUCK_HOURS = (9, 17)
NIGHT_HOURS = (1, 7)
NEAR_TIE_MW = 25
MAX_MISSING_LOAD_HOURS = 2
MIN_TEMP_READINGS = 18
SKY_HOURS = (9, 15)          # reports stamped 9:00 to 15:59 local, which covers 9:00 to 16:00
MIN_SKY_REPORTS = 5
LATITUDE = 42.3


def daily_load(load):
    per_day = load.groupBy("date").agg(
        F.count("*").alias("load_hours"),
        F.min(F.when(F.col("hour_ending").between(*NIGHT_HOURS), F.col("native_load_mw"))).alias("night_min_mw"),
        F.min(F.when(F.col("hour_ending").between(*DUCK_HOURS), F.col("native_load_mw"))).alias("day_min_mw"))
    lowest = (load.withColumn("rank", F.row_number().over(
                  Window.partitionBy("date").orderBy("native_load_mw", "utc_begin")))
              .filter("rank = 1")
              .select("date", F.col("hour_ending").alias("min_load_hour"),
                      F.col("native_load_mw").alias("min_load_mw")))
    out = (per_day.join(lowest, "date")
           .withColumn("is_duck_day", F.col("min_load_hour").between(*DUCK_HOURS).cast("int"))
           .withColumn("gap_mw", F.col("night_min_mw") - F.col("day_min_mw"))
           .withColumn("near_tie", (F.abs("gap_mw") < NEAR_TIE_MW).cast("int")))
    return out


def daily_weather(wx):
    station = wx.groupBy("station", "date").agg(
        F.max("temp_f").alias("high_f"),
        F.count("temp_f").alias("temp_readings"),
        F.avg(F.when(F.col("local_hour").between(*SKY_HOURS), F.col("sky_cover_oktas"))).alias("sky"),
        F.count(F.when(F.col("local_hour").between(*SKY_HOURS), F.col("sky_cover_oktas"))).alias("sky_reports"))
    station = (station
               .withColumn("high_f", F.when(F.col("temp_readings") >= MIN_TEMP_READINGS, F.col("high_f")))
               .withColumn("sky", F.when(F.col("sky_reports") >= MIN_SKY_REPORTS, F.col("sky"))))
    region = station.groupBy("date").agg(
        F.avg("high_f").alias("daily_high_temp_f"),
        F.count("high_f").alias("temp_stations"),
        F.avg("sky").alias("daytime_sky_cover_oktas"),
        F.count("sky").alias("sky_stations"))
    return station, region


def main():
    args = parse_args(__doc__)
    spark = spark_session("build_daily_model")

    load = spark.read.parquet(f"{args.base}/curated/hourly_load")
    wx = spark.read.parquet(f"{args.base}/curated/hourly_weather")
    solar = (spark.read.option("header", True).csv(f"{reference_dir(args.base)}/btm_solar_by_year.csv")
             .select((F.col("year_end").cast("int") + 1).alias("year"),
                     F.col("btm_solar_mw").cast("double").alias("btm_solar_mw")))

    dload = daily_load(load).filter(F.col("date").between(STUDY_START, STUDY_END))
    n_days = dload.count()
    kept = dload.filter(F.col("load_hours") >= 24 - MAX_MISSING_LOAD_HOURS)
    n_dropped = n_days - kept.count()

    _, region = daily_weather(wx)

    years = list(range(int(STUDY_START[:4]), int(STUDY_END[:4]) + 1))
    us = holidays.US(years=years, observed=True)
    hol = spark.createDataFrame([(str(d), name) for d, name in sorted(us.items())], "d string, holiday_name string") \
        .select(F.to_date("d").alias("date"), "holiday_name")

    decl = F.radians(F.lit(23.44)) * F.sin(2 * 3.141592653589793 * (284 + F.dayofyear("date")) / 365)
    cos_h = -F.tan(F.radians(F.lit(LATITUDE))) * F.tan(decl)

    daily = (kept.join(region, "date", "left").join(hol, "date", "left")
             .withColumn("year", F.year("date"))
             .join(F.broadcast(solar), "year", "left")
             .withColumn("month", F.month("date"))
             .withColumn("day_of_week", ((F.dayofweek("date") + 5) % 7) + 1)      # 1 = Monday
             .withColumn("is_holiday", F.col("holiday_name").isNotNull().cast("int"))
             .withColumn("day_length_hours", F.round(24 / 3.141592653589793 * F.acos(cos_h), 3))
             .select("date", "year", "month", "day_of_week", "is_holiday",
                     F.round("daily_high_temp_f", 2).alias("daily_high_temp_f"),
                     F.round("daytime_sky_cover_oktas", 3).alias("daytime_sky_cover_oktas"),
                     "btm_solar_mw", "min_load_hour", F.round("min_load_mw", 3).alias("min_load_mw"), "is_duck_day",
                     "holiday_name", "temp_stations", "sky_stations", "load_hours",
                     F.round("gap_mw", 1).alias("gap_mw"), "near_tie", "day_length_hours"))

    (daily.repartition("year").write.mode("overwrite").partitionBy("year")
     .parquet(f"{args.base}/curated/daily_model"))

    out = spark.read.parquet(f"{args.base}/curated/daily_model")
    print(f"holidays package version: {holidays.__version__}")
    print(f"dates in the study period with load data: {n_days:,}")
    print(f"days dropped for more than {MAX_MISSING_LOAD_HOURS} missing load hours: {n_dropped}")
    print(f"rows written: {out.count():,}")
    show("days and duck days per year (must reproduce Step 0):",
         out.groupBy("year").agg(F.count("*").alias("days"), F.sum("is_duck_day").alias("duck_days")).orderBy("year"))
    show("missing values per column, by year:",
         out.groupBy("year").agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in
                                   ["daily_high_temp_f", "daytime_sky_cover_oktas", "btm_solar_mw", "min_load_mw"]])
         .orderBy("year"))
    show("dates with a missing predictor:",
         out.filter("daily_high_temp_f is null or daytime_sky_cover_oktas is null or btm_solar_mw is null")
         .select("date", "daily_high_temp_f", "daytime_sky_cover_oktas", "temp_stations", "sky_stations", "is_duck_day")
         .orderBy("date"))
    show("days built from fewer than 6 stations:",
         out.agg(F.sum((F.col("temp_stations") < 6).cast("int")).alias("temperature"),
                 F.sum((F.col("sky_stations") < 6).cast("int")).alias("sky_cover"),
                 F.min("temp_stations").alias("fewest_temp_stations"),
                 F.min("sky_stations").alias("fewest_sky_stations")))
    show("installed solar used for each year:", out.groupBy("year").agg(F.first("btm_solar_mw").alias("btm_solar_mw")).orderBy("year"))
    show("duck days by month:", out.groupBy("month").agg(F.count("*").alias("days"), F.sum("is_duck_day").alias("duck_days")).orderBy("month"))
    show("first rows:", out.orderBy("date").select(
        "date", "month", "day_of_week", "is_holiday", "daily_high_temp_f", "daytime_sky_cover_oktas",
        "btm_solar_mw", "min_load_hour", "min_load_mw", "is_duck_day"), 5)
    spark.stop()


if __name__ == "__main__":
    main()
