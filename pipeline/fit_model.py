"""Fit the logistic regression and run the hypothesis test.

Usage: spark-submit pipeline/fit_model.py [--base <folder or s3://bucket>]

Input:  <base>/curated/daily_model
Output (local run; in AWS the summary and numbers go to <base>/results/):
        results/model_summary.txt   full text of everything printed here
        results/model_numbers.csv   name,value pairs for numbers.csv
        results/coefficients.csv    one row per model term
        results/predictions.csv     fitted probability for every scored day

Model (as approved): Spark ML GeneralizedLinearRegression, family binomial,
link logit. One row per day. Outcome is_duck_day. Predictors: month, day of
week, holiday flag, daily high temperature, daytime sky cover, installed
solar capacity.

  - Month and day of week are categories, entered as 0/1 columns. January and
    Monday are the reference levels, so each month's odds ratio is relative to
    January and each day's is relative to Monday.
  - Solar is entered in thousands of MW so its odds ratio reads "per 1,000 MW".
    This is a change of units, not a different predictor.

Hypothesis test: likelihood ratio test of the full model against a model with
an intercept only, both fitted on the training data.
    G  = baseline deviance - full model deviance
    df = number of non-intercept parameters in the full model
    p  = P(chi-square with df degrees of freedom > G)
Reject the null hypothesis when p < 0.05.

Data split: train 2019-06-01 to 2024-12-31, test once on 2025. 2026 is scored
as a second holdout and reported separately. Its solar value is a year-end
survey figure like the others, but the year is incomplete.

Also reported: correlations and VIF for the continuous predictors, whether the
fit converged, how often a duck day follows a duck day (the days are not
independent, which the test assumes), and one sensitivity fit without the
near-tie days.
"""
import csv
import sys
from pathlib import Path

import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.sql import functions as F
from scipy import stats

from common import ROOT, parse_args, spark_session

RESULTS_DIR = ROOT / "results"
TRAIN_END, TEST_YEAR, HOLDOUT_YEAR = "2024-12-31", 2025, 2026
ALPHA = 0.05
MAX_ITER = 100
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
CONTINUOUS = ["daily_high_temp_f", "daytime_sky_cover_oktas", "btm_solar_gw"]
LABELS = {"is_holiday": "Federal holiday", "daily_high_temp_f": "Daily high temperature (per 1 F)",
          "daytime_sky_cover_oktas": "Daytime sky cover (per okta)",
          "btm_solar_gw": "Installed solar (per 1,000 MW)"}

lines = []
numbers = []


def say(text=""):
    print(text)
    lines.append(str(text))


def num(name, value):
    numbers.append((name, value))


def add_features(df):
    """0/1 columns for month and day of week. Returns the frame and the feature names in order."""
    names = []
    for m in range(2, 13):
        df = df.withColumn(f"month_{MONTHS[m - 1]}", (F.col("month") == m).cast("double"))
        names.append(f"month_{MONTHS[m - 1]}")
    for d in range(2, 8):
        df = df.withColumn(f"dow_{DAYS[d - 1]}", (F.col("day_of_week") == d).cast("double"))
        names.append(f"dow_{DAYS[d - 1]}")
    df = df.withColumn("btm_solar_gw", F.col("btm_solar_mw") / 1000)
    names += ["is_holiday", "daily_high_temp_f", "daytime_sky_cover_oktas", "btm_solar_gw"]
    return df, names


def glm(df, feature_names):
    data = VectorAssembler(inputCols=feature_names, outputCol="features").transform(df)
    model = GeneralizedLinearRegression(family="binomial", link="logit", labelCol="is_duck_day",
                                        featuresCol="features", predictionCol="probability",
                                        maxIter=MAX_ITER, tol=1e-8, regParam=0.0).fit(data)
    return model


def lr_test(train, feature_names, tag):
    base = glm(train, [])
    full = glm(train, feature_names)
    d0, d1 = base.summary.deviance, full.summary.deviance
    g = d0 - d1
    df = len(feature_names)
    p = stats.chi2.sf(g, df)
    say(f"  baseline (intercept only): intercept {base.intercept:.4f}, deviance {d0:.3f}, "
        f"iterations {base.summary.numIterations}")
    say(f"  full model: deviance {d1:.3f}, iterations {full.summary.numIterations} of at most {MAX_ITER}, "
        f"AIC {full.summary.aic:.2f}")
    say(f"  G = {d0:.3f} - {d1:.3f} = {g:.3f}")
    say(f"  df = {df}")
    say(f"  p = P(chi-square({df}) > {g:.3f}) = {p:.3e}")
    say(f"  decision at alpha {ALPHA}: {'reject' if p < ALPHA else 'fail to reject'} the null hypothesis")
    for k, v in [("baseline_deviance", round(d0, 3)), ("full_deviance", round(d1, 3)), ("G", round(g, 3)),
                 ("df", df), ("p_value", f"{p:.3e}"),
                 ("decision", "reject null" if p < ALPHA else "fail to reject null"),
                 ("iterations", full.summary.numIterations),
                 ("converged", int(full.summary.numIterations < MAX_ITER)), ("aic", round(full.summary.aic, 2))]:
        num(f"{tag}{k}", v)
    return base, full


def coefficient_table(model, feature_names):
    s = model.summary
    est = list(model.coefficients) + [model.intercept]          # Spark lists the intercept last
    rows = []
    for name, b, se, z, p in zip(feature_names + ["intercept"], est, s.coefficientStandardErrors, s.tValues, s.pValues):
        rows.append({"term": name, "coefficient": b, "std_error": se, "z": z, "p_value": p,
                     "odds_ratio": np.exp(b), "or_ci_low": np.exp(b - 1.96 * se), "or_ci_high": np.exp(b + 1.96 * se)})
    return rows


def score(model, df, feature_names, label):
    data = VectorAssembler(inputCols=feature_names, outputCol="features").transform(df)
    pred = model.transform(data).withColumn("predicted", (F.col("probability") >= 0.5).cast("int"))
    c = {(r["is_duck_day"], r["predicted"]): r["count"] for r in pred.groupBy("is_duck_day", "predicted").count().collect()}
    tp, fp, fn, tn = c.get((1, 1), 0), c.get((0, 1), 0), c.get((1, 0), 0), c.get((0, 0), 0)
    precision = tp / (tp + fp) if tp + fp else float("nan")
    recall = tp / (tp + fn) if tp + fn else float("nan")
    accuracy = (tp + tn) / (tp + fp + fn + tn)
    say(f"\n{label}: {tp + fp + fn + tn} days scored, {tp + fn} duck days, threshold 0.5")
    say(f"                      predicted duck   predicted not")
    say(f"  actual duck day     {tp:14d}   {fn:13d}")
    say(f"  actual other day    {fp:14d}   {tn:13d}")
    say(f"  precision = TP / (TP + FP) = {tp} / {tp + fp} = {precision:.3f}")
    say(f"  recall    = TP / (TP + FN) = {tp} / {tp + fn} = {recall:.3f}")
    say(f"  accuracy  = {accuracy:.3f}   (always guessing 'not a duck day' would score {(fp + tn) / (tp + fp + fn + tn):.3f})")
    return pred.select("date", "is_duck_day", F.round("probability", 4).alias("probability"), "predicted"), \
        {"days": tp + fp + fn + tn, "TP": tp, "FP": fp, "FN": fn, "TN": tn,
         "precision": round(precision, 4), "recall": round(recall, 4), "accuracy": round(accuracy, 4)}


def vif_table(pdf):
    """VIF for each continuous predictor: 1 / (1 - R squared) from regressing it on the others."""
    out = {}
    for col in CONTINUOUS:
        y = pdf[col].to_numpy()
        x = np.column_stack([np.ones(len(pdf))] + [pdf[c].to_numpy() for c in CONTINUOUS if c != col])
        resid = y - x @ np.linalg.lstsq(x, y, rcond=None)[0]
        r2 = 1 - resid.var() / y.var()
        out[col] = 1 / (1 - r2)
    return out


def main():
    args = parse_args(__doc__)
    spark = spark_session("fit_model")
    daily = spark.read.parquet(f"{args.base}/curated/daily_model")
    daily, features = add_features(daily)

    predictors = ["month", "day_of_week", "is_holiday", "daily_high_temp_f", "daytime_sky_cover_oktas", "btm_solar_mw"]
    complete = daily.dropna(subset=predictors).cache()
    n_all, n_complete = daily.count(), complete.count()
    train = complete.filter(F.col("date") <= TRAIN_END).cache()
    test = complete.filter(F.col("year") == TEST_YEAR)
    holdout = complete.filter(F.col("year") == HOLDOUT_YEAR)

    say("DUCK CURVE DAY MODEL")
    say(f"Spark {spark.version}. Rows in daily table: {n_all:,}. Rows with every predictor: {n_complete:,} "
        f"({n_all - n_complete} dropped for a missing weather value).")
    for name, part in [("train 2019-06-01 to 2024-12-31", train), (f"test {TEST_YEAR}", test),
                       (f"holdout {HOLDOUT_YEAR} (to Sep 29)", holdout)]:
        r = part.agg(F.count("*").alias("n"), F.sum("is_duck_day").alias("d")).first()
        say(f"  {name}: {r['n']:,} days, {r['d']} duck days")
        key = name.split()[0]
        num(f"{key}_days", r["n"])
        num(f"{key}_duck_days", r["d"])
    num("days_in_daily_table", n_all)
    num("days_dropped_missing_weather", n_all - n_complete)

    say("\nSEPARATION CHECK: training duck days by category (a category with 0 would break the fit)")
    for col, labels in [("month", MONTHS), ("day_of_week", DAYS)]:
        rows = train.groupBy(col).agg(F.count("*").alias("n"), F.sum("is_duck_day").alias("d")).orderBy(col).collect()
        say("  " + "  ".join(f"{labels[r[col] - 1]} {r['d']}/{r['n']}" for r in rows))
        if any(r["d"] == 0 or r["d"] == r["n"] for r in rows):
            say(f"  STOP: a {col} level has no duck days or only duck days. Decision gate 2 applies.")
            sys.exit(1)
    r = train.groupBy("is_holiday").agg(F.count("*").alias("n"), F.sum("is_duck_day").alias("d")).orderBy("is_holiday").collect()
    say("  " + "  ".join(f"holiday={x['is_holiday']} {x['d']}/{x['n']}" for x in r))

    say("\nHYPOTHESIS TEST: likelihood ratio test on the training data")
    base, full = lr_test(train, features, "")
    num("baseline_intercept", round(base.intercept, 4))

    say("\nCOEFFICIENTS (full model, training data). Odds ratio = exp(coefficient). 95% CI = exp(coef +/- 1.96 SE).")
    say(f"  {'term':28} {'coef':>9} {'SE':>8} {'z':>8} {'p':>10} {'odds ratio':>11} {'95% CI':>20}")
    coefs = coefficient_table(full, features)
    for c in coefs:
        say(f"  {c['term']:28} {c['coefficient']:9.4f} {c['std_error']:8.4f} {c['z']:8.3f} {c['p_value']:10.2e} "
            f"{c['odds_ratio']:11.4f}   [{c['or_ci_low']:.4f}, {c['or_ci_high']:.4f}]")
        if c["term"] != "intercept":
            for k in ("odds_ratio", "or_ci_low", "or_ci_high"):
                num(f"{k}_{c['term']}", round(c[k], 4))
            num(f"p_{c['term']}", f"{c['p_value']:.3e}")
    say("  Reference levels: January, Monday. Solar is per 1,000 MW of installed capacity.")

    say("\nDIAGNOSTICS")
    pdf = train.select(*CONTINUOUS, "is_duck_day", "date").orderBy("date").toPandas()
    corr = pdf[CONTINUOUS].corr()
    say("  correlations among the continuous predictors (training data):")
    for a in CONTINUOUS:
        say("    " + f"{a:26}" + "  ".join(f"{corr.loc[a, b]:6.3f}" for b in CONTINUOUS))
    for col, v in vif_table(pdf).items():
        say(f"  VIF {col}: {v:.3f}")
        num(f"vif_{col}", round(v, 3))
    say("  Installed solar rises every year and there is no separate time trend in the model,")
    say("  so the solar coefficient also carries anything else that changed steadily over these years.")

    allp = complete.select("date", "is_duck_day").orderBy("date").toPandas()
    prev = allp["is_duck_day"].shift(1)
    after_duck = allp.loc[prev == 1, "is_duck_day"].mean()
    after_other = allp.loc[prev == 0, "is_duck_day"].mean()
    overall = allp["is_duck_day"].mean()
    say(f"\n  Days are not independent. Overall duck day rate {overall:.3f}. "
        f"After a duck day {after_duck:.3f}. After a non-duck day {after_other:.3f}.")
    say("  The likelihood ratio test assumes independent days, so its p-value is smaller than it should be.")
    num("duck_rate_overall", round(overall, 4))
    num("duck_rate_after_duck_day", round(after_duck, 4))
    num("duck_rate_after_non_duck_day", round(after_other, 4))

    say("\nTEST SET (scored once; these describe the model and do not decide the hypothesis)")
    pred_test, m_test = score(full, test, features, f"Test year {TEST_YEAR}")
    pred_hold, m_hold = score(full, holdout, features, f"Second holdout {HOLDOUT_YEAR} (Jan 1 to Sep 29), reported separately")
    for k, v in m_test.items():
        num(f"test_{k}", v)
    for k, v in m_hold.items():
        num(f"holdout_{k}", v)

    say("\nSENSITIVITY: same test without the near-tie days (daytime and overnight minimum within 25 MW)")
    firm = train.filter("near_tie = 0")
    say(f"  training days {firm.count():,} (dropped {train.count() - firm.count()}), "
        f"duck days {firm.agg(F.sum('is_duck_day')).first()[0]}")
    _, full_firm = lr_test(firm, features, "sens_")
    firm_coefs = {c["term"]: c for c in coefficient_table(full_firm, features)}
    say("  odds ratios, main fit vs without near ties:")
    for c in coefs:
        if c["term"] in LABELS:
            say(f"    {c['term']:26} {c['odds_ratio']:8.4f}  vs {firm_coefs[c['term']]['odds_ratio']:8.4f}")

    say("\nRAW SPARK SUMMARY, FULL MODEL")
    say(str(full.summary))
    say("\nRAW SPARK SUMMARY, BASELINE MODEL")
    say(str(base.summary))

    if not args.base.startswith("s3"):
        RESULTS_DIR.mkdir(exist_ok=True)
        (RESULTS_DIR / "model_summary.txt").write_text("\n".join(lines) + "\n")
        with (RESULTS_DIR / "model_numbers.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["name", "value"])
            w.writerows(numbers)
        with (RESULTS_DIR / "coefficients.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(coefs[0]))
            w.writeheader()
            w.writerows(coefs)
        pred_test.withColumn("set", F.lit("test_2025")).unionByName(
            pred_hold.withColumn("set", F.lit("holdout_2026"))).orderBy("date").toPandas() \
            .to_csv(RESULTS_DIR / "predictions.csv", index=False)
        print("\nwrote results/model_summary.txt, model_numbers.csv, coefficients.csv, predictions.csv")
    else:
        # In AWS the same outputs go to the bucket's results/ prefix.
        out = f"{args.base}/results"
        spark.createDataFrame([(x,) for x in lines], "value string").coalesce(1) \
            .write.mode("overwrite").text(f"{out}/model_summary")
        spark.createDataFrame([(k, str(v)) for k, v in numbers], "name string, value string").coalesce(1) \
            .write.mode("overwrite").option("header", True).csv(f"{out}/model_numbers")
        print(f"\nwrote {out}/model_summary and {out}/model_numbers")
    spark.stop()


if __name__ == "__main__":
    main()
