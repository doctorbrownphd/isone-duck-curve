# Pipeline

The data engineering pipeline. Each job is a PySpark script that takes `--base`, the folder or bucket holding `raw/` and `curated/`. Locally that defaults to the repo's `data/` folder. In AWS Glue it is `s3://<bucket>`. See `AWS_RUNBOOK.md` for the AWS run.

## Local run

Needs Java 17 and the packages in `requirements.txt`. On macOS, `brew install openjdk@17` provides it.

```
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
cd pipeline

python ../step0/download_hourlysysload.py   # Extract 1: ISO-NE native load (needs .env), about an hour
python extract_noaa_lcd.py                  # Extract 2: NOAA LCD, 48 files
python ../step0/duck_day_check_hourly.py    # Step 0: definition check against ISO-NE's counts
python clean_hourly_load.py                 # raw JSON -> curated/hourly_load
python clean_hourly_weather.py              # raw LCD  -> curated/hourly_weather
python build_daily_model.py                 # join and aggregate -> curated/daily_model
python fit_model.py                         # model, likelihood ratio test, 2025 test set
python make_results.py                      # figures, numbers.csv, daily table sample
```

Both extract scripts skip files that are already on disk, so a rerun only downloads what is missing. The manifest is append-only.

Each job prints row counts, what was dropped and why, and a few sample rows.

## What each job writes

| Job | Output | Rows (local run, 2026-10-05) |
|---|---|---|
| `clean_hourly_load.py` | `curated/hourly_load`, Parquet by year | 64,294 (64,296 read, 2 exact duplicates removed) |
| `clean_hourly_weather.py` | `curated/hourly_weather`, Parquet by year | 406,766 hourly rows kept of 659,920 raw rows |
| `build_daily_model.py` | `curated/daily_model`, Parquet by year | 2,678 days, 492 duck days |
| `fit_model.py` | `results/model_summary.txt` and CSVs | 2,670 days with every predictor |
