# AWS runbook

These are the commands that run the pipeline on AWS Glue, S3, and Athena.

Replace `<bucket>` with your bucket name, `<account-id>` with your 12-digit account id, and `<role-arn>` with the ARN of an IAM role that Glue can assume and that can read and write the bucket. Do not paste the account id or keys into any file in this repo or show them in a screenshot.

Costs: S3 storage for about 0.5 GB of raw files is a few cents a month. Each Glue job at 2 workers of type G.1X bills 2 DPU for the time it runs, with a 1 minute minimum. At the current list price of $0.44 per DPU-hour, a 3 minute job is about $0.04. Athena bills $5 per TB scanned and these tables are a few MB. Check current prices on the AWS pricing pages before running. The whole run should stay well under $1, far below the $10 gate.

## 0. Budget alert at $25 (before anything else)

In the console: Billing and Cost Management, Budgets, Create budget, "Monthly cost budget", amount 25, alert at 80 percent of actual cost, your email. The console is simpler than the CLI for this one.

## 1. Bucket and raw files

```
aws s3 mb s3://<bucket>
aws s3api put-public-access-block --bucket <bucket> --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# Raw files go up unchanged. Only the two sources the pipeline reads are needed.
aws s3 sync data/raw/isone_hourlysysload s3://<bucket>/raw/isone_hourlysysload
aws s3 sync data/raw/noaa_lcd            s3://<bucket>/raw/noaa_lcd
aws s3 cp reference/btm_solar_by_year.csv s3://<bucket>/raw/reference/btm_solar_by_year.csv
aws s3 cp manifest/downloads.csv          s3://<bucket>/raw/manifest/downloads.csv

# Scripts
aws s3 sync pipeline s3://<bucket>/scripts --exclude "*" --include "*.py"
```

The response variable comes from the hourly native load files, so the zonal workbooks are not needed in the bucket. The 2014 workbook is only used locally for figure f1.

## 2. Glue jobs

Glue version 5.0 runs Spark 3.5, the same line as the local tests (3.5.3). One job per script. `common.py` is passed to each as an extra Python file.

```
for job in clean_hourly_load clean_hourly_weather build_daily_model fit_model; do
  aws glue create-job --name duck-$job \
    --role <role-arn> \
    --glue-version 5.0 --worker-type G.1X --number-of-workers 2 --timeout 20 \
    --command "Name=glueetl,ScriptLocation=s3://<bucket>/scripts/$job.py,PythonVersion=3" \
    --default-arguments '{"--base":"s3://<bucket>","--extra-py-files":"s3://<bucket>/scripts/common.py","--additional-python-modules":"holidays==0.83,scipy","--enable-continuous-cloudwatch-log":"true"}'
done
```

Corrections found on the first run:
- Glue 5.0 does not include scipy. `fit_model` needs `"--additional-python-modules":"holidays==0.83,scipy==1.13.1"`.
- Add `--tags project=duck-curve` to each `create-job` so the cost can be found later.
- Starting a job twice gives `ConcurrentRunsExceededException`. It is harmless: the first run is still going.

Run them in order and wait for each to succeed before the next:

```
aws glue start-job-run --job-name duck-clean_hourly_load
aws glue start-job-run --job-name duck-clean_hourly_weather
aws glue start-job-run --job-name duck-build_daily_model
aws glue start-job-run --job-name duck-fit_model
aws glue get-job-runs --job-name duck-build_daily_model --max-items 1
```

Each job prints the same summary tables as the local run. They are in the job's CloudWatch output log. Compare them with the local output: the duck days per year table must be identical.

`fit_model` in Glue writes `results/model_summary/` and `results/model_numbers/` in the bucket. The figures and `results/numbers.csv` are built locally by `make_results.py`.

## 3. Glue Data Catalog

```
aws glue create-database --database-input Name=duck_curve
aws glue create-crawler --name duck-curated --role <role-arn> --database-name duck_curve \
  --targets '{"S3Targets":[{"Path":"s3://<bucket>/curated/hourly_load/"},{"Path":"s3://<bucket>/curated/hourly_weather/"},{"Path":"s3://<bucket>/curated/daily_model/"}]}'
aws glue start-crawler --name duck-curated
aws glue get-tables --database-name duck_curve --query 'TableList[].Name'
```

## 4. Athena

In the Athena console, set a query result location such as `s3://<bucket>/athena-results/`, choose database `duck_curve`, and run the statements in `pipeline/athena_validation.sql` one at a time.


## 5. Cost

After the runs, Billing and Cost Management, Cost Explorer, filter to the days of the run. Put the total into `results/numbers.csv` as `aws_total_cost_usd` (it currently reads "not yet run in AWS").

## 6. Clean up when the report is accepted

```
aws glue delete-crawler --name duck-curated
for job in clean_hourly_load clean_hourly_weather build_daily_model fit_model; do aws glue delete-job --job-name duck-$job; done
```

