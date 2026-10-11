"""Shared helpers for the pipeline jobs.

Every job takes --base, the root that holds raw/ and curated/. Locally that is
the repo's data/ folder. In AWS Glue it is the bucket, for example
s3://my-bucket. The same script runs in both places without edits.
"""
import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
STUDY_START, STUDY_END = "2019-06-01", "2026-09-29"


def parse_args(description: str):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--base", default=str(ROOT / "data"),
                   help="folder or s3:// bucket that holds raw/ and curated/")
    args, _ = p.parse_known_args()          # Glue passes extra arguments of its own
    args.base = args.base.rstrip("/")
    return args


def reference_dir(base: str) -> str:
    """Hand-entered reference tables: raw/reference/ in the bucket, reference/ in the repo."""
    return f"{base}/raw/reference" if base.startswith("s3") else str(ROOT / "reference")


def spark_session(app_name: str) -> SparkSession:
    # Spark starts separate Python worker processes. Without this they use whatever
    # "python3" is first on the PATH, which can be a different version from the one
    # running this script, and Spark then refuses to run.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName(app_name)
             # All timestamps are handled in UTC and converted explicitly, so
             # results do not depend on the clock of the machine running the job.
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def show(title: str, df, n: int = 40):
    print(f"\n{title}")
    df.show(n, truncate=False)
