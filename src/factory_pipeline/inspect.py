"""Display a few Delta Gold rows and source counts without external BI tools."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from factory_pipeline.spark_common import spark_session, exists, path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10)
    args = parser.parse_args()
    spark = spark_session("factory-inspect")
    try:
        for name in ("bronze", "silver", "quarantine", "gold_hourly", "gold_daily", "quality_daily", "reconciliation_daily"):
            if not exists(name):
                print(name, "NOT FOUND")
                continue
            df = spark.read.format("delta").load(path(name))
            print("\n====", name, "total rows:", df.count(), "====")
            if name in ("gold_hourly", "gold_daily", "quality_daily", "reconciliation_daily"):
                df.show(args.rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
