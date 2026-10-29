"""Bounded Delta jobs: Gold, quality, reconciliation, business exports.

Launch with scripts/submit.sh, not plain python, so Delta JARs are loaded.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F
from factory_pipeline.spark_common import spark_session, path, exists
from factory_pipeline.validation import add_validity, parse_raw

IDEAL_UNITS_PER_MIN = 8.0  # assumed nominal machine capacity; not a measured factory value


def table(spark, name: str) -> DataFrame:
    if not exists(name):
        raise RuntimeError(f"Missing Delta {name} table. Run Bronze and Silver first.")
    return spark.read.format("delta").load(path(name))


def infer_day(spark, requested: str) -> str:
    if requested != "auto":
        datetime.strptime(requested, "%Y-%m-%d")
        return requested
    mx = table(spark, "silver").select(F.max(F.to_date("event_time")).alias("d")).first()["d"]
    if mx is None:
        raise RuntimeError("Silver has no valid records; cannot infer date.")
    return mx.isoformat()


def day_raw(spark, day: str) -> DataFrame:
    parsed = add_validity(parse_raw(table(spark, "bronze")))
    # Invalid event timestamps fall back to Kafka's timestamp for quarantine.
    return (parsed.withColumn("production_date", F.coalesce(F.to_date("event_time"), F.to_date("kafka_timestamp")))
            .where(F.col("production_date") == F.lit(day)))


def day_silver(spark, day: str) -> DataFrame:
    return (table(spark, "silver")
            .where(F.to_date("event_time") == F.lit(day)))


def overwrite_day(df: DataFrame, name: str, day: str) -> None:
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"production_date = '{day}'")
     .partitionBy("production_date")
     .save(path(name)))


def gold(spark, day: str, fail_after_daily: bool = False) -> None:
    silver = day_silver(spark, day)
    minute = F.date_trunc("minute", F.col("event_time"))
    # Producer contract: one interval sample per machine per minute.
    # If a duplicate has a DIFFERENT event_id, pick the latest received sample.
    unique_samples = (silver.withColumn("sample_minute", minute)
                      .withColumn("sample_rank", F.row_number().over(
                          Window.partitionBy("machine_id", "sample_minute")
                          .orderBy(F.col("ingestion_time").desc(), F.col("offset").desc())))
                      .where("sample_rank = 1"))
    unique_samples = unique_samples.withColumn("production_date", F.to_date("sample_minute"))
    unique_samples = unique_samples.withColumn("production_hour", F.date_trunc("hour", "sample_minute"))

    def aggregate(keys: list[str], planned_minutes: int) -> DataFrame:
        by_machine = (unique_samples.groupBy(*keys)
                      .agg(F.count("sample_minute").alias("observed_minutes"),
                           F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running_minutes"),
                           F.sum("units_produced").alias("units_produced"),
                           F.sum("defect_count").alias("defect_count")))
        by_machine = (by_machine
                      .withColumn("planned_minutes", F.lit(planned_minutes))
                      .withColumn("sample_coverage", F.least(F.lit(1.0), F.col("observed_minutes") / F.lit(planned_minutes)))
                      .withColumn("availability", F.least(F.lit(1.0), F.col("running_minutes") / F.lit(planned_minutes)))
                      .withColumn("performance", F.when(F.col("running_minutes") > 0,
                          F.least(F.lit(1.0), F.col("units_produced") / (F.lit(IDEAL_UNITS_PER_MIN) * F.col("running_minutes"))))
                          .otherwise(F.lit(0.0)))
                      .withColumn("quality", F.when(F.col("units_produced") > 0,
                          (F.col("units_produced") - F.col("defect_count")) / F.col("units_produced"))
                          .otherwise(F.lit(0.0)))
                      .withColumn("defect_rate", F.when(F.col("units_produced") > 0,
                          F.col("defect_count") / F.col("units_produced")).otherwise(F.lit(0.0)))
                      .withColumn("throughput_units_per_planned_hour",
                          F.col("units_produced") * F.lit(60.0 / planned_minutes))
                      .withColumn("oee", F.col("availability") * F.col("performance") * F.col("quality")))
        return by_machine

    # Fixed demo calendar: 24/7 planned production. Real deployments need shifts,
    # breaks, planned maintenance and each machine's rated speed from a master table.
    overwrite_day(aggregate(["production_date", "machine_id"], 1440), "gold_daily", day)
    if fail_after_daily:
        raise RuntimeError("INJECTED CRASH: daily Gold committed; hourly Gold not yet committed")
    overwrite_day(aggregate(["production_date", "production_hour", "machine_id"], 60), "gold_hourly", day)
    print(f"Gold daily/hourly partitions rebuilt for {day} (deterministic overwrite)")


def quality(spark, day: str) -> None:
    raw = day_raw(spark, day).cache()
    total = raw.count()
    valid = raw.where("is_valid = true")
    valid_count = valid.count()
    distinct_valid = valid.select("event_id").distinct().count()
    invalid_count = total - valid_count
    duplicate_count = valid_count - distinct_valid
    additive = raw.where(F.get_json_object("raw_payload", "$.firmware_version").isNotNull()).count()
    breaking = raw.where(
        F.get_json_object("raw_payload", "$.temp_c").isNotNull()
        & F.get_json_object("raw_payload", "$.temperature_c").isNull()).count()
    silver_count = day_silver(spark, day).count()
    quarantined = (raw.where("is_valid = false")
                   .select("production_date", "topic", "partition", "offset", "raw_payload", "kafka_timestamp"))
    overwrite_day(quarantined, "quarantine", day)
    metric = {
        "production_date": day,
        "bronze_records": total,
        "valid_raw_records": valid_count,
        "invalid_records": invalid_count,
        "valid_duplicate_event_ids": duplicate_count,
        "distinct_valid_event_ids": distinct_valid,
        "silver_records": silver_count,
        "additive_schema_records": additive,
        "breaking_schema_records": breaking,
        "check_status": "WARN" if (invalid_count or duplicate_count) else "PASS",
    }
    overwrite_day(spark.createDataFrame([metric]), "quality_daily", day)
    raw.unpersist()
    print(json.dumps(metric, indent=2, default=str))


def reconcile(spark, day: str) -> None:
    raw = day_raw(spark, day).where("is_valid = true")
    by_id = raw.groupBy("event_id").agg(F.min("event_time").alias("event_time"))
    silver_ids = day_silver(spark, day).select("event_id").distinct()
    missing = by_id.join(silver_ids, on="event_id", how="left_anti")
    missing_count = missing.count()
    # Diagnostic only: stream watermark is MAX EVENT TIME observed so far - delay,
    # not a wall-clock latency test. Order of microbatches also matters.
    max_raw_event = raw.agg(F.max("event_time")).first()[0]
    possibly_too_late = (missing.where(F.col("event_time") <
                         F.lit(max_raw_event).cast("timestamp") - F.expr("INTERVAL 45 MINUTES")).count()
                         if max_raw_event else 0)
    unexplained = missing_count - possibly_too_late
    metric = {
        "production_date": day,
        "missing_valid_event_ids": missing_count,
        "missing_older_than_max_event_minus_45m": possibly_too_late,
        "missing_within_global_45m_horizon": unexplained,
        "reconciliation_status": "REVIEW" if missing_count else "PASS",
    }
    overwrite_day(spark.createDataFrame([metric]), "reconciliation_daily", day)
    print(json.dumps(metric, indent=2, default=str))
    # Don't fail an Airflow DAG solely because an out-of-order record missed the
    # streaming watermark; alert for review and possible governed Bronze replay.


def write_csv(pathname: Path, data: list[dict]) -> None:
    if not data:
        pathname.write_text("No rows\n", encoding="utf-8")
        return
    with pathname.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def report(spark, day: str) -> None:
    root = Path(path("reports")) / day
    root.mkdir(parents=True, exist_ok=True)
    tables = {}
    for table_name in ["gold_hourly", "gold_daily", "quality_daily", "reconciliation_daily"]:
        df = table(spark, table_name).where(F.col("production_date") == F.lit(day))
        records = [r.asDict(recursive=True) for r in df.orderBy("machine_id").collect()] if "gold" in table_name else [r.asDict() for r in df.collect()]
        tables[table_name] = records
        if "gold" in table_name:
            write_csv(root / f"{table_name}.csv", records)
        else:
            (root / f"{table_name}.json").write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
    hourly = tables["gold_hourly"]
    total_units = sum(int(r["units_produced"]) for r in hourly)
    total_defects = sum(int(r["defect_count"]) for r in hourly)
    total_observed = sum(int(r["observed_minutes"]) for r in hourly)
    avg_hourly_oee = sum(float(r["oee"]) for r in hourly) / len(hourly) if hourly else 0
    lines = [f"# Factory KPI report — {day} UTC", "",
             "Synthetic factory data only. Each observation represents a one-minute interval.", "",
             f"- Hourly machine rows: {len(hourly)}",
             f"- Produced units across hourly windows: {total_units}",
             f"- Defects: {total_defects}",
             f"- Defect rate: {total_defects / total_units:.2%}" if total_units else "- Defect rate: n/a (zero output)",
             f"- Mean machine-hour OEE (unweighted): {avg_hourly_oee:.2%}",
             f"- Observed machine-minutes: {total_observed}", "",
             "## Metric contract", "",
             "Availability = RUNNING minutes / planned production minutes.",
             "Performance = min(1, units / (8 rated units/min × RUNNING minutes)).",
             "Quality = (units − defects) / units; defect rate = defects / units.",
             "OEE = availability × performance × quality.",
             "Throughput = produced units per planned hour.",
             "Coverage = observed one-minute samples / planned minutes.", "",
             "**Assumptions:** 24/7 production calendar, 60 planned minutes per hour, 1,440 planned minutes/day; "
             "missing samples conservatively count as non-operating. Demo OEE is not an audited plant OEE. "
             "Partial hours/days have low OEE until planned intervals are covered.", "",
             "For raw exceptions, inspect /data/quarantine and the quality/reconciliation JSON files.", ""]
    (root / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"Saved report to {root}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("job", choices=["gold", "quality", "reconcile", "report"])
    p.add_argument("--date", default="auto", help="UTC YYYY-MM-DD; 'auto' uses latest Silver event date")
    p.add_argument("--fail-after-daily-write", action="store_true", help="Gold crash demonstration ONLY")
    args = p.parse_args()
    if args.fail_after_daily_write and args.job != "gold":
        p.error("--fail-after-daily-write only works with gold")
    spark = spark_session("factory-" + args.job)
    day = infer_day(spark, args.date)
    try:
        if args.job == "gold":
            gold(spark, day, args.fail_after_daily_write)
        elif args.job == "quality":
            quality(spark, day)
        elif args.job == "reconcile":
            reconcile(spark, day)
        else:
            report(spark, day)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
