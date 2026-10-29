"""Spark parser/validation shared by streaming Silver and daily data-quality checks."""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("machine_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("units_produced", T.LongType()),
    T.StructField("defect_count", T.LongType()),
])


def parse_raw(df: DataFrame) -> DataFrame:
    raw = df.withColumn("p", F.from_json("raw_payload", SCHEMA))
    return raw.select(
        "raw_payload", "topic", "partition", "offset", "kafka_timestamp", "ingestion_time",
        F.col("p.event_id").alias("event_id"),
        F.col("p.machine_id").alias("machine_id"),
        F.to_timestamp(F.col("p.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        F.col("p.temperature_c").alias("temperature_c"),
        F.col("p.status").alias("status"),
        F.col("p.units_produced").alias("units_produced"),
        F.col("p.defect_count").alias("defect_count"),
    )


def add_validity(df: DataFrame) -> DataFrame:
    valid = (
        F.col("event_id").rlike(r"^[0-9a-fA-F-]{36}$")
        & F.col("machine_id").rlike(r"^M-(0[1-9]|10)$")
        & F.col("event_time").isNotNull()
        & F.col("temperature_c").between(-20, 150)
        & F.col("status").isin("RUNNING", "IDLE", "DOWN")
        & (F.col("units_produced") >= 0)
        & (F.col("defect_count") >= 0)
        & (F.col("defect_count") <= F.col("units_produced"))
        & F.when(F.col("status") != "RUNNING", F.col("units_produced") == 0).otherwise(F.lit(True))
    )
    return df.withColumn("is_valid", F.coalesce(valid, F.lit(False)))
