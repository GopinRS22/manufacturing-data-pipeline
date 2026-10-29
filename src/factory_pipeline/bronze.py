"""Raw JSON Kafka -> append-only Delta Bronze, including invalid/duplicate records."""
from __future__ import annotations

import argparse
import os
from pyspark.sql import functions as F
from factory_pipeline.spark_common import spark_session, path


def main() -> None:
    parser = argparse.ArgumentParser()
    trigger = parser.add_mutually_exclusive_group(required=True)
    trigger.add_argument("--available-now", action="store_true")
    trigger.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    spark = spark_session("factory-kafka-bronze")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:19092"))
           .option("subscribe", "factory.machine-events")
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")
           .option("maxOffsetsPerTrigger", "5000")
           .load())
    bronze = raw.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.col("key").cast("string").alias("record_key"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_time"),
    )
    writer = (bronze.writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", path("checkpoints/bronze")))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
    q = writer.start(path("bronze"))
    print(f"Bronze query started ({'availableNow' if args.available_now else 'continuous'})", flush=True)
    q.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
