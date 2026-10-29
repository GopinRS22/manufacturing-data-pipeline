"""Bronze Delta -> validated and watermark-deduplicated Delta Silver."""
from __future__ import annotations

import argparse
from pyspark.sql import functions as F
from factory_pipeline.spark_common import spark_session, path
from factory_pipeline.validation import parse_raw, add_validity


def main() -> None:
    parser = argparse.ArgumentParser()
    trigger = parser.add_mutually_exclusive_group(required=True)
    trigger.add_argument("--available-now", action="store_true")
    trigger.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    spark = spark_session("factory-bronze-silver")
    bronze = spark.readStream.format("delta").load(path("bronze"))
    valid = add_validity(parse_raw(bronze)).where("is_valid = true")
    # State is bounded by event-time watermark. Same event_id is allowed only once
    # while represented in state; records older than the watermark can be dropped.
    dedup = (valid.withWatermark("event_time", "45 minutes")
             .dropDuplicatesWithinWatermark(["event_id"])
             .drop("is_valid", "raw_payload"))
    writer = (dedup.writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", path("checkpoints/silver")))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
    q = writer.start(path("silver"))
    print(f"Silver query started ({'availableNow' if args.available_now else 'continuous'})", flush=True)
    q.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
