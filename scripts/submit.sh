#!/usr/bin/env bash
set -euo pipefail
# Package versions are pinned to Spark 3.5.8 / Delta 3.3.2 (Scala 2.12).
exec spark-submit \
  --master 'local[2]' \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.3.2' \
  --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.sql.shuffle.partitions=4' \
  --conf 'spark.ui.enabled=false' \
  --conf 'spark.driver.memory=1g' \
  "$@"
