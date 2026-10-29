# Standalone reference example — 2026-10-27 10:00–10:59 UTC

This file is calculated in pure Python from the included 601 synthetic JSON messages; it is **not** a result from running Kafka, Spark, Delta or Airflow.

- Emitted Kafka-like messages: 601
- Exact duplicate event ID: 1 extra delivery
- Breaking schema events: 1 (`temp_c` instead of `temperature_c`)
- Invalid count events: 1 (defects > units)
- Accepted raw records (before event-ID dedup): 599
- Unique valid event IDs: 598
- Per-machine production minutes in model: 60 planned, missing intervals flagged
- Accepted distinct minute samples across all machines: 598
- Units produced: 3247
- Defects: 82
- Arithmetic mean hourly OEE: 65.94%

`expected_hourly_kpis.csv` is a reference for formulas. Integrated output is dependent on Spark watermark/order and Spark-side conflict-resolution semantics; compare results rather than assuming exactly the same.
