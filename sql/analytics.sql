-- Spark SQL examples after running the pipeline.
-- Query Delta paths without requiring Hive metastore registration.

-- Latest hourly machine effectiveness and data-completeness signal.
SELECT production_hour, machine_id,
       observed_minutes, planned_minutes,
       ROUND(100 * sample_coverage, 2) AS coverage_pct,
       ROUND(100 * availability, 2) AS availability_pct,
       ROUND(100 * performance, 2) AS performance_pct,
       ROUND(100 * quality, 2) AS quality_pct,
       ROUND(100 * oee, 2) AS oee_pct,
       units_produced, defect_count,
       ROUND(100 * defect_rate, 2) AS defect_pct,
       ROUND(throughput_units_per_planned_hour, 2) AS throughput_per_planned_hour
FROM delta.`/data/gold_hourly`
ORDER BY production_hour DESC, machine_id;

-- Operational priority: worst-performing observed machine-hours.
SELECT production_hour, machine_id,
       ROUND(oee * 100, 2) AS oee_pct,
       ROUND(sample_coverage * 100, 2) AS telemetry_coverage_pct
FROM delta.`/data/gold_hourly`
WHERE observed_minutes > 0
ORDER BY oee, sample_coverage
LIMIT 10;

-- Producer-level duplicate delivery check (Bronze retains all occurrences).
SELECT topic, partition, offset, COUNT(*) AS copies
FROM delta.`/data/bronze`
GROUP BY topic, partition, offset
HAVING COUNT(*) > 1;

-- Review synthetic invalid/renamed records kept in quarantine.
SELECT production_date, topic, partition, offset, raw_payload
FROM delta.`/data/quarantine`
ORDER BY production_date, offset;
