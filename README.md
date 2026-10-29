# Manufacturing Data Pipeline — End-to-End Capstone

**Scenario:** Monitor ten fictional factory machines and produce trustworthy production-quality analytics using **Kafka → Spark Structured Streaming → Delta Lake Bronze/Silver/Gold → Airflow**. Every event is **synthetic**; no real factory/customer information is used.

## 1. Architecture and learning goals

```text
Synthetic producer (10 machines; 1 event/machine/minute)
   |
   v
Kafka factory.machine-events (6 partitions; key = machine_id)
   |
   v
Spark query 1: Kafka -> Delta Bronze (raw JSON + topic/partition/offset; checkpoint)
   |
   v
Spark query 2: Bronze -> parse/validate -> 45-minute event-time watermark
                         -> dedupe by event_id -> Delta Silver (checkpoint)
                                                    |
                         +--------------------------+
                         v
                  Airflow bounded jobs
                    - daily/hourly Delta Gold KPIs
                    - quarantine + quality counts
                    - reconciliation + CSV/JSON/Markdown report
```

Two different persistent streaming checkpoints make Kafka-to-Bronze and Bronze-to-Silver independently restartable. Airflow supervises **bounded** runs (`availableNow`) and batch jobs; it does not try to schedule an endless stream inside a DAG. A separate continuous mode is available for hands-on exploration. **Do not run continuous and bounded consumers with the same checkpoint concurrently.**

**Pinned demo versions** (chosen as a compatible Spark 3.5/Delta 3.3 line, not the latest versions): Kafka 4.1.2, Python 3.11, PySpark 3.5.8, Delta Lake 3.3.2, Airflow 3.1.7. Spark uses Java 17 and the `spark-sql-kafka-0-10_2.12:3.5.8` connector. Spark's `dropDuplicatesWithinWatermark` requires Spark 3.5+.

## 2. Prerequisites

- Docker Desktop or Docker Engine + Compose v2; allocate **8–12 GB of RAM** for the combined stack (Airflow plus Spark are memory-intensive).
- Internet connectivity to build images and download the Maven Kafka/Delta JARs on first use.
- macOS/Linux terminal (Windows users can use WSL2). Run commands **from the project directory**.
- `python3` + `pytest` only if running the standalone pure-Python tests; **Docker isn't needed for those tests**.
- Local lab only: Kafka PLAINTEXT, no authentication/TLS, single broker, file-backed Delta paths, local Spark. **Not production security or HA**.

## 3. Fast end-to-end demo

```bash
unzip manufacturing-pipeline-capstone.zip
cd manufacturing-pipeline-capstone
bash scripts/demo.sh
```

The script builds the Spark image, starts Kafka, creates the topic, publishes a simulated 60-minute production hour (600 base records + one exact duplicate, one delayed event, two malformed/schema exceptions), processes the two streams in bounded mode, rebuilds Gold, and writes a report. The fake hour is the **previous complete UTC hour** so hourly denominators are exactly 60 minutes. No need to wait for a real hour.

To inspect Delta tables directly, run `docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/inspect.py`. Spark SQL examples are in `sql/analytics.sql`.

Find results under `data/reports/YYYY-MM-DD/`:

- `gold_hourly.csv` — one row per observed machine/hour, with OEE components
- `gold_daily.csv` — one row per machine/day (24/7 planned-day denominator)
- `quality_daily.json` — invalid records, duplicate IDs, schema exceptions
- `reconciliation_daily.json` — valid raw event IDs missing from Silver
- `summary.md` — business-facing high-level report and metric assumptions

The **exact output numbers vary** if you change the seed or injection switches. The initial demo defaults to `--seed 42` and a consistent per-hour simulation. The provided `sample/` directory contains an independent example report from pure Python; the Docker demo generates its own real Delta-backed report.

## 4. Run each component yourself

```bash
mkdir -p data && chmod 0777 data   # demo-only shared UID permissions
bash scripts/init_kafka.sh
# Builds local PySpark + Java + Kafka client image:
docker compose build spark

# 600 clean records: ten machines x sixty 1-minute intervals
docker compose run --rm spark python -m factory_pipeline.producer --ticks 60

# Bounded streaming, reading the available Kafka/Delta backlog
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/bronze.py --available-now
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/silver.py --available-now

# Idempotently regenerate the current date's Gold and diagnostics
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py gold --date auto
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py quality --date auto
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py reconcile --date auto
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py report --date auto
```

To see a continuous source, run each Spark query in its own terminal with `--continuous` instead of `--available-now`. Keep them separate from scheduled bounded Airflow runs, and terminate them cleanly before switching modes. They can be restarted with the *same* checkpoint locations.

### Kafka consumer-group experiment

```bash
# Read only 20 messages with an independent consumer group, then commit.
docker compose run --rm spark python -m factory_pipeline.consumer --group factory-observer --max-messages 20

# Inspect committed offsets and remaining lag.
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --describe --group factory-observer

# List topic partitions and retention settings.
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --describe --topic factory.machine-events
```

Produce more data and inspect lag again. Open two `consumer.py` processes using the same `--group`; Kafka will rebalance partitions between them. A **different** `--group` reads independently. **Spark Structured Streaming stores its progress in Spark checkpoints, not this `factory-observer` group offset.**

## 5. Data contract

Each sample represents **one one-minute interval per machine**. `units_produced` and `defect_count` are per-interval values **not cumulative counters** (the opposite assumption would require calculating deltas and handling counter resets).

```json
{
  "event_id": "2e16a6d4-8f4f-4d2e-bb6b-66d5cdfa2153",
  "machine_id": "M-03",
  "timestamp": "2026-10-27T10:12:00Z",
  "temperature_c": 72.3,
  "status": "RUNNING",
  "units_produced": 7,
  "defect_count": 0
}
```

**Validation in Silver:** event ID has UUID-like syntax; machine IDs `M-01`...`M-10`; parseable UTC timestamp; temperature -20 to 150 °C; status RUNNING/IDLE/DOWN; non-negative counts, defects not greater than units; non-running records have zero production. Bronze never silently discards events, even invalid JSON values. Invalid raw records remain queryable and go to the **batch-generated quarantine** table for that date.

**Watermark:** `withWatermark("event_time", "45 minutes").dropDuplicatesWithinWatermark(["event_id"])`. Thirty minutes of event-time disorder is within the 45-minute tolerance in a typical stream. The watermark is relative to event-time progress, **not** wall-clock arrival time; older records may be dropped depending on the stream's batch history. Changing watermark duration/stateful logic may require checkpoint migration and a planned replay. Duplicates delayed beyond the watermark are not guaranteed deduplicated.

## 6. Gold KPI definitions — critical interview material

| KPI | Calculation | Interpretation |
| --- | --- | --- |
| Availability | `RUNNING minutes / planned minutes` | A deliberate conservative policy: IDLE and DOWN are non-operating |
| Performance | `min(1, produced units / (8 units/min × RUNNING minutes))` | Assumed nominal speed 8 good-or-defective units/min |
| Quality | `(produced units − defects) / produced units` | Fraction of output not defective |
| OEE | `Availability × Performance × Quality` | Equipment effectiveness under this **declared** model |
| Defect rate | `defects / produced units` | Share of output that is defective |
| Throughput | `produced units × 60 / planned minutes` | Units per planned hour, not just running-hour speed |
| Sample coverage | `distinct observed machine-minutes / planned minutes` | Independent telemetry completeness signal |

Both the hourly (60-minute) and daily (1,440-minute) calculations use a **24/7 synthetic planned-production calendar**. The machine's shift calendar, changeovers, exclusions and **rated ideal cycle time** are unknown; so the project OEE is a reproducible **demonstration** rather than a certified plant OEE. A partial day legitimately looks underutilized because no production has been simulated for its other planned minutes. Missing telemetry lowers measured operating minutes; a real system would separately classify missing signals as UNKNOWN rather than assert physical downtime. Gold chooses the most recently ingested sample if two **different** event IDs claim the same machine/minute; document this conflict-resolution policy.

**Example:** For a 60-minute planned period with 48 running minutes, 360 units and 18 defects, and a nominal 8 units/min:

- Availability `48/60 = 80%`
- Performance `360/(48×8) = 93.75%`
- Quality `(360−18)/360 = 95%`
- **OEE `0.80×0.9375×0.95 = 71.25%`**
- Defect rate `18/360 = 5%`; throughput `360 units/planned hour`

## 7. Deliberate failure and recovery drills

### A. Exact duplicate

```bash
docker compose run --rm spark python -m factory_pipeline.producer --inject duplicate
# Re-run Bronze then Silver availableNow; regenerate quality, Gold and reconciliation.
```

Expected: Bronze has **both** Kafka messages, Silver has one event ID while within watermark, Gold does not double count production. `quality_daily.valid_duplicate_event_ids` increases. **Don't reset/delete checkpoints** to make a test pass.

### B. Delayed record (30 synthetic minutes)

```bash
docker compose run --rm spark python -m factory_pipeline.producer --inject late
```

One minute-12 record is held until after minute 42. Inspect its earlier `event_time` and later Kafka offset. It should be retained in a typical first-run within the 45-minute watermark; verify actual Silver and reconciliation results. The event timestamp and Kafka ingestion timestamp serve different purposes. Test 60+ minute lateness by adapting `events.py` to explore watermark expiration and a governed backfill from Bronze.

### C. Schema drift and invalid counts

```bash
docker compose run --rm spark python -m factory_pipeline.producer --inject schema invalid
```

`firmware_version` is an **additive** JSON field, ignored by the versioned Silver parser but preserved in Bronze. In contrast, one record renames `temperature_c` to `temp_c` (breaking change); validation rejects it. Another record has more defects than units. Both stay in Bronze, appear in the Delta **quarantine** table, and are counted in `quality_daily.json`. Real schema migrations should be versioned and contract-tested; this example deliberately **does not auto-accept a breaking rename**.

### D. Crash during Gold creation (deterministic retry)

```bash
# This command deliberately exits with an error immediately after Gold daily commits.
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py \
  gold --date auto --fail-after-daily-write

# Resume safely: date-scoped replaceWhere rebuilds daily and hourly Gold deterministically.
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py gold --date auto
```

Expected: the first run fails after daily Gold is committed, the retry finishes both tables without appending duplicate KPI rows. Delta transactions are atomic **per table**, not atomically across daily/hourly tables: reports must be published **after** both succeed. The DAG enforces that dependency.

### E. Crash/kill a streaming worker, then resume from checkpoint

```bash
# Ensure no other Bronze query is using this checkpoint.
docker compose run -d --name factory-bronze-crash spark \
  bash scripts/submit.sh src/factory_pipeline/bronze.py --continuous
# Produce data in another terminal; kill after the stream starts.
docker kill --signal=KILL factory-bronze-crash
# Resume the same query/checkpoint in bounded mode (no checkpoint deletion).
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/bronze.py --available-now
# Catch Silver up as well.
docker compose run --rm spark bash scripts/submit.sh src/factory_pipeline/silver.py --available-now
```

Inspect counts/`topic, partition, offset` in Bronze for duplicates (and compare against Kafka). Structured Streaming + Delta checkpoint and transaction-log cooperation handles retry of a committed microbatch. Crash injection timing matters; not every kill interrupts an in-flight commit. Do not claim exactly-once behavior for arbitrary sinks or cross-table transactions. A real pipeline also needs Kafka retention longer than maximum replay/recovery backlog.

**Expected reconciliation:** On the original `--ticks 60 --inject duplicate late schema invalid` scenario, you would normally see 601 raw records, 2 invalid, 1 extra valid duplicate ID, and about 598 distinct valid IDs/rows in Silver (assuming no watermark loss). Treat these as **expectations to verify**, not guaranteed results under every streaming timing/history. The reconciliation job reports missing IDs for review; an older-than-watermark classification is only a diagnostic, not proof of why it was lost.

## 8. Airflow deployment

```bash
# Not alongside active --continuous streams using the same checkpoint.
mkdir -p data && chmod 0777 data
docker compose --profile airflow up -d --build kafka airflow
# Airflow standalone admin credentials are generated in its state volume.
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Visit **http://localhost:8080**, log in as `admin` using the generated password, and open DAG `factory_daily_reconciliation` (`dags/factory_daily.py`). It runs daily UTC and finalizes the **previous UTC calendar day** (Airflow `data_interval_start`). Tasks are `bronze_catchup → silver_catchup → [build_gold, data_quality_and_quarantine] → reconcile → publish_report`. For the ad-hoc **previous-hour replay**, prefer `scripts/demo.sh` with `--date auto` instead of the scheduled previous-day Airflow date.

For demonstration without leaving Airflow running, stop just the Airflow service with `docker compose --profile airflow stop airflow`. Spark and Airflow share a local filesystem, user UID 50000, and Maven-package cache directories are separate. A production platform would use object storage, a distributed Spark deployment, a real metastore/catalog, managed credentials, monitoring and tests at source/sink boundaries.

## 9. Testing, checks and reset

```bash
PYTHONPATH=src python3 -m pytest -q tests
# Inspect image, container and query logs if the integrated run fails.
docker compose logs kafka
# Tear down containers but preserve data/ and Kafka volume:
docker compose --profile airflow --profile tools down
# COMPLETE RESET (irreversible; lab-only):
docker compose --profile airflow --profile tools down -v
rm -rf data/
```

**What to inspect after each run:** (1) Kafka offsets and consumer-group lag, (2) Bronze raw count and unique `(topic,partition,offset)`, (3) Silver valid event ID count, (4) quarantine exceptions, (5) hourly machine coverage and OEE, (6) reconciliation status and report freshness, (7) Airflow task retries.

## 10. Learning sequence / interview checkpoints

| Session | Build / investigate | Explain aloud |
| --- | --- | --- |
| 1 | Event contract + synthetic producer | Per-interval vs cumulative counters, status model |
| 2 | Kafka partitions, keying, consumer groups and lag | Why key by machine? Offset vs timestamp? |
| 3 | Spark Bronze ingestion and persisted checkpoint | What is replayable? Why retain source metadata? |
| 4 | Silver parser, watermark, dedup, quarantine | Why 45 minutes? What gets dropped? |
| 5 | Delta Gold and OEE math | How do missing samples/partial shifts affect OEE? |
| 6 | Airflow DAG, day-scoped writes and reconciliation | Why bounded jobs? Why date-partition overwrite? |
| 7 | Crash and drift drills, troubleshooting | How do you establish commit boundaries and prove recovery? |

### Possible 60-second interview answer

> “I built a synthetic manufacturing telemetry pipeline for ten machines. I keyed JSON events by machine in Kafka, persisted the complete payload and Kafka offsets to a Delta Bronze table, and parsed Bronze incrementally into validated Silver records. Silver uses event-time watermarking and event-ID deduplication to handle delays and retries. I calculate OEE from explicitly defined minute samples and rated speed, publishing coverage alongside effectiveness so missing telemetry isn't hidden. Airflow runs bounded catch-up, daily reconciliation, quality/quarantine and report jobs. I tested duplicate events, a 30-minute delay, schema drift, and partial Gold failures; date-scoped Delta overwrites and checkpoint recovery keep retries safe. The key tradeoff is that source-level at-least-once delivery, watermarks and multi-table publishing each need distinct operational controls.”

## Source references

- [Delta Lake release compatibility](https://docs.delta.io/releases/)
- [Spark 3.5 Structured Streaming Guide](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html)
- [Spark Structured Streaming Kafka connector](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
- [Kafka official Docker examples](https://github.com/apache/kafka/tree/4.1.2/docker/examples)
- [Airflow 3.1 DAG authoring](https://airflow.apache.org/docs/apache-airflow/3.1.7/core-concepts/dags.html)

**Scope and limitations:** Local runnable template, not production-ready, and the full Docker distributed integration was **not executed** in the authoring environment. Pure-Python unit tests and syntax checks were executed. Integration readiness must be confirmed in Docker before claiming passing end-to-end results.
