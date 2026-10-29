"""Kafka producer; emits all ten machine records per synthetic minute."""
from __future__ import annotations

import argparse
import json
import os
import time
from kafka import KafkaProducer
from factory_pipeline.events import demo_events, MACHINES


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default="factory.machine-events")
    p.add_argument("--ticks", type=int, default=60, help="Each tick represents one synthetic minute")
    p.add_argument("--sleep", type=float, default=0.0, help="Real seconds between synthetic minutes")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--inject", nargs="*", choices=["duplicate", "late", "schema", "invalid"], default=[])
    args = p.parse_args()
    if args.sleep < 0:
        p.error("--sleep must be non-negative")
    rows = demo_events(ticks=args.ticks, seed=args.seed, inject=frozenset(args.inject))
    producer = KafkaProducer(bootstrap_servers=args.bootstrap.split(","), acks="all", retries=5,
                             key_serializer=lambda x: x.encode("utf-8"),
                             value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    try:
        prev_ts = None
        for record in rows:
            # Group events in 10-machine minute ticks, keeping anomaly emission order.
            if prev_ts is not None and record["machine_id"] == MACHINES[0] and args.sleep:
                time.sleep(args.sleep)
            producer.send(args.topic, key=record["machine_id"], value=record).get(timeout=30)
            prev_ts = record["timestamp"]
        producer.flush(timeout=30)
    finally:
        producer.close()
    print(f"Published {len(rows)} JSON messages to {args.topic}; "
          f"base={args.ticks * len(MACHINES)}, injection={args.inject}")


if __name__ == "__main__":
    main()
