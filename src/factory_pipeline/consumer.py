"""Independent Kafka group for demonstrating offsets, lag, and rebalancing."""
from __future__ import annotations

import argparse
import os
from kafka import KafkaConsumer


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--group", default="factory-observer")
    p.add_argument("--max-messages", type=int, default=20)
    args = p.parse_args()
    consumer = KafkaConsumer(
        "factory.machine-events", bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:19092"),
        group_id=args.group, enable_auto_commit=False, auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    count = 0
    try:
        for msg in consumer:
            print(f"{msg.topic} partition={msg.partition} offset={msg.offset} key={msg.key!r}")
            count += 1
            if count >= args.max_messages:
                break
        consumer.commit()
    finally:
        consumer.close()
    print(f"Committed offsets for {count} messages (group={args.group}).")


if __name__ == "__main__":
    main()
