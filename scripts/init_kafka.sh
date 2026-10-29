#!/usr/bin/env bash
set -euo pipefail
docker compose up -d --wait kafka
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic factory.machine-events --partitions 6 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --describe --topic factory.machine-events
