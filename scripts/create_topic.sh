#!/usr/bin/env bash
set -euo pipefail
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic accident-reports --partitions 6 --replication-factor 1
docker compose exec namenode hdfs dfs -mkdir -p /data/stats19/raw /data/stats19/output /data/stats19/tmp
