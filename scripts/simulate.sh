#!/usr/bin/env bash
# usage: simulate.sh 2025-10-03 2025-12-31
# For each day: publish that day's collisions to Kafka, wait for the sink, run the daily job.
set -euo pipefail
cd "$(dirname "$0")/.."
FROM="$1"; TO="$2"
CSV=data/dft-road-casualty-statistics-collision-last-5-years.csv
d="$FROM"
while [[ ! "$d" > "$TO" ]]; do
  docker compose exec -T app python ingest/producer.py --csv "$CSV" --from "$d" --to "$d"
  sleep 15                      # sink flushes after FLUSH_SECS (10 s)
  ./scripts/run_daily.sh "$d"
  d=$(date -I -d "$d + 1 day" 2>/dev/null || date -j -v+1d -f "%Y-%m-%d" "$d" +%F 2>/dev/null || python3 -c "import datetime; print(datetime.date.fromisoformat('$d') + datetime.timedelta(days=1))")
done
