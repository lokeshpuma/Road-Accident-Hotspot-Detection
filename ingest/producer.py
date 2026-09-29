#!/usr/bin/env python3
"""Replay STATS19 collisions into Kafka, one event-day at a time.

Examples
  # backfill 90 days before the first as-of date, as fast as possible
  python ingest/producer.py --csv data/dft-road-casualty-statistics-collision-last-5-years.csv \
         --from 2025-07-05 --to 2025-10-02
  # simulate one day of the live feed
  python ingest/producer.py --csv ... --from 2025-10-03 --to 2025-10-03
"""
import argparse
import csv
import json
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "accident-reports")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def load(path):
    """Return {date: [event, ...]} for valid rows, and a count of skipped rows."""
    by_day, skipped = defaultdict(list), 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            try:
                d = datetime.strptime(r["date"], "%d/%m/%Y").date()
                lat, lon = float(r["latitude"]), float(r["longitude"])
                if not (49.0 <= lat <= 61.0 and -9.0 <= lon <= 2.5):
                    raise ValueError("outside Great Britain bounding box")
                hh, mm = r["time"].split(":")
                by_day[d].append({
                    "collision_index": r["collision_index"],
                    "date": d.isoformat(),
                    "time": f"{int(hh):02d}:{int(mm):02d}",
                    "latitude": lat,
                    "longitude": lon,
                    "severity": int(r["collision_severity"]),
                    "casualties": int(r["number_of_casualties"]),
                    "vehicles": int(r["number_of_vehicles"]),
                    "police_force": int(r["police_force"]),
                })
            except (KeyError, ValueError):
                skipped += 1  # missing/invalid coordinates, bad time, etc.
    return by_day, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    ap.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    ap.add_argument("--sleep", type=float, default=0.0, help="seconds between event days")
    args = ap.parse_args()

    by_day, skipped = load(args.csv)
    print(f"loaded {sum(map(len, by_day.values()))} valid rows, skipped {skipped}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=5,
        linger_ms=50,
        compression_type="gzip",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

    day, total = args.start, 0
    while day <= args.end:
        events = by_day.get(day, [])
        for e in events:
            producer.send(TOPIC, key=e["collision_index"], value=e)
        producer.flush()
        total += len(events)
        print(f"{day}: sent {len(events)} events")
        if args.sleep:
            time.sleep(args.sleep)
        day += timedelta(days=1)
    producer.close()
    print(f"done, {total} events")


if __name__ == "__main__":
    main()
