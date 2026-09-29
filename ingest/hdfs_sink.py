#!/usr/bin/env python3
"""Kafka -> HDFS sink. Writes TSV micro-batches to /data/stats19/raw/dt=YYYY-MM-DD/."""
import json
import os
import signal
import time

from hdfs import InsecureClient
from kafka import KafkaConsumer

TOPIC = os.getenv("KAFKA_TOPIC", "accident-reports")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WEBHDFS = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
RAW = os.getenv("HDFS_RAW", "/data/stats19/raw")
FLUSH_RECORDS = int(os.getenv("FLUSH_RECORDS", "20000"))
FLUSH_SECS = float(os.getenv("FLUSH_SECS", "10"))

running = True


def stop(*_):
    global running
    running = False


def to_tsv(e):
    return "\t".join(str(x) for x in (
        e["collision_index"], e["date"], e["time"],
        e["latitude"], e["longitude"], e["severity"], e["casualties"])) + "\n"


class Sink:
    def __init__(self):
        self.client = InsecureClient(WEBHDFS, user=HDFS_USER)
        self.buf = {}  # (date, partition) -> {"first", "last", "lines"}

    def add(self, msg):
        try:
            e = json.loads(msg.value)
            line = to_tsv(e)
        except (ValueError, KeyError):
            print(f"skip malformed message p{msg.partition}@{msg.offset}")
            return
        b = self.buf.setdefault((e["date"], msg.partition),
                                {"first": msg.offset, "last": msg.offset, "lines": []})
        b["last"] = msg.offset
        b["lines"].append(line)

    def flush(self, consumer):
        for (dt, part), b in self.buf.items():
            folder = f"{RAW}/dt={dt}"
            name = f"p{part}-o{b['first']:012d}-{b['last']:012d}.tsv"
            tmp, final = f"{folder}/_inflight-{name}", f"{folder}/{name}"
            self.client.write(tmp, data="".join(b["lines"]), overwrite=True, encoding="utf-8")
            if self.client.status(final, strict=False):
                self.client.delete(tmp)          # identical batch already written (replay)
            else:
                self.client.rename(tmp, final)   # atomic publish
        n = sum(len(b["lines"]) for b in self.buf.values())
        self.buf.clear()
        consumer.commit()                         # commit only after HDFS write succeeded
        print(f"flushed {n} records")


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="hdfs-sink",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=5000,
    )
    sink, pending, last_flush = Sink(), 0, time.time()
    while running:
        for _tp, msgs in consumer.poll(timeout_ms=1000).items():
            for m in msgs:
                sink.add(m)
                pending += 1
        if pending and (pending >= FLUSH_RECORDS or time.time() - last_flush >= FLUSH_SECS):
            sink.flush(consumer)
            pending, last_flush = 0, time.time()
    if pending:
        sink.flush(consumer)
    consumer.close()


if __name__ == "__main__":
    main()
