# PRD — Road Accident Hotspot Detection

| | |
|---|---|
| **Project** | #9 — Road Accident Hotspot Detection |
| **Dataset** | UK Road Safety (STATS19), file: `dft-road-casualty-statistics-collision-last-5-years.csv` |
| **Tech stack** | Apache Kafka · Hadoop HDFS · Hadoop MapReduce (YARN) · Geospatial heatmap (Folium / Leaflet) |
| **Status** | Draft v1.0 |

---

## 1. Overview

Traffic-safety authorities currently plan enforcement from **annual reports**, which are months out of date and hide *when* and *where* risk is concentrated. This project builds a batch-streaming pipeline that:

1. **Ingests** collision reports as events through **Kafka**,
2. **Persists** them on **HDFS**, partitioned by event date,
3. Runs a **MapReduce** job every day that ranks the **top 20 accident hotspots** (location × time-of-day cluster) over a **rolling 90-day window**,
4. Publishes the result as a **geospatial heatmap** plus a ranked table.

## 2. Problem Statement

| Today | Needed |
|---|---|
| Resources allocated from annual reports | Resources allocated from the **last 90 days** |
| Location only ("this district") | Location **and time band** ("this ~2 km cell, weekday evening peak") |
| Refreshed yearly | Refreshed **daily** |
| Tables/PDFs | **Map** an officer can read in seconds |

## 3. Goals and Non-Goals

**Goals**
- G1. Daily-refreshed ranking of the top 20 location-time hotspots over a rolling 90-day window.
- G2. Fully use the stack: Kafka → HDFS → MapReduce → heatmap.
- G3. Reproducible: one `docker compose up`, one script to run a day, one validator that proves MapReduce output equals a pandas reference.
- G4. Safe to re-run: replays and duplicate events must not change results.

**Non-goals (v1)**
- Real-time (sub-minute) alerting; this is a daily batch ranking.
- Snapping collisions to individual road segments or junction-level clustering (DBSCAN/H3 are future work).
- Predictive modelling (forecasting future accidents).
- Authentication / multi-tenant access control.

## 4. Users

| Persona | Need |
|---|---|
| Traffic enforcement planner | "Where and when should patrols go this week?" |
| Road-safety analyst | Compare hotspots across days, drill into severity mix |
| Project evaluator / student | See every stack component working end-to-end |

## 5. Dataset (profiled from the uploaded file)

| Property | Value |
|---|---|
| Records | **513,801** collisions |
| Date range | **2021-01-01 → 2025-12-31** (about 101k–106k per year) |
| Records in the last 90 days (as-of 2025-12-31) | **25,659** with valid coordinates |
| Missing coordinates | **53** rows (dropped, counted, never crash the pipeline) |
| Severity codes (`collision_severity`) | 1 = Fatal (7,553) · 2 = Serious (116,813) · 3 = Slight (389,435) |
| Coordinate range | lat 49.91 → 60.50, lon −7.49 → 1.76 (Great Britain) |
| `date` format | `dd/mm/yyyy` |
| `time` format | `HH:MM` |

Important schema notes (the uploaded file is a newer STATS19 release than many tutorials assume):
- Columns are named **`collision_*`**, not `accident_*` (`collision_index`, `collision_year`, `collision_severity`).
- Columns used by the pipeline: `collision_index, date, time, latitude, longitude, collision_severity, number_of_casualties, number_of_vehicles, police_force`.

## 6. Design Decisions (backed by a prototype on your data)

I prototyped the ranking in pandas on the last 90 days to choose the cluster definition. **The main risk is sparsity**: the finer the cluster, the more the top-20 is filled with clusters containing a single collision.

| Cell size | Time bands | Distinct clusters | Collisions in top-20 clusters (min → max) | Verdict |
|---|---|---|---|---|
| 0.01° (~1 km) | 4 bands | 20,815 | 1 → 12 (most are 1) | Too sparse — one fatal crash dominates |
| **0.02° (~2 km)** | **4 bands** | 16,234 | **6 → 29** | **Chosen** |
| 0.02° (~2 km) | none | 10,134 | 22 → 73 | Ignores "when" |
| 0.05° (~5 km) | 4 bands | 9,451 | 27 → 92 | Too coarse for patrol planning |

**Cluster = (grid cell, time band).**
- **Grid cell:** `floor(lat / 0.02)`, `floor(lon / 0.02)` (~2.2 km north–south).
- **Time bands** (from `time`): `AM_PEAK` 07:00–09:59 · `DAY` 05:00–06:59 & 10:00–15:59 · `PM_PEAK` 16:00–18:59 · `NIGHT` 19:00–04:59.
- Cell size, half-life and severity weights are **configurable** (`Hs.java` / job config).

**Hotspot score** (per collision, summed per cluster):

```
score = severity_weight × 0.5^(age_days / half_life_days)
severity_weight: Fatal = 10, Serious = 5, Slight = 1
half_life_days = 30      age_days = asof − collision_date   (0 ≤ age < 90)
```

Recent collisions count more, so the ranking reacts to new patterns instead of averaging them away. Setting `half_life_days` very large makes it a plain severity-weighted count.

## 7. Functional Requirements

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-1 | Producer publishes each collision as a JSON event to Kafka topic `accident-reports` (key = `collision_index`) | Topic message count equals valid CSV rows in the replayed range |
| FR-2 | Producer can replay a date range day-by-day to simulate a daily feed | `--from D --to D` sends exactly day D |
| FR-3 | HDFS sink consumes the topic and writes TSV files to `/data/stats19/raw/dt=YYYY-MM-DD/` | Files appear per event date; no partial files are visible to MapReduce (`_inflight-` prefix, then rename) |
| FR-4 | Sink commits Kafka offsets only after HDFS write succeeds (at-least-once) | Kill -9 the sink mid-run, restart → no data loss |
| FR-5 | MapReduce Job 1 filters to the 90-day window, **de-duplicates by `collision_index`**, computes cell, band, score | Duplicates counter > 0 when a replay is injected, results unchanged |
| FR-6 | Job 2 aggregates by (cell, band) with a combiner: score, count, centroid, severity mix | `cells/` output has one row per cluster |
| FR-7 | Job 3 outputs the global **top 20** by score (single reducer) | Exactly 20 rows ranked 1..20 (fewer only if fewer clusters exist) |
| FR-8 | Driver takes `asof` date; output is written to `/data/stats19/output/asof=YYYY-MM-DD/` and is idempotent | Re-running the same `asof` overwrites and yields identical output |
| FR-9 | Daily scheduler runs the driver and the heatmap build for yesterday | Cron entry produces a new dated output each day |
| FR-10 | Heatmap: an interactive map with an "All" layer, one layer per time band, and top-20 ranked markers with popups | Opens offline in a browser; layer toggle works |
| FR-11 | Validator compares the MapReduce top 20 with a pandas reference | Same clusters, scores within 1e-3 |

## 8. Non-Functional Requirements

| Area | Target |
|---|---|
| Freshness | Report for day *D* available by 03:00 on *D+1* |
| Runtime | Daily MapReduce chain < 10 min on the single-node dev cluster (window ≈ 25k records) |
| Reliability | At-least-once ingestion + reducer-side de-duplication ⇒ effectively-once results |
| Idempotency | Same `asof` ⇒ byte-identical top-20 |
| Scalability | Kafka partitions = 6; all MapReduce stages are horizontally scalable except the final 20-row merge |
| Observability | Hadoop counters (`malformed`, `outside_window`, `duplicates`), Kafka consumer lag, YARN UI |
| Data protection | STATS19 is anonymised; no personal data stored. Attribution: UK Open Government Licence |

## 9. Architecture

```
                 ┌──────────────┐   JSON events   ┌────────────────────┐
 STATS19 CSV ───►│ producer.py  │────────────────►│ Kafka topic         │
 (replay by day) │ (Python)     │ key=collision_id│ accident-reports    │
                 └──────────────┘                 │ 6 partitions        │
                                                  └─────────┬──────────┘
                                                            │ consumer group "hdfs-sink"
                                                            ▼
                                                  ┌────────────────────┐
                                                  │ hdfs_sink.py        │ commit offsets after write
                                                  └─────────┬──────────┘
                                                            ▼  WebHDFS
   HDFS  /data/stats19/raw/dt=2025-12-31/p3-o…-….tsv   (partitioned by event date)
                                                            │ last 90 dt= folders
                                                            ▼
   ┌──────────────────────── MapReduce on YARN (daily, asof = D) ───────────────────────┐
   │ Job 1  DedupScore : filter window → key=collision_id → 1 record → cell|band + score │
   │ Job 2  Aggregate  : key=cell|band (+combiner) → score, n, centroid, severity mix    │
   │ Job 3  TopN       : per-mapper top-20 → 1 reducer → global top-20                   │
   └──────────────────────────────────────────┬─────────────────────────────────────────┘
                                              ▼
   HDFS /data/stats19/output/asof=D/{cells,top20}
                                              ▼
                              heatmap.py (Folium HeatMap + ranked markers)
                                              ▼
                         site/index.html  +  site/top20.csv   (served on :8000)
```

### 9.1 Kafka event schema (`accident-reports`)

```json
{
  "collision_index": "202417H105824",
  "date": "2024-10-03",
  "time": "16:06",
  "latitude": 54.6984,
  "longitude": -1.247,
  "severity": 2,
  "casualties": 1,
  "vehicles": 2,
  "police_force": 17
}
```

### 9.2 HDFS layout

```
/data/stats19/raw/dt=YYYY-MM-DD/p{partition}-o{firstOffset}-{lastOffset}.tsv   # TSV: id, date, time, lat, lon, severity, casualties
/data/stats19/tmp/asof=YYYY-MM-DD/scored/                                     # Job 1 output (deleted after run)
/data/stats19/output/asof=YYYY-MM-DD/cells/                                   # Job 2 output: all clusters (heat layers)
/data/stats19/output/asof=YYYY-MM-DD/top20/                                   # Job 3 output: ranked top 20
```

Output row formats:
- `cells/`: `cell|band <TAB> score <TAB> count <TAB> centLat <TAB> centLon <TAB> fatal <TAB> serious <TAB> slight`
- `top20/`: `rank <TAB>` + the same fields.

## 10. Project Layout

```
road-hotspot/
├── docker-compose.yml
├── hadoop.env
├── requirements.txt
├── PRD.md
├── data/dft-road-casualty-statistics-collision-last-5-years.csv
├── ingest/
│   ├── producer.py
│   └── hdfs_sink.py
├── mapreduce/
│   ├── pom.xml
│   └── src/main/java/hotspot/
│       ├── Hs.java
│       ├── DedupScore.java
│       ├── Aggregate.java
│       ├── TopN.java
│       └── HotspotDriver.java
├── heatmap/heatmap.py
├── validate/validate_reference.py
├── scripts/
│   ├── create_topic.sh
│   ├── run_daily.sh
│   └── simulate.sh
└── site/
```

---

## 11. Implementation

### 11.1 `docker-compose.yml`

Single-node Kafka (KRaft, no ZooKeeper) and a single-node Hadoop (HDFS + YARN). The `app` container runs the Python components on the same network.

```yaml
services:
  kafka:
    image: apache/kafka:3.7.0
    hostname: kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  namenode:
    image: apache/hadoop:3.3.6
    hostname: namenode
    command: ["hdfs", "namenode"]
    ports: ["9870:9870"]
    env_file: [./hadoop.env]
    environment:
      ENSURE_NAMENODE_DIR: "/tmp/hadoop-root/dfs/name"

  datanode:
    image: apache/hadoop:3.3.6
    command: ["hdfs", "datanode"]
    env_file: [./hadoop.env]

  resourcemanager:
    image: apache/hadoop:3.3.6
    hostname: resourcemanager
    command: ["yarn", "resourcemanager"]
    ports: ["8088:8088"]
    env_file: [./hadoop.env]
    volumes:
      - ./mapreduce/target:/jobs

  nodemanager:
    image: apache/hadoop:3.3.6
    command: ["yarn", "nodemanager"]
    env_file: [./hadoop.env]

  app:
    image: python:3.11-slim
    working_dir: /app
    volumes: ["./:/app"]
    ports: ["8000:8000"]
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      WEBHDFS_URL: http://namenode:9870
      HDFS_USER: root
    command: sh -c "pip install -q -r requirements.txt && sleep infinity"
```

### 11.2 `hadoop.env`

```properties
CORE-SITE.XML_fs.default.name=hdfs://namenode
CORE-SITE.XML_fs.defaultFS=hdfs://namenode
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=1
HDFS-SITE.XML_dfs.webhdfs.enabled=true
HDFS-SITE.XML_dfs.permissions.enabled=false
MAPRED-SITE.XML_mapreduce.framework.name=yarn
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle
YARN-SITE.XML_yarn.nodemanager.resource.memory-mb=4096
YARN-SITE.XML_yarn.scheduler.maximum-allocation-mb=4096
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-am-resource-percent=0.5
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.maximum-allocation-mb=4096
```

> The `apache/hadoop` image reads `FILE.XML_property=value` lines and generates the XML config at start. Image tags and property names can differ between releases; if a container fails to start, check `docker compose logs <service>`.

### 11.3 `requirements.txt`

```
kafka-python==2.0.2
hdfs==2.7.3
folium==0.17.1
pandas==2.2.2
numpy==1.26.4
```

### 11.4 `ingest/producer.py` — CSV → Kafka

```python
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
```

### 11.5 `ingest/hdfs_sink.py` — Kafka → HDFS

Key details: files are written as `_inflight-*` (Hadoop's `FileInputFormat` ignores names beginning with `_`), then renamed atomically, and offsets are committed **only after** all files are safely on HDFS.

```python
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
```

### 11.6 `mapreduce/pom.xml`

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>hotspot</groupId>
  <artifactId>hotspot-mr</artifactId>
  <version>1.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.source>8</maven.compiler.source>
    <maven.compiler.target>8</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>3.3.6</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
```

Build: `cd mapreduce && mvn -q clean package` → `target/hotspot-mr-1.0.jar` (mounted into the ResourceManager container as `/jobs`).

### 11.7 `Hs.java` — shared configuration and helpers

```java
package hotspot;

/** Shared constants and pure helper functions (unit-testable). */
public final class Hs {
    private Hs() {}

    public static final String ASOF   = "hotspot.asof";          // yyyy-MM-dd
    public static final String WINDOW = "hotspot.window.days";   // default 90
    public static final String CELL   = "hotspot.cell.deg";     // default 0.02
    public static final String HALF   = "hotspot.halflife.days"; // default 30
    public static final String TOPN   = "hotspot.topn";          // default 20

    /** Time band from the hour of day (0-23). */
    public static String band(int hour) {
        if (hour >= 7 && hour <= 9)   return "AM_PEAK";
        if (hour >= 16 && hour <= 18) return "PM_PEAK";
        if (hour >= 19 || hour < 5)   return "NIGHT";
        return "DAY";
    }

    /** STATS19 severity: 1 fatal, 2 serious, 3 slight. */
    public static double severityWeight(int severity) {
        switch (severity) {
            case 1:  return 10.0;
            case 2:  return 5.0;
            default: return 1.0;
        }
    }
}
```

### 11.8 `DedupScore.java` — Job 1 (window filter, de-duplicate, score)

```java
package hotspot;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input : id \t date(ISO) \t HH:MM \t lat \t lon \t severity \t casualties
 * Output: cell|band \t score \t lat \t lon \t severity      (one line per unique collision)
 */
public class DedupScore {

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private LocalDate asof;
        private int window;
        private double cell, halfLife;
        private final Text k = new Text(), v = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration c = ctx.getConfiguration();
            asof = LocalDate.parse(c.get(Hs.ASOF));
            window = c.getInt(Hs.WINDOW, 90);
            cell = c.getDouble(Hs.CELL, 0.02);
            halfLife = c.getDouble(Hs.HALF, 30.0);
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 7) {
                ctx.getCounter("hotspot", "malformed").increment(1);
                return;
            }
            try {
                long age = ChronoUnit.DAYS.between(LocalDate.parse(f[1]), asof);
                if (age < 0 || age >= window) {
                    ctx.getCounter("hotspot", "outside_window").increment(1);
                    return;
                }
                int hour = Integer.parseInt(f[2].substring(0, 2));
                double lat = Double.parseDouble(f[3]);
                double lon = Double.parseDouble(f[4]);
                int sev = Integer.parseInt(f[5]);

                long li = (long) Math.floor(lat / cell);
                long oi = (long) Math.floor(lon / cell);
                double score = Hs.severityWeight(sev) * Math.pow(0.5, age / halfLife);

                k.set(f[0]); // collision_index -> duplicates meet in the same reducer call
                v.set(li + ":" + oi + "|" + Hs.band(hour) + "\t" + score + "\t" + lat + "\t" + lon + "\t" + sev);
                ctx.write(k, v);
            } catch (RuntimeException e) { // bad date / number / time
                ctx.getCounter("hotspot", "malformed").increment(1);
            }
        }
    }

    public static class R extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text id, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            boolean first = true;
            for (Text t : vals) {
                if (first) {
                    ctx.write(NullWritable.get(), t);
                    first = false;
                } else {
                    ctx.getCounter("hotspot", "duplicates").increment(1);
                }
            }
        }
    }
}
```

### 11.9 `Aggregate.java` — Job 2 (per-cluster totals, with combiner)

```java
package hotspot;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input : cell|band \t score \t lat \t lon \t severity
 * Output: cell|band \t score \t count \t centLat \t centLon \t fatal \t serious \t slight
 * Intermediate value format: score,count,sumLat,sumLon,fatal,serious,slight
 */
public class Aggregate {

    static String fmt(double s, long n, double la, double lo, long f, long se, long sl) {
        return s + "," + n + "," + la + "," + lo + "," + f + "," + se + "," + sl;
    }

    static final class Sum {
        double score, lat, lon;
        long n, fatal, serious, slight;

        void add(String v) {
            String[] p = v.split(",");
            score += Double.parseDouble(p[0]);
            n += Long.parseLong(p[1]);
            lat += Double.parseDouble(p[2]);
            lon += Double.parseDouble(p[3]);
            fatal += Long.parseLong(p[4]);
            serious += Long.parseLong(p[5]);
            slight += Long.parseLong(p[6]);
        }
    }

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text(), v = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 5) return;
            int sev = Integer.parseInt(f[4]);
            k.set(f[0]);
            v.set(fmt(Double.parseDouble(f[1]), 1, Double.parseDouble(f[2]), Double.parseDouble(f[3]),
                    sev == 1 ? 1 : 0, sev == 2 ? 1 : 0, sev >= 3 ? 1 : 0));
            ctx.write(k, v);
        }
    }

    public static class C extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Sum s = new Sum();
            for (Text t : vals) s.add(t.toString());
            ctx.write(key, new Text(fmt(s.score, s.n, s.lat, s.lon, s.fatal, s.serious, s.slight)));
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Sum s = new Sum();
            for (Text t : vals) s.add(t.toString());
            ctx.write(key, new Text(String.format(Locale.ROOT, "%.4f\t%d\t%.6f\t%.6f\t%d\t%d\t%d",
                    s.score, s.n, s.lat / s.n, s.lon / s.n, s.fatal, s.serious, s.slight)));
        }
    }
}
```

### 11.10 `TopN.java` — Job 3 (global top 20)

Each mapper keeps only its local top *N* in a min-heap and emits it in `cleanup()`; the single reducer merges them. Shuffle volume is at most *N × #mappers* rows.

```java
package hotspot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** Input line: cell|band \t score \t ...   Output: rank \t <input line> */
public class TopN {

    static final class Rec implements Comparable<Rec> {
        final double score;
        final String line;

        Rec(String line) {
            this.line = line;
            this.score = Double.parseDouble(line.split("\t")[1]);
        }

        @Override
        public int compareTo(Rec o) { // score asc, then line asc => deterministic ties
            int c = Double.compare(score, o.score);
            return c != 0 ? c : line.compareTo(o.line);
        }
    }

    static void offer(PriorityQueue<Rec> pq, Rec r, int n) {
        pq.add(r);
        if (pq.size() > n) pq.poll(); // drop the smallest
    }

    public static class M extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final PriorityQueue<Rec> pq = new PriorityQueue<>();
        private int n;

        @Override
        protected void setup(Context ctx) { n = ctx.getConfiguration().getInt(Hs.TOPN, 20); }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) {
            offer(pq, new Rec(line.toString()), n);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Rec r : pq) ctx.write(NullWritable.get(), new Text(r.line));
        }
    }

    public static class R extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            int n = ctx.getConfiguration().getInt(Hs.TOPN, 20);
            PriorityQueue<Rec> pq = new PriorityQueue<>();
            for (Text t : vals) offer(pq, new Rec(t.toString()), n);
            List<Rec> sorted = new ArrayList<>(pq);
            Collections.sort(sorted, Collections.reverseOrder());
            int rank = 1;
            for (Rec r : sorted) {
                ctx.write(NullWritable.get(), new Text(rank++ + "\t" + r.line));
            }
        }
    }
}
```

### 11.11 `HotspotDriver.java` — chains the three jobs for one `asof` date

```java
package hotspot;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Usage: hadoop jar hotspot-mr-1.0.jar hotspot.HotspotDriver <asof yyyy-MM-dd> <baseDir> [windowDays=90] [topN=20]
 * Example: ... HotspotDriver 2025-12-31 /data/stats19 90 20
 */
public class HotspotDriver {

    public static void main(String[] a) throws Exception {
        if (a.length < 2) {
            System.err.println("usage: HotspotDriver <asof yyyy-MM-dd> <baseDir> [windowDays] [topN]");
            System.exit(1);
        }
        LocalDate asof = LocalDate.parse(a[0]);
        String base = a[1];
        int window = a.length > 2 ? Integer.parseInt(a[2]) : 90;
        int topN = a.length > 3 ? Integer.parseInt(a[3]) : 20;

        Configuration conf = new Configuration();
        conf.set(Hs.ASOF, asof.toString());
        conf.setInt(Hs.WINDOW, window);
        conf.setInt(Hs.TOPN, topN);
        FileSystem fs = FileSystem.get(conf);

        // 1. Rolling window = the `window` most recent dt= folders ending at asof
        List<Path> inputs = new ArrayList<>();
        for (int i = 0; i < window; i++) {
            Path p = new Path(base + "/raw/dt=" + asof.minusDays(i));
            if (fs.exists(p)) inputs.add(p);
        }
        if (inputs.isEmpty()) {
            System.err.println("No input folders found for window ending " + asof);
            System.exit(2);
        }
        if (inputs.size() < window) {
            System.err.println("WARN: only " + inputs.size() + "/" + window + " day-folders present");
        }

        Path tmp = new Path(base + "/tmp/asof=" + asof);
        Path out = new Path(base + "/output/asof=" + asof);
        fs.delete(tmp, true); // idempotent re-run
        fs.delete(out, true);
        Path scored = new Path(tmp, "scored");
        Path cells = new Path(out, "cells");
        Path top = new Path(out, "top20");

        // Job 1: filter window, de-duplicate, score
        Job j1 = Job.getInstance(conf, "hotspot-1-dedup-score-" + asof);
        j1.setJarByClass(HotspotDriver.class);
        j1.setMapperClass(DedupScore.M.class);
        j1.setReducerClass(DedupScore.R.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(NullWritable.class);
        j1.setOutputValueClass(Text.class);
        j1.setNumReduceTasks(4);
        FileInputFormat.setInputPaths(j1, inputs.toArray(new Path[0]));
        FileOutputFormat.setOutputPath(j1, scored);
        if (!j1.waitForCompletion(true)) System.exit(3);

        // Job 2: aggregate by (cell, band)
        Job j2 = Job.getInstance(conf, "hotspot-2-aggregate-" + asof);
        j2.setJarByClass(HotspotDriver.class);
        j2.setMapperClass(Aggregate.M.class);
        j2.setCombinerClass(Aggregate.C.class);
        j2.setReducerClass(Aggregate.R.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        j2.setNumReduceTasks(4);
        FileInputFormat.setInputPaths(j2, scored);
        FileOutputFormat.setOutputPath(j2, cells);
        if (!j2.waitForCompletion(true)) System.exit(4);

        // Job 3: global top-N
        Job j3 = Job.getInstance(conf, "hotspot-3-topn-" + asof);
        j3.setJarByClass(HotspotDriver.class);
        j3.setMapperClass(TopN.M.class);
        j3.setReducerClass(TopN.R.class);
        j3.setOutputKeyClass(NullWritable.class);
        j3.setOutputValueClass(Text.class);
        j3.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(j3, cells);
        FileOutputFormat.setOutputPath(j3, top);
        boolean ok = j3.waitForCompletion(true);

        fs.delete(tmp, true);
        System.exit(ok ? 0 : 5);
    }
}
```

### 11.12 `heatmap/heatmap.py` — geospatial heatmap

```python
#!/usr/bin/env python3
"""Build the hotspot heatmap for one as-of date from the MapReduce output on HDFS."""
import argparse
import csv
import os
import shutil

import folium
from folium.plugins import HeatMap
from hdfs import InsecureClient

WEBHDFS = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
BASE = os.getenv("HDFS_BASE", "/data/stats19")
BANDS = ["AM_PEAK", "DAY", "PM_PEAK", "NIGHT"]
BAND_LABEL = {"AM_PEAK": "Morning peak 07-10", "DAY": "Daytime 05-07 & 10-16",
              "PM_PEAK": "Evening peak 16-19", "NIGHT": "Night 19-05"}


def read_dir(client, path):
    rows = []
    for name in client.list(path):
        if name.startswith(("_", ".")):
            continue
        with client.read(f"{path}/{name}", encoding="utf-8") as r:
            rows += [ln.split("\t") for ln in r.read().splitlines() if ln.strip()]
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", required=True)
    ap.add_argument("--site", default="site")
    args = ap.parse_args()

    client = InsecureClient(WEBHDFS, user=HDFS_USER)
    out = f"{BASE}/output/asof={args.asof}"

    # cells rows: key, score, count, lat, lon, fatal, serious, slight
    cells = [{"band": r[0].split("|")[1], "score": float(r[1]), "lat": float(r[3]), "lon": float(r[4])}
             for r in read_dir(client, f"{out}/cells")]
    # top rows: rank + the 8 fields above
    top = [{"rank": int(r[0]), "cell": r[1], "band": r[1].split("|")[1], "score": float(r[2]),
            "n": int(r[3]), "lat": float(r[4]), "lon": float(r[5]),
            "fatal": int(r[6]), "serious": int(r[7]), "slight": int(r[8])}
           for r in read_dir(client, f"{out}/top20")]
    top.sort(key=lambda t: t["rank"])
    if not cells or not top:
        raise SystemExit("no MapReduce output found for " + args.asof)

    vmax = max(c["score"] for c in cells)
    center = [top[0]["lat"], top[0]["lon"]]
    m = folium.Map(location=[54.5, -3.0], zoom_start=6, tiles="CartoDB positron")

    def heat(name, subset, show):
        HeatMap([[c["lat"], c["lon"], c["score"] / vmax] for c in subset],
                name=name, radius=18, blur=22, min_opacity=0.25, show=show).add_to(m)

    heat("All time bands", cells, True)
    for b in BANDS:
        heat(BAND_LABEL[b], [c for c in cells if c["band"] == b], False)

    fg = folium.FeatureGroup(name="Top 20 hotspots", show=True)
    for t in top:
        popup = (f"<b>#{t['rank']}</b> {BAND_LABEL[t['band']]}<br>"
                 f"score {t['score']:.1f} &middot; {t['n']} collisions<br>"
                 f"fatal {t['fatal']} / serious {t['serious']} / slight {t['slight']}")
        folium.Marker(
            [t["lat"], t["lon"]], popup=folium.Popup(popup, max_width=260),
            tooltip=f"#{t['rank']} {t['band']}",
            icon=folium.DivIcon(html=(
                "<div style='background:#c0392b;color:#fff;border-radius:50%;width:24px;height:24px;"
                "line-height:24px;text-align:center;font:bold 12px sans-serif;border:2px solid #fff'>"
                f"{t['rank']}</div>")),
        ).add_to(fg)
    fg.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    m.get_root().html.add_child(folium.Element(
        f"<div style='position:fixed;top:10px;left:60px;z-index:9999;background:#fff;padding:6px 12px;"
        f"border-radius:6px;font:14px sans-serif;box-shadow:0 1px 4px #0005'>"
        f"<b>Accident hotspots</b> &mdash; 90 days to {args.asof}</div>"))

    os.makedirs(args.site, exist_ok=True)
    dated = f"{args.site}/heatmap_{args.asof}.html"
    m.save(dated)
    shutil.copyfile(dated, f"{args.site}/index.html")

    with open(f"{args.site}/top20.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "cell", "band", "score", "collisions", "lat", "lon", "fatal", "serious", "slight"])
        for t in top:
            w.writerow([t["rank"], t["cell"].split("|")[0], t["band"], f"{t['score']:.4f}", t["n"],
                        t["lat"], t["lon"], t["fatal"], t["serious"], t["slight"]])
    print("wrote", dated, "and", f"{args.site}/top20.csv", "center", center)


if __name__ == "__main__":
    main()
```

### 11.13 `validate/validate_reference.py` — proves correctness

Independent pandas implementation of the same definition; run it against the CSV and compare with `site/top20.csv`.

```python
#!/usr/bin/env python3
"""Compare MapReduce top-20 (site/top20.csv) with a pandas reference for the same as-of date."""
import argparse
import math

import numpy as np
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--asof", required=True)
ap.add_argument("--mr", default="site/top20.csv")
ap.add_argument("--cell", type=float, default=0.02)
ap.add_argument("--half", type=float, default=30.0)
ap.add_argument("--window", type=int, default=90)
a = ap.parse_args()

df = pd.read_csv(a.csv, usecols=["collision_index", "date", "time", "latitude", "longitude",
                                 "collision_severity"], low_memory=False)
df = df.dropna(subset=["latitude", "longitude"]).drop_duplicates("collision_index")
df["d"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
asof = pd.Timestamp(a.asof)
df["age"] = (asof - df["d"]).dt.days
df = df[(df.age >= 0) & (df.age < a.window)].copy()

h = df["time"].str[:2].astype(int)
df["band"] = np.select([h.between(7, 9), h.between(16, 18), (h >= 19) | (h < 5)],
                       ["AM_PEAK", "PM_PEAK", "NIGHT"], "DAY")
df["li"] = np.floor(df.latitude / a.cell).astype(int)
df["oi"] = np.floor(df.longitude / a.cell).astype(int)
df["cell"] = df.li.astype(str) + ":" + df.oi.astype(str)
df["score"] = df.collision_severity.map({1: 10.0, 2: 5.0}).fillna(1.0) * np.power(0.5, df.age / a.half)

ref = (df.groupby(["cell", "band"]).agg(score=("score", "sum"), n=("score", "size"))
         .reset_index().sort_values(["score", "cell", "band"], ascending=[False, False, False]).head(20))
mr = pd.read_csv(a.mr, dtype={"cell": str})

ok = True
for i, (r, m) in enumerate(zip(ref.itertuples(), mr.itertuples()), 1):
    same = (r.cell == m.cell and r.band == m.band and math.isclose(r.score, m.score, abs_tol=1e-3)
            and r.n == m.collisions)
    ok &= same
    print(f"#{i:2d} ref {r.cell:>10s} {r.band:8s} {r.score:8.3f} n={r.n:3d} | "
          f"mr {m.cell:>10s} {m.band:8s} {m.score:8.3f} n={m.collisions:3d} | {'OK' if same else 'DIFF'}")
print("PASS" if ok and len(ref) == len(mr) else "FAIL (check ties at rank 20 before assuming a bug)")
```

### 11.14 Scripts

`scripts/create_topic.sh`
```bash
#!/usr/bin/env bash
set -euo pipefail
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic accident-reports --partitions 6 --replication-factor 1
docker compose exec namenode hdfs dfs -mkdir -p /data/stats19/raw /data/stats19/output /data/stats19/tmp
```

`scripts/run_daily.sh` — the daily job (MapReduce → heatmap)
```bash
#!/usr/bin/env bash
# usage: run_daily.sh [asof yyyy-mm-dd]   (default: yesterday)
set -euo pipefail
cd "$(dirname "$0")/.."
ASOF="${1:-$(date -d 'yesterday' +%F)}"

docker compose exec -T resourcemanager \
  hadoop jar /jobs/hotspot-mr-1.0.jar hotspot.HotspotDriver "$ASOF" /data/stats19 90 20
docker compose exec -T app python heatmap/heatmap.py --asof "$ASOF"
echo "hotspot report for $ASOF ready: site/index.html"
```

Cron entry (host, 02:00 every day):
```
0 2 * * * /path/to/road-hotspot/scripts/run_daily.sh >> /var/log/hotspot.log 2>&1
```

`scripts/simulate.sh` — replay the dataset as a live daily feed
```bash
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
  d=$(date -I -d "$d + 1 day")
done
```

---

## 12. Run Guide

```bash
# 0. Put the CSV in ./data/ and build the MapReduce jar
mvn -q -f mapreduce/pom.xml clean package

# 1. Start the platform (wait ~30 s for HDFS to leave safe mode)
docker compose up -d
./scripts/create_topic.sh

# 2. Start the Kafka -> HDFS sink (keeps running)
docker compose exec -d app python ingest/hdfs_sink.py

# 3. Backfill the 90 days before the first report date
docker compose exec app python ingest/producer.py \
   --csv data/dft-road-casualty-statistics-collision-last-5-years.csv \
   --from 2025-07-05 --to 2025-10-02

# 4. Run one report (as-of 2025-12-31) after also replaying 2025-10-03..2025-12-31
docker compose exec app python ingest/producer.py \
   --csv data/dft-road-casualty-statistics-collision-last-5-years.csv \
   --from 2025-10-03 --to 2025-12-31
./scripts/run_daily.sh 2025-12-31

# 5. View the map
docker compose exec -d app python -m http.server 8000 --directory site
#    open http://localhost:8000   (HDFS UI :9870, YARN UI :8088)

# 6. Validate against the pandas reference
docker compose exec app python validate/validate_reference.py \
   --csv data/dft-road-casualty-statistics-collision-last-5-years.csv --asof 2025-12-31
```

To demo the **daily rolling behaviour**, run `./scripts/simulate.sh 2025-10-03 2025-12-31` and watch the ranking shift as new days enter and old days leave the window.

## 13. Test Plan

| # | Test | Expected |
|---|---|---|
| T1 | `Hs.band()` for hours 4, 5, 7, 9, 10, 16, 18, 19 | NIGHT, DAY, AM_PEAK, AM_PEAK, DAY, PM_PEAK, PM_PEAK, NIGHT |
| T2 | Producer on the full CSV | 53 rows skipped (missing coordinates), rest published |
| T3 | Replay the same day twice | Job 1 `duplicates` counter equals that day's count; top-20 unchanged |
| T4 | Kill the sink between write and commit, restart | No lost days; duplicates only, removed by Job 1 |
| T5 | Window boundary: as-of 2025-12-31 | 2025-10-03 included (age 89), 2025-10-02 excluded (age 90); ~25.6k input records |
| T6 | Re-run the same `asof` | Output directory replaced, identical top-20 |
| T7 | `validate_reference.py` | PASS |
| T8 | Missing day-folders in the window | Driver warns and still produces a result |
| T9 | Heatmap | Layer toggles work; 20 numbered markers; popups show the severity mix |

## 14. Success Metrics

| Metric | Target |
|---|---|
| Report freshness | ≤ 24 h behind the latest collision date |
| MapReduce vs reference agreement | 100% of top-20 clusters match |
| Daily pipeline runtime | < 10 min on the dev cluster |
| Data loss on sink crash | 0 records |
| Planner usability | Top hotspot identifiable within 10 s of opening the map |

## 15. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Fine clusters are sparse (see §6) | One fatal crash dominates the top-20 | 2 km cells and 4 time bands; the severity mix is shown in popups; the weights are configurable |
| Grid cells ≠ real roads | A cell may straddle several roads | Documented limitation; v2 uses road-segment snapping or H3 |
| Reporting lag in STATS19 | The last days of a window can be incomplete in real deployments | Recency decay is a trade-off; show the window end date on the map |
| Duplicate events after retries | Inflated scores | Kafka `acks=all`, deterministic file names, reducer de-duplication |
| Small-file problem on HDFS | Many small TSVs slow MapReduce | Sink batches by count/time; optional weekly compaction job |
| Docker image/config drift | Cluster won't start | Pin image versions; `hadoop.env` is the single config source |
| Single reducer in Job 3 | Bottleneck | Safe: each mapper emits ≤ 20 rows |

## 16. Milestones

| Week | Deliverable |
|---|---|
| 1 | Docker platform up; producer + sink; data visible on HDFS |
| 2 | MapReduce Jobs 1–3 + driver; the validator passes |
| 3 | Heatmap, daily script/cron, simulation of a rolling window |
| 4 | Test plan executed, report + demo (screenshots of HDFS UI, YARN UI, map) |

## 17. Future Work

- Replace the fixed grid with **DBSCAN/H3** clusters or road-segment snapping.
- Add weather/light-condition/speed-limit features to explain *why* a hotspot forms.
- Move Job 1 filtering to Kafka Streams for near-real-time updates; keep MapReduce for the audited daily ranking.
- Schedule with Airflow; alert when a hotspot's rank jumps by more than 5 places.
- Add road-safety KPIs (casualties per 100k vehicle-km) if traffic-count data is available.

## 18. Glossary

| Term | Meaning |
|---|---|
| STATS19 | Great Britain police-reported road collision dataset published by the Department for Transport |
| Hotspot | A (grid cell, time band) cluster with a high recency- and severity-weighted collision score |
| As-of date | The last day included in the rolling window |
| Rolling window | The 90 days ending on the as-of date |
