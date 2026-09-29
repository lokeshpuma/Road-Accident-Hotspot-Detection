# Road Accident Hotspot Detection 🚦📍

[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Live%20Demo-brightgreen?logo=github)](https://lokeshpuma.github.io/Road-Accident-Hotspot-Detection/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.6-yellow?logo=apachehadoop)](https://hadoop.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-black?logo=apachekafka)](https://kafka.apache.org/)
[![Java](https://img.shields.io/badge/Java-8%20%2F%20Maven-red?logo=openjdk)](https://www.java.com/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![Folium](https://img.shields.io/badge/Leaflet%20%2F%20Folium-Heatmap-green)](https://python-visualization.github.io/folium/)

An end-to-end Big Data streaming and batch processing pipeline that ingests, deduplicates, scores, aggregates, and visualizes road collision hotspots using **Apache Kafka**, **Hadoop HDFS**, **YARN MapReduce**, and an interactive **Leaflet/Folium geospatial heatmap**.

🔗 **Live Interactive Heatmap**: [https://lokeshpuma.github.io/Road-Accident-Hotspot-Detection/](https://lokeshpuma.github.io/Road-Accident-Hotspot-Detection/)

---

## 📌 1. Overview & Problem Statement

Traffic safety authorities traditionally allocate enforcement and preventative patrols based on **annual reports**, which are months out of date and hide *when* and *where* risk is concentrated.

This project delivers a production-grade architecture that:
1. **Ingests** collision reports in near real-time as JSON events into **Kafka**.
2. **Persists** events onto **HDFS** partitioned by event date (`/data/stats19/raw/dt=YYYY-MM-DD/`) with atomic rename commits.
3. **Executes** a daily 3-stage **Hadoop MapReduce** chain on **YARN** ranking the **Top 20 accident hotspots** over a **rolling 90-day window**.
4. **Visualizes** hotspots on an interactive **geospatial heatmap** featuring time-band toggle layers and ranked markers detailing collision severity distributions.
5. **Validates** cluster accuracy and scoring against an independent pandas baseline.

---

## 🏗️ 2. Architecture

```text
                 ┌──────────────┐   JSON events   ┌────────────────────┐
 STATS19 CSV ───►│ producer.py  │────────────────►│ Kafka topic         │
 (replay by day) │ (Python)     │ key=collision_id│ accident-reports    │
                 └──────────────┘                 │ 6 partitions        │
                                                  └─────────┬──────────┘
                                                            │ consumer group "hdfs-sink"
                                                            ▼
                                                  ┌────────────────────┐
                                                  │ hdfs_sink.py       │ commit offsets after write
                                                  └─────────┬──────────┘
                                                            ▼  WebHDFS
   HDFS  /data/stats19/raw/dt=2025-12-31/p3-o…-….tsv   (partitioned by event date)
                                                            │ rolling 90 dt= folders
                                                            ▼
   ┌──────────────────────── MapReduce on YARN (daily, asof = D) ───────────────────────┐
   │ Job 1  DedupScore : filter window → key=collision_id → 1 record → cell|band + score │
   │ Job 2  Aggregate  : key=cell|band (+combiner) → score, n, centroid, severity mix    │
   │ Job 3  TopN       : per-mapper min-heap top-20 → 1 reducer → global top-20          │
   └──────────────────────────────────────────┬─────────────────────────────────────────┘
                                              ▼
   HDFS /data/stats19/output/asof=D/{cells,top20}
                                              ▼
                              heatmap.py (Folium HeatMap + ranked markers)
                                              ▼
                         site/index.html  +  site/top20.csv   (GitHub Pages)
```

---

## 📊 3. Dataset & Hotspot Methodology

The project analyzes the official **UK Road Safety (STATS19)** collision dataset spanning 2021–2025 (513,801 total collisions).
[DATASET LINK](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data)

### Spatial & Temporal Clustering
- **Spatial Grid Resolution**: $0.02^\circ \times 0.02^\circ$ latitude/longitude cells ($\approx 2.2\text{ km} \times 1.3\text{ km}$).
- **Diurnal Time Bands**:
  - `AM_PEAK`: 07:00 – 09:59
  - `DAY`: 05:00 – 06:59 & 10:00 – 15:59
  - `PM_PEAK`: 16:00 – 18:59
  - `NIGHT`: 19:00 – 04:59

### Recency & Severity Scoring Model
Each collision event receives an exponential recency decay score weighted by collision injury severity:

$$\text{score} = \text{severity\_weight} \times 0.5^{\frac{\text{age\_days}}{\text{half\_life}}}$$

- **Severity Weights**: Fatal (`1`) = 10, Serious (`2`) = 5, Slight (`3`) = 1
- **Half-Life ($\lambda$)**: 30 days
- **Rolling Window**: 90 days ending on `asof` date ($0 \le \text{age} < 90$)

---

## 🏆 4. Top 20 Hotspots (As of 2025-12-31)

| Rank | Grid Cell | Time Band | Score | Collisions | Centroid Lat | Centroid Lon | Fatal | Serious | Slight |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **1** | `2575:-7` | `DAY` | 24.8265 | 24 | 51.510653 | -0.130075 | 1 | 8 | 15 |
| **2** | `2575:-8` | `DAY` | 22.1397 | 19 | 51.511545 | -0.150444 | 0 | 7 | 12 |
| **3** | `2575:-8` | `NIGHT` | 19.6665 | 24 | 51.509444 | -0.147490 | 0 | 5 | 19 |
| **4** | `2576:-6` | `NIGHT` | 18.8486 | 13 | 51.528532 | -0.109176 | 0 | 6 | 7 |
| **5** | `2573:-2` | `DAY` | 16.8453 | 16 | 51.471811 | -0.030681 | 0 | 4 | 12 |
| **6** | `2575:-7` | `NIGHT` | 16.5332 | 29 | 51.511850 | -0.129820 | 0 | 3 | 26 |
| **7** | `2574:-4` | `DAY` | 15.8611 | 10 | 51.488555 | -0.071392 | 1 | 5 | 4 |
| **8** | `2575:0` | `NIGHT` | 15.3573 | 12 | 51.512154 | 0.008854 | 0 | 2 | 10 |
| **9** | `2575:-4` | `NIGHT` | 15.2716 | 12 | 51.510260 | -0.070678 | 0 | 4 | 8 |
| **10** | `2673:-112` | `NIGHT` | 15.2592 | 10 | 53.471561 | -2.231335 | 1 | 4 | 5 |
| **11** | `2574:-9` | `DAY` | 15.0556 | 13 | 51.492270 | -0.167772 | 0 | 6 | 7 |
| **12** | `2673:-113` | `NIGHT` | 14.5150 | 10 | 53.472863 | -2.251792 | 1 | 2 | 7 |
| **13** | `2576:-5` | `NIGHT` | 14.2357 | 18 | 51.528593 | -0.086710 | 0 | 4 | 14 |
| **14** | `2648:-59` | `PM_PEAK` | 14.0712 | 6 | 52.967287 | -1.169418 | 0 | 4 | 2 |
| **15** | `2674:-113` | `DAY` | 13.7540 | 6 | 53.491070 | -2.249950 | 0 | 4 | 2 |
| **16** | `2569:-6` | `PM_PEAK` | 13.5113 | 13 | 51.389290 | -0.107832 | 0 | 3 | 10 |
| **17** | `2575:-3` | `NIGHT` | 13.4816 | 19 | 51.511271 | -0.053421 | 0 | 4 | 15 |
| **18** | `2576:-4` | `NIGHT` | 13.2063 | 16 | 51.527135 | -0.072613 | 0 | 4 | 12 |
| **19** | `2541:-7` | `DAY` | 13.0193 | 15 | 50.829393 | -0.132838 | 0 | 4 | 11 |
| **20** | `2687:-18` | `DAY` | 12.9812 | 13 | 53.748345 | -0.351138 | 0 | 3 | 10 |

---

## 📁 5. Repository Layout

```text
.
├── PRD.md                       # Comprehensive Product Requirements Document
├── README.md                    # Project documentation and guide
├── docker-compose.yml           # Single-node Kafka, HDFS & YARN setup
├── hadoop.env                   # Hadoop & YARN environment variables
├── requirements.txt             # Python libraries
├── .github/workflows/
│   └── deploy-pages.yml         # GitHub Actions workflow for GitHub Pages
├── ingest/
│   ├── producer.py              # Ingests & replays STATS19 CSV to Kafka
│   └── hdfs_sink.py             # Consumes Kafka & writes partitioned HDFS TSVs
├── mapreduce/
│   ├── pom.xml                  # Maven dependencies & build packaging
│   └── src/
│       ├── main/java/hotspot/
│       │   ├── Hs.java          # Shared pure helpers and configuration
│       │   ├── DedupScore.java  # Job 1: Window filter, dedup & scoring
│       │   ├── Aggregate.java   # Job 2: Cluster aggregation & combiner
│       │   ├── TopN.java        # Job 3: Global Top-20 ranking
│       │   └── HotspotDriver.java # 3-stage MapReduce orchestrator
│       └── test/java/hotspot/
│           └── HsTest.java      # Unit tests
├── heatmap/
│   └── heatmap.py               # Generates Folium heatmap and top20.csv
├── validate/
│   └── validate_reference.py    # Independent pandas verification script
├── scripts/
│   ├── create_topic.sh          # Sets up Kafka topic & HDFS paths
│   ├── run_daily.sh             # Executes daily MapReduce and builds map
│   └── simulate.sh              # Day-by-day continuous replay simulation
└── site/
    ├── index.html               # Main interactive heatmap application
    ├── heatmap_2025-12-31.html  # Dated heatmap report
    └── top20.csv                # Top 20 ranked hotspots CSV
```

---

## 🚀 6. Getting Started

### Prerequisites
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- [Maven](https://maven.apache.org/) and JDK 8+ (to package MapReduce JAR)
- Python 3.10+ (if running scripts directly on host)

### 1. Build the MapReduce JAR
```bash
mvn -f mapreduce/pom.xml clean package
```

### 2. Start the Cluster
```bash
docker compose up -d
./scripts/create_topic.sh
```

### 3. Start the Kafka-to-HDFS Sink
```bash
docker compose exec -d app python ingest/hdfs_sink.py
```

### 4. Replay Collision Data to Kafka
```bash
# Replay 90-day window ending on 2025-12-31:
docker compose exec app python ingest/producer.py \
   --csv data/dft-road-casualty-statistics-collision-last-5-years.csv \
   --from 2025-10-03 --to 2025-12-31
```

### 5. Run Daily MapReduce Ranking & Build Heatmap
```bash
./scripts/run_daily.sh 2025-12-31
```

### 6. View the Heatmap
Open `http://localhost:8000/` or inspect [`site/index.html`](file:///Users/lokesh/Desktop/bda%20road/site/index.html).

### 7. Run Verification Test
```bash
docker compose exec app python validate/validate_reference.py \
   --csv data/dft-road-casualty-statistics-collision-last-5-years.csv \
   --asof 2025-12-31
```
Output:
```text
# 1 ref    2575:-7 DAY        24.827 n= 24 | mr    2575:-7 DAY        24.826 n= 24 | OK
# 2 ref    2575:-8 DAY        22.140 n= 19 | mr    2575:-8 DAY        22.140 n= 19 | OK
...
#20 ref   2687:-18 DAY        12.981 n= 13 | mr   2687:-18 DAY        12.981 n= 13 | OK
PASS
```

---

## 🌐 7. Deploying to GitHub Pages

This repository is pre-configured for instant GitHub Pages deployment via **GitHub Actions**:
1. Push to the `main` branch.
2. In your GitHub repository:
   - Navigate to **Settings** > **Pages**.
   - Under **Build and deployment** > **Source**, select **GitHub Actions** (or select **Deploy from a branch** and choose `gh-pages`).
3. The interactive map will be live at:
   `https://<username>.github.io/<repo>/`

---

## 📜 License
This project uses road safety data under the **UK Open Government Licence (OGL v3.0)** published by the Department for Transport.
