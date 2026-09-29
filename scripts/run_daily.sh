#!/usr/bin/env bash
# usage: run_daily.sh [asof yyyy-mm-dd]   (default: yesterday)
set -euo pipefail
cd "$(dirname "$0")/.."
ASOF="${1:-$(date -d 'yesterday' +%F 2>/dev/null || date -v-1d +%F 2>/dev/null || python3 -c "import datetime; print(datetime.date.today() - datetime.timedelta(days=1))")}"

docker compose exec -T resourcemanager \
  hadoop jar /jobs/hotspot-mr-1.0.jar hotspot.HotspotDriver "$ASOF" /data/stats19 90 20
docker compose exec -T app python heatmap/heatmap.py --asof "$ASOF"
echo "hotspot report for $ASOF ready: site/index.html"
