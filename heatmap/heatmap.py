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
    m = folium.Map(location=[54.5, -3.0], zoom_start=6, tiles=None)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ",
        name="Light Gray Canvas",
        control=True,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Source: Esri, DeLorme, NAVTEQ, USGS",
        name="World Street Map",
        control=True,
    ).add_to(m)

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
