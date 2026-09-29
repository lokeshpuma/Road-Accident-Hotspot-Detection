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
