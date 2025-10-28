#!/usr/bin/env python3
import csv, statistics
from pathlib import Path

csv_path = Path("/home/ec2-user/pythonQF/data/latency.csv")
lat = []

with csv_path.open() as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            lat.append(float(row["latency_ms"]))
        except (KeyError, ValueError):
            pass

if not lat:
    print("No latency data found.")
else:
    lat.sort()
    n = len(lat)
    def pct(p): return lat[int(p*(n-1))]
    print(f"n={n}  mean={statistics.fmean(lat):.3f}ms  "
          f"p50={pct(0.50):.3f}ms  p90={pct(0.90):.3f}ms  "
          f"p99={pct(0.99):.3f}ms  max={lat[-1]:.3f}ms")