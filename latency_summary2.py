#!/usr/bin/env python3
# Stdlib-only latency snapshot + ASCII histogram for /home/ec2-user/pythonQF/data/latency.csv
import csv, statistics, math, argparse
from pathlib import Path

def load_latencies(csv_path: Path):
    lat = []
    if not csv_path.exists():
        return lat
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            v = row.get("latency_ms")
            if v is None: 
                continue
            try:
                lat.append(float(v))
            except ValueError:
                pass
    return lat

def percentile(sorted_vals, p):
    # p in [0,1]; inclusive interpolation by index
    n = len(sorted_vals)
    if n == 0: return float("nan")
    idx = int(p * (n - 1))
    return sorted_vals[idx]

def make_hist(lat, bin_ms: float, max_ms: float):
    """
    Returns (bins_edges, counts, overflow)
      bins_edges: list of (lo, hi) inclusive-exclusive [lo, hi)
      counts: same length as bins_edges
      overflow: count >= max_ms
    """
    n_bins = max(1, int(math.ceil(max_ms / bin_ms)))
    edges = [(i*bin_ms, (i+1)*bin_ms) for i in range(n_bins)]
    counts = [0]*n_bins
    overflow = 0
    for x in lat:
        if x < 0:
            # ignore negatives, but if needed could put in first bucket
            continue
        if x >= max_ms:
            overflow += 1
        else:
            idx = int(x // bin_ms)  # floor
            # guard in case x == max_ms due to float quirks
            if idx >= n_bins: 
                idx = n_bins - 1
            counts[idx] += 1
    return edges, counts, overflow

def render_hist(edges, counts, overflow, width=40):
    total = sum(counts) + overflow
    if total == 0:
        return "No histogram to display (no samples)."
    peak = max(counts) if counts else 0
    scale = (width / peak) if peak > 0 else 1.0
    lines = []
    for (lo, hi), c in zip(edges, counts):
        bar = "#" * int(round(c * scale))
        label = f"{lo:.2f}-{hi:.2f} ms"
        lines.append(f"{label:>16} | {bar} {c}")
    if overflow:
        bar = "#" * int(round(overflow * scale))
        lines.append(f"{edges[-1][1]:>6.2f}ms+      | {bar} {overflow}")
    return "\n".join(lines)

def main():
    p = argparse.ArgumentParser(description="Latency snapshot + histogram from data/latency.csv")
    p.add_argument("--csv", default="/home/ec2-user/pythonQF/data/latency.csv", help="Path to latency.csv")
    p.add_argument("--bin-ms", type=float, default=1.0, help="Histogram bin width in milliseconds (default: 1.0)")
    p.add_argument("--max-ms", type=float, default=20.0, help="Max range for histogram (values >= go to overflow)")
    p.add_argument("--width", type=int, default=40, help="Max bar width (characters) for histogram")
    args = p.parse_args()

    csv_path = Path(args.csv)
    lat = load_latencies(csv_path)
    if not lat:
        print(f"No latency data found at {csv_path}")
        return

    lat.sort()
    n   = len(lat)
    mean= statistics.fmean(lat)
    p50 = percentile(lat, 0.50)
    p90 = percentile(lat, 0.90)
    p99 = percentile(lat, 0.99)
    mx  = lat[-1]

    print(f"n={n}  mean={mean:.3f}ms  p50={p50:.3f}ms  p90={p90:.3f}ms  p99={p99:.3f}ms  max={mx:.3f}ms")

    # Choose a good default max_ms if the data is much larger than the default
    max_ms = args.max_ms
    if mx > max_ms * 1.5:
        # Expand to next round bucket above max value (nice step)
        step = args.bin_ms
        max_ms = math.ceil(mx / step) * step

    edges, counts, overflow = make_hist(lat, args.bin_ms, max_ms)
    print("\nHistogram:")
    print(render_hist(edges, counts, overflow, width=args.width))

if __name__ == "__main__":
    main()