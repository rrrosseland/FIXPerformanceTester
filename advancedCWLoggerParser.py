#!/usr/bin/env python3
import json, re, argparse, sys
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from pathlib import Path

# ---------- parsing helpers ----------
def split_concatenated_json(fp):
    """Yield each top-level JSON object from a file that may contain multiple concatenated JSON blobs."""
    dec = json.JSONDecoder()
    buf = fp.read()
    idx, n = 0, len(buf)
    while idx < n:
        while idx < n and buf[idx].isspace():
            idx += 1
        if idx >= n:
            break
        obj, end = dec.raw_decode(buf, idx)
        yield obj
        idx = end

def parse_outer_iso8601(val):
    # "2025-10-17T18:20:56.916694774Z" (nanoseconds ok, we keep microseconds)
    if not val:
        return None
    m = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$', val.strip())
    if not m:
        return None
    base, frac = m.groups()
    

def parse_fix_ts(val):
    # "YYYYMMDD-HH:MM:SS[.fffffffff]" (with occasional trailing space)
    if not val:
        return None
    s = val.strip()
    m = re.match(r'^(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$', s)
    if not m:
        return None
    y, M, d, h, m_, s, frac = m.groups()
    micro = int((frac + '000000')[:6]) if frac else 0
    try:
        return datetime(int(y), int(M), int(d), int(h), int(m_), int(s), micro, tzinfo=timezone.utc)
    except Exception:
        return None

def parse_fix_line(msg):
    """Parse '8=FIX.. | 9=.. | 35=.. | ...' into a dict of tags."""
    if not msg:
        return {}
    s = msg.replace('\x01', '|')
    parts = [p.strip() for p in s.split('|') if p.strip()]
    tags = {}
    for p in parts:
        if '=' in p:
            k, v = p.split('=', 1)
            tags[k.strip()] = v.strip()
    tags['_raw'] = s
    return tags

# ---------- main pipeline ----------
def build_dataframe(path: Path):
    rows = []
    with path.open('r', encoding='utf-8') as fp:
        for block in split_concatenated_json(fp):
            for evt in block.get('events', []):
                try:
                    envelope = json.loads(evt['message'])
                except Exception:
                    continue
                # Prefer log_processed.msg; fallback to the embedded 'log' JSON
                log_proc = envelope.get('log_processed') or {}
                inner_time = log_proc.get('time')
                inner_msg  = log_proc.get('msg')
                if inner_msg is None:
                    try:
                        inner = json.loads(envelope.get('log', '{}'))
                    except Exception:
                        inner = {}
                    inner_time = inner_time or inner.get('time')
                    inner_msg  = inner_msg  or inner.get('msg', '')

                tags = parse_fix_line(inner_msg)
                if not tags:
                    continue
                tags['_outer_time'] = envelope.get('time')      # container log time
                tags['_inner_time'] = inner_time                # app log time
                rows.append(tags)

    df = pd.DataFrame(rows)
    for c in ['35','49','56','11','17','52','60','_outer_time','_inner_time','_raw']:
        if c not in df.columns:
            df[c] = None

    # Normalize timestamps
    df['_outer_dt'] = df['_outer_time'].apply(parse_outer_iso8601)
    df['_inner_dt'] = df['_inner_time'].apply(parse_outer_iso8601)
    df['_52_dt']    = df['52'].apply(parse_fix_ts)
    df['_60_dt']    = df['60'].apply(parse_fix_ts)

    return df

def delta_ms(a, b):
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return np.nan
    return (b - a).total_seconds() * 1000.0

def describe(series: pd.Series):
    s = series.dropna()
    if s.empty:
        return pd.Series({'count': 0, 'mean_ms': np.nan, 'median_ms': np.nan,
                          'p90_ms': np.nan, 'p99_ms': np.nan, 'min_ms': np.nan, 'max_ms': np.nan})
    return pd.Series({
        'count':  int(s.count()),
        'mean_ms': float(s.mean()),
        'median_ms': float(s.median()),
        'p90_ms': float(s.quantile(0.90)),
        'p99_ms': float(s.quantile(0.99)),
        'min_ms': float(s.min()),
        'max_ms': float(s.max()),
    })

def main():
    ap = argparse.ArgumentParser(
        description="Pair FIX 35=D/5 (49=4C001) with 35=8 (49=ForecastEx) by 11=ClOrdID and compute latencies."
    )
    ap.add_argument("input", help="Path to CloudWatch export (e.g., fix_raw.json)")
    ap.add_argument("--pairs", default="fix_latency_pairs.csv", help="Output CSV of per-order pairs")
    ap.add_argument("--summary", default="fix_latency_summary.csv", help="Output CSV of summary stats")
    ap.add_argument(
        "--first-response-only",
        dest="first_response_only",
        action="store_true",
        help="If multiple 35=8 per ClOrdID, keep only the earliest response"
    )
    args = ap.parse_args()

    df = build_dataframe(Path(args.input))

    # Requests (client) and responses (server)
    req = df[(df['35'].isin(['D','5'])) & (df['49'] == '4C001')].copy()
    res = df[(df['35'] == '8') & (df['49'] == 'ForecastEx')].copy()

    # Optionally collapse to earliest response per ClOrdID
    if args.first_response_only:
        res = res.sort_values(['11','_52_dt','_60_dt','_outer_dt','_inner_dt'])
        res = res.groupby('11', as_index=False).first()

    # Rename columns to keep request/response separate after merge
    req = req.rename(columns={'17':'17_req','52':'52_req','60':'60_req',
                              '_52_dt':'_52_dt_req','_60_dt':'_60_dt_req',
                              '_outer_dt':'_outer_dt_req','_inner_dt':'_inner_dt_req','_raw':'_raw_req'})
    res = res.rename(columns={'17':'17_res','52':'52_res','60':'60_res',
                              '_52_dt':'_52_dt_res','_60_dt':'_60_dt_res',
                              '_outer_dt':'_outer_dt_res','_inner_dt':'_inner_dt_res','_raw':'_raw_res'})

    merged = pd.merge(req, res, on='11', how='inner', suffixes=('_req','_res'))

    # Compute latencies
    merged['lat_ms_52_to_52'] = merged.apply(lambda r: delta_ms(r['_52_dt_req'], r['_52_dt_res']), axis=1)
    merged['lat_ms_52_to_60'] = merged.apply(lambda r: delta_ms(r['_52_dt_req'], r['_60_dt_res']), axis=1)
    merged['lat_ms_outer']    = merged.apply(lambda r: delta_ms(r['_outer_dt_req'], r['_outer_dt_res']), axis=1)
    merged['lat_ms_inner']    = merged.apply(lambda r: delta_ms(r['_inner_dt_req'], r['_inner_dt_res']), axis=1)

    # Keep the most useful columns
    cols = [
        '11','49_req','35_req','56_req','52_req','60_req','_52_dt_req','_60_dt_req','_outer_dt_req','_inner_dt_req','_raw_req',
        '49_res','35_res','56_res','52_res','60_res','_52_dt_res','_60_dt_res','_outer_dt_res','_inner_dt_res','_raw_res',
        'lat_ms_52_to_52','lat_ms_52_to_60','lat_ms_outer','lat_ms_inner'
    ]
    for c in cols:
        if c not in merged.columns:
            merged[c] = np.nan

    merged = merged[cols].sort_values('_52_dt_req')
    merged.to_csv(args.pairs, index=False)

    summary = pd.DataFrame({
        'lat_ms_52_to_52': describe(merged['lat_ms_52_to_52']),
        'lat_ms_52_to_60': describe(merged['lat_ms_52_to_60']),
        'lat_ms_outer':    describe(merged['lat_ms_outer']),
        'lat_ms_inner':    describe(merged['lat_ms_inner']),
    }).T
    summary.to_csv(args.summary)

    # Print a tiny on-screen digest
    print("\n=== Summary (ms) ===")
    print(summary[['count','mean_ms','median_ms','p90_ms','p99_ms','min_ms','max_ms']].round(3).to_string())
    print(f"\nWrote pairs:   {args.pairs}")
    print(f"Wrote summary: {args.summary}")

if __name__ == "__main__":
    try:
        main()
    except ModuleNotFoundError as e:
        if "pandas" in str(e):
            print("pandas is not installed in this environment. Activate your venv and install with:\n"
                  "  python3 -m venv venv-fix && source venv-fix/bin/activate\n"
                  "  pip install pandas numpy", file=sys.stderr)
        raise