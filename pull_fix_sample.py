#!/usr/bin/env python3
import argparse, time, json, sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

def main():
    ap = argparse.ArgumentParser(description="Pull a small sample of FIX lines from CloudWatch Logs")
    ap.add_argument("--region", default="us-east-1", help="AWS region (default us-east-1)")
    ap.add_argument("--group", required=True, help="CloudWatch Log Group name")
    ap.add_argument("--minutes", type=int, default=60, help="Look back this many minutes (default 60)")
    ap.add_argument("--pattern", default="8=FIX", help="Filter pattern (default '8=FIX')")
    ap.add_argument("--max", type=int, default=200, help="Max events to save (default 200)")
    ap.add_argument("--out", default="fix_sample.jsonl", help="Output file (NDJSON)")
    args = ap.parse_args()

    # Time window (epoch millis)
    end = int(time.time() * 1000)
    start = int((datetime.now(timezone.utc) - timedelta(minutes=args.minutes)).timestamp() * 1000)
    # ... when creating the client
    #logs = boto3.client("logs", config=Config(retries={"max_attempts": 10, "mode": "standard"}))
    logs = boto3.client(
        "logs",
        region_name=args.region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"})
    )
    
    saved = 0
    next_token = None
    with open(args.out, "w", encoding="utf-8") as fout:
        while True:
            kw = {
                "logGroupName": args.group,
                "startTime": start,
                "endTime": end,
                "filterPattern": args.pattern,
                "limit": min(1000, args.max - saved),
            }
            if next_token:
                kw["nextToken"] = next_token

            resp = logs.filter_log_events(**kw)
            events = resp.get("events", [])
            for e in events:
                # Write each event as one JSON line (keeps original message + timestamp)
                fout.write(json.dumps({
                    "timestamp": e["timestamp"],           # epoch millis
                    "ingestionTime": e.get("ingestionTime"),
                    "logStreamName": e.get("logStreamName"),
                    "message": e.get("message", "")
                }, ensure_ascii=False) + "\n")
                saved += 1
                if saved >= args.max:
                    break

            next_token = resp.get("nextToken")
            if saved >= args.max or not next_token:
                break

    print(f"Saved {saved} event(s) to {args.out}")
    if saved == 0:
        print("No events matched. Try increasing --minutes or adjusting --pattern.", file=sys.stderr)

if __name__ == "__main__":
    main()