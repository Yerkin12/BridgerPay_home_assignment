#!/usr/bin/env python3
"""
Generate synthetic web events with duplicates and late arrivals.
Output: data/events/web_events_YYYY-MM-DD.jsonl
Columns: event_id, ts (ISO UTC), customer_id, event, sku, device
"""
import argparse, random, json, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--late_prob", type=float, default=0.10, help="Share of late events")
    p.add_argument("--dup_prob", type=float, default=0.03, help="Share of duplicates")
    args = p.parse_args()
    random.seed(args.seed)

    out_dir = Path("data/events"); out_dir.mkdir(parents=True, exist_ok=True)
    event_types = ["page_view", "add_to_cart", "checkout_start", "purchase"]
    devices = ["mobile", "desktop", "tablet"]

    start = (datetime.now(timezone.utc).date() - timedelta(days=args.days))
    for d in range(args.days):
        day = start + timedelta(days=d)
        rows = []
        for _ in range(random.randint(400, 700)):
            ts = datetime(day.year, day.month, day.day,
                          random.randint(0,23), random.randint(0,59), random.randint(0,59),
                          tzinfo=timezone.utc)
            evt = random.choices(event_types, weights=[0.6,0.25,0.1,0.05])[0]
            row = {
                "event_id": str(uuid.uuid4()),
                "ts": ts.isoformat(),
                "customer_id": f"C{random.randint(1000,1199)}",
                "event": evt,
                "sku": f"SKU-{random.randint(1,100):04d}",
                "device": random.choice(devices)
            }
            rows.append(row)

            # duplicate ~3%
            if random.random() < args.dup_prob:
                rows.append(row)
            # late arrival: ts - 36h (~10%)
            if random.random() < args.late_prob:
                late = dict(row)
                late["ts"] = (ts - timedelta(hours=36)).isoformat()
                rows.append(late)

        out = out_dir / f"web_events_{day.isoformat()}.jsonl"
        with open(out, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    print(f"[✓] Wrote {args.days} web-event files to {out_dir.resolve()}")

if __name__ == "__main__":
    main()
