#!/usr/bin/env python3
"""
Generate synthetic "Operational DB" daily dumps for the Northwind-Next task.

Each generated day produces ONE JSONL file mixing records from three logical tables:
- customers
- orders
- order_items

Design goals:
- Provide realistic daily variability
- Allow late/updated customer records (to justify SCD on the warehouse side)
- Keep timestamps timezone-aware (UTC) and orders in mixed currencies (USD/EUR)
- Be deterministic with --seed for reproducible review runs

Output:
  data/opdb/opdb_YYYY-MM-DD.jsonl
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic OPDB daily JSONL files.")
    p.add_argument("--days", type=int, default=7, help="How many days to generate (>=1).")
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (use the same seed to get same data).",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="Optional ISO date (YYYY-MM-DD). If set, day #1 will be this date.",
    )
    p.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Shift the generated window into the past/future by N days (applied after start-date).",
    )
    return p.parse_args()


def iso_day(d: datetime) -> str:
    """Return YYYY-MM-DD string for a datetime.date/datetime in UTC."""
    return d.date().isoformat()


def main() -> None:
    args = parse_args()
    assert args.days >= 1, "--days must be >= 1"
    random.seed(args.seed)

    out_dir = Path("data/opdb")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Domain enumerations
    countries = ["KZ", "US", "GB", "DE", "AE", "CY", "NL", "SE", "SG", "AU"]
    statuses = ["new", "paid", "shipped", "cancelled"]
    skus = [f"SKU-{i:04d}" for i in range(1, 101)]

    # A fixed pool of customers; some will be created/updated over time.
    customer_ids = [f"C{1000 + i}" for i in range(200)]
    created_at_map: Dict[str, str] = {}

    # Decide the start day:
    # - if --start-date provided, use it
    # - else: today - days (so the last day ~= today)
    if args.start_date:
        start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    else:
        start = datetime.now(timezone.utc) - timedelta(days=args.days)

    # Apply optional offset
    start = start + timedelta(days=args.start_offset)

    order_id = 100_000

    for day_idx in range(args.days):
        day_dt = start + timedelta(days=day_idx)
        rows: List[dict] = []

        # --- customers block -------------------------------------------------
        # Create new customers (15% chance) and occasional "updates" (2% chance).
        # Updates are duplicated records with possibly different status/country,
        # which will trigger SCD2 behavior in the warehouse.
# --- customers block (with op_ts for SCD2) -------------------------------
        for cid in customer_ids:
            new_prob = 0.15
            upd_prob = 0.02
            if cid not in created_at_map and random.random() < new_prob:
                created_at_map[cid] = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=timezone.utc).isoformat()
                rows.append({
                    "table": "customers",
                    "customer_id": cid,
                    "created_at": created_at_map[cid],
                    "status": random.choice(["active", "blocked"]),
                    "country": random.choice(countries),
                    # op_ts = the effective timestamp of this version (used for SCD2)
                    "op_ts": datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=timezone.utc).isoformat()
                })
            elif cid in created_at_map and random.random() < upd_prob:
                rows.append({
                    "table": "customers",
                    "customer_id": cid,
                    "created_at": created_at_map[cid],
                    "status": random.choice(["active", "blocked"]),
                    "country": random.choice(countries),
                    "op_ts": datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=timezone.utc).isoformat()
                })


        # --- orders + order_items block -------------------------------------
        # For each day, emit 40..80 orders. Each order gets 1..5 items with random prices.
        # Orders are stamped with UTC timestamps and mixed currencies (USD/EUR).
        num_orders = random.randint(40, 80)
        for _ in range(num_orders):
            order_id += 1
            cid = random.choice(customer_ids)
            ts = datetime(
                day_dt.year,
                day_dt.month,
                day_dt.day,
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
                tzinfo=timezone.utc,
            )
            status = random.choices(statuses, weights=[0.6, 0.25, 0.1, 0.05])[0]
            currency = random.choice(["USD", "EUR"])

            total_amount = 0.0
            items: List[dict] = []
            for _i in range(random.randint(1, 5)):
                sku = random.choice(skus)
                qty = random.randint(1, 4)
                price = round(random.uniform(5, 250), 2)
                total_amount += qty * price
                items.append(
                    {
                        "table": "order_items",
                        "order_id": order_id,
                        "sku": sku,
                        "quantity": qty,
                        "unit_price": price,
                    }
                )

            rows.append(
                {
                    "table": "orders",
                    "order_id": order_id,
                    "customer_id": cid,
                    # epoch seconds are easy to convert to TIMESTAMP in BigQuery
                    "order_timestamp": int(ts.timestamp()),
                    "status": status,
                    "currency": currency,
                    "total_amount": round(total_amount, 2),
                }
            )
            rows.extend(items)

        # --- write one JSONL per day ----------------------------------------
        out_path = out_dir / f"opdb_{iso_day(day_dt)}.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[✓] Generated {args.days} daily files under {out_dir.resolve()}")


if __name__ == "__main__":
    main()
