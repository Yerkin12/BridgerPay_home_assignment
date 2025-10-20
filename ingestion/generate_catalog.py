#!/usr/bin/env python3
"""
Generate daily product catalog snapshots (Parquet preferred, CSV fallback).

Each snapshot contains 1 row per SKU with the current attributes as of that day.
Some days are intentionally skipped to simulate missing snapshots in real life.

Output files:
  data/catalog/products_YYYY-MM-DD.parquet  (if pyarrow/fastparquet available)
  or
  data/catalog/products_YYYY-MM-DD.csv      (fallback)

Columns:
  - sku (string)
  - title (string)
  - category (string; e.g., electronics, books, ...)
  - unit_cost (float)
  - active_flag (bool)
  - snapshot_date (YYYY-MM-DD)
"""

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate daily product catalog snapshots.")
    p.add_argument("--days", type=int, default=7, help="How many days to generate (>=1).")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="Optional ISO date (YYYY-MM-DD). If set, day #1 will be this date.",
    )
    p.add_argument(
        "--skip-prob",
        type=float,
        default=0.15,
        help="Probability to skip a day to simulate missing snapshots (0..1).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    assert args.days >= 1, "--days must be >= 1"
    assert 0.0 <= args.skip_prob <= 1.0, "--skip-prob must be between 0 and 1"
    random.seed(args.seed)

    out_dir = Path("data/catalog")
    out_dir.mkdir(parents=True, exist_ok=True)

    categories = ["electronics", "books", "sports", "beauty", "toys", "fashion", "home"]

    # Determine start day:
    if args.start_date:
        start = datetime.fromisoformat(args.start_date).date()
    else:
        # end ~ today
        start = (datetime.utcnow().date() - timedelta(days=args.days))

    for i in range(args.days):
        day = start + timedelta(days=i)

        # Randomly skip some days to emulate missing snapshots
        if random.random() < args.skip_prob:
            continue

        rows = []
        for n in range(100):  # 100 SKUs
            sku = f"SKU-{n+1:04d}"
            rows.append(
                {
                    "sku": sku,
                    "title": f"Product {sku}",
                    "category": random.choice(categories),
                    "unit_cost": round(random.uniform(1.0, 120.0), 2),
                    "active_flag": random.choice([True, True, True, False]),
                    "snapshot_date": str(day),
                }
            )

        df = pd.DataFrame(rows)
        base = out_dir / f"products_{day.isoformat()}"

        # Try Parquet first; fallback to CSV if pyarrow/fastparquet is not installed.
        try:
            df.to_parquet(str(base) + ".parquet", index=False)  # requires pyarrow or fastparquet
        except Exception:
            df.to_csv(str(base) + ".csv", index=False)

    print(f"[✓] Generated up to {args.days} daily snapshots under {out_dir.resolve()} (with some skipped days).")


if __name__ == "__main__":
    main()
