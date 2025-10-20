#!/usr/bin/env python3
"""
Generate daily EUR->USD FX rates (mock).
Output: data/fx_rates.csv with columns: date, eur_usd
"""
import argparse, csv
from datetime import datetime, timedelta

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = p.parse_args()

    start = datetime.fromisoformat(args.start).date()
    end = datetime.fromisoformat(args.end).date()

    rate = 1.08
    rows = []
    cur = start
    while cur <= end:
        # simple pseudo-random drift around ~1.08
        rate += (((hash(str(cur)) % 100)/100) - 0.5) * 0.002
        rows.append((str(cur), round(rate, 4)))
        cur += timedelta(days=1)

    with open("data/fx_rates.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["date","eur_usd"]); w.writerows(rows)
    print("[✓] Wrote data/fx_rates.csv")

if __name__ == "__main__":
    main()
