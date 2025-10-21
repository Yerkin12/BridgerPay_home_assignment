# BridgerPay — Northwind-Next (BigQuery **Sandbox** Implementation)

**Goal**
Build an end-to-end analytics pipeline in **BigQuery Sandbox (EU)** using **CTAS-only** models (no DML), from raw multi-source data to daily business metrics.

**Why Sandbox?**
Sandbox forbids DML (`MERGE/UPDATE/DELETE`) and streaming. We therefore rebuild models with **`CREATE OR REPLACE TABLE … AS SELECT` (CTAS)** — either full rebuild or by date windows. This keeps the solution deterministic and simple to review.

---

## Live Configuration

* **GCP Project ID:** `bp-nw-next-yerkin`
* **BigQuery Dataset:** `northwind_next`
* **Location:** **EU**
* **Reporting currency:** `USD`
* **Late events threshold:** `> 6h` (computed via `load_day` → `load_ts`)

> Note: Everything here assumes **BigQuery Web UI** (no `gcloud`/`bq` CLI needed).

---

## What’s Included (What I actually built)

* **Synthetic sources (local files):**

  * **OPDB** day files (JSONL): customers, orders, order_items (with `op_ts` for SCD2).
  * **Catalog snapshots** (Parquet/CSV): `sku`, `title`, `category`, `unit_cost`, `active_flag`, `snapshot_date` (some days intentionally missing).
  * **Web events** (JSONL): `event_id`, `ts`, `customer_id`, `event`, `sku`, `device`, **`load_day`** (to compute lateness).
  * **FX rates** (CSV): daily `eur_usd`.

* **Bronze (BigQuery, raw) — loaded via Web UI (Upload):**

  * `bronze_opdb_raw` — combined OPDB feed (has a column `table` = customers|orders|order_items).
  * `bronze_catalog_raw`
  * `bronze_web_events_raw` (base, without lateness) and `bronze_web_events_raw_v2` (**with `load_day`**).
  * `bronze_fx_rates`

* **Silver (CTAS models, no DML):**

  * `dim_customer` — **SCD2** from customer versions (ordered by `op_ts`).
  * `dim_product` — **SCD2** from daily snapshots (ordered by `snapshot_date`).
  * `fact_orders` — `item_count` from items + **USD normalization** via FX on `DATE(order_ts)`.
  * `fact_web_events` — **dedup** by `event_id` (earliest `ts`).
  * `fact_web_events_v2` — **dedup + lateness** with `delay_h = load_ts − ts`, where `load_ts = end_of_day(load_day)`.

* **Gold (CTAS marts):**

  * `gold_daily_metrics` — per-day: orders (excl. cancelled), `gmv_usd`, `aov_usd`, `add_to_cart_rate`, `conversion_rate`.
  * *(Optional)* `gold_daily_late_impact` — daily share of late events and late purchases.

* **Data Quality (SQL checks)** and **Reviewer queries** (3 scripts).

---

## Repository Layout

```
/ingestion
  generate_opdb.py         # OPDB JSONL (customers/orders/items; customers include op_ts)
  generate_catalog.py      # product snapshots (Parquet preferred, CSV fallback; some days missing)
  generate_web_events.py   # web events with duplicates + late arrivals + load_day
  generate_fx.py           # daily EUR->USD rates (CSV)

/models
  /silver
    dim_customer_ctas.sql
    dim_product_ctas.sql
    fact_orders_ctas.sql
    fact_web_events_ctas.sql
    fact_web_events_v2_ctas.sql
  /gold
    gold_daily_metrics_ctas.sql
    gold_daily_late_impact_ctas.sql
  /tests
    dq_checks.sql

/queries
  top5_categories_gmv_last7d.sql
  conversion_funnel_last14d.sql
  late_events_impact.sql
```

---

## Source Semantics (Data Contracts)

* **OPDB daily JSONL**
  Mixed rows for three logical tables:

  * `customers(customer_id, created_at, status, country, op_ts)` — **op_ts** is the effective-from timestamp (for SCD2).
  * `orders(order_id, customer_id, order_timestamp, status, currency, total_amount)`
  * `order_items(order_id, sku, quantity, unit_price)`
* **Catalog snapshots**
  One row per `sku` per day: `sku, title, category, unit_cost, active_flag, snapshot_date`. Some days are intentionally missing.
* **Web events JSONL**
  `event_id, ts (UTC ISO), customer_id, event, sku, device, load_day`. Contains duplicates and some late events.
* **FX CSV**
  `date, eur_usd` (daily EUR→USD).

---

## BigQuery — How to Run (Web UI, Sandbox, EU)

### 1) Create dataset (EU)

* In **BigQuery Explorer** → **Create dataset** → `northwind_next` → **Location**: **EU**.

### 2) Load Bronze tables (Upload)

For each local file:

* **Create table** → **Source**: *Upload* → **File format**: JSON (newline) / CSV / Parquet (as appropriate).
* **Destination**: `northwind_next.<table>` (see names below).
* **Schema**: **Auto-detect** (Parquet auto-infers).
* **Write disposition**: first file → *Write if empty*, subsequent files → *Append to table*.

Tables and file types:

* `bronze_opdb_raw` ← `data/opdb/opdb_*.jsonl` (JSONL)
* `bronze_catalog_raw` ← `data/catalog/products_*.parquet` *(or CSV)*
* `bronze_web_events_raw` ← `data/events/web_events_*.jsonl` (JSONL; base)
* `bronze_web_events_raw_v2` ← same JSONL **with `load_day`** (for lateness)
* `bronze_fx_rates` ← `data/fx_rates.csv` (CSV)

> Upload in UI supports **one file at a time**; that’s fine for this demo.

### 3) Build **Silver** (run CTAS scripts in order)

* `models/silver/dim_customer_ctas.sql`
* `models/silver/dim_product_ctas.sql`
* `models/silver/fact_orders_ctas.sql`
* `models/silver/fact_web_events_ctas.sql` (base)
* `models/silver/fact_web_events_v2_ctas.sql` (late events)

### 4) Build **Gold** (CTAS)

* `models/gold/gold_daily_metrics_ctas.sql`  *(no-CTE version to avoid UI parsing issues)*
* *(Optional)* `models/gold/gold_daily_late_impact_ctas.sql`

### 5) Run **DQ checks**

* Execute `models/tests/dq_checks.sql` — expect zeros on synthetic data.

### 6) Run **Reviewer queries**

* `queries/top5_categories_gmv_last7d.sql`
* `queries/conversion_funnel_last14d.sql`
* `queries/late_events_impact.sql` (uses `fact_web_events_v2`)

---

## Modeling Notes (What the SQL actually does)

* **SCD2** (customers/products):
  Ordered versions (by `op_ts` or `snapshot_date`) → intervals `[valid_from, valid_to]` with:

  * `valid_to = LEAD(valid_from) - 1s` (customers) or `next_day - 1 day` (products)
  * `is_current` when there’s no next version
* **As-of join** (facts → dims):
  Join on business key **and** date containment:
  `DATE(fact_ts) BETWEEN dim.valid_from AND dim.valid_to`
* **Orders fact**:
  `item_count` from `order_items`; `total_amount_usd` via FX by `DATE(order_ts)`.
* **Web events fact**:
  Deduplicate by `event_id` (earliest `ts`).
  **v2 lateness:** `load_ts = end_of_day(load_day)`, `delay_h = load_ts − ts`.
* **Gold daily metrics**:
  Per day: `orders` (status != 'cancelled'), `gmv_usd`, `aov_usd`, and event-based rates (add-to-cart, conversion).

---

## Data Quality (what’s checked)

* **PK uniqueness** in facts (`order_id`, `event_id`).
* **Not-null** key fields (IDs, timestamps).
* **Freshness** of `gold_daily_metrics` (simple stale-day sanity).

---
