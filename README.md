# BridgerPay — Northwind-Next (Data Engineering Take-Home)

**Goal.** Build an end-to-end, production-style pipeline on GCP that lands multi-source data, models it with a layered approach (**bronze → silver → gold**) in BigQuery, and exposes daily commerce metrics with basic DQ and backfill strategy.

**Why it matters.** Demonstrates practical mastery in scalable ingestion, DWH modeling (incl. SCD), incremental processing, orchestration, and operational guardrails (DQ, late/duplicate handling).

---

## Live Config

* **GCP Project ID:** `bp-nw-next-yerkin`
* **BigQuery Dataset:** `northwind_next`
* **Dataset Location:** `EU`
* **Reporting currency:** `USD`
* **Event watermark (late data):** `48h`
* **SCD policy:** `dim_customer — Type 2`, `dim_product — Type 2`
* *(Optional)* **GCS raw bucket:** `gs://bp-nw-next-yerkin-raw`

---

## Architecture Overview

* **Landing / Bronze (BigQuery):** schema-tolerant raw tables partitioned by event/ingest time (opDB, catalog snapshots, web events, FX).
* **Silver (BigQuery):** business-ready dimensions & facts

  * `dim_customer` — **SCD2** (`valid_from`, `valid_to`, `is_current`) to track status/country history.
  * `dim_product` — **SCD2** to keep historical `unit_cost/category` for correct retro GMV/AOV.
  * `fact_orders` — item_count, currency normalization to USD via daily FX; idempotent `MERGE`.
  * `fact_web_events` — dedup by `event_id`, watermark window (`48h`) for late arrivals.
* **Gold (BigQuery):** daily metrics mart — orders, GMV (USD), AOV (USD), add-to-cart rate, conversion rate.
* **Orchestration:** Airflow 2 (Cloud Composer ready) — `bronze → silver → gold → dq`, retries, simple backfill flow.
* **Data Quality:** PK uniqueness, not-nulls for key fields, freshness of daily partitions.

---

## Repository Layout

```
/ingestion          # generators & loaders (opDB JSONL, catalog Parquet/CSV, web events JSONL, FX CSV)
/models
  /bronze           # DDL for bronze landing tables
  /silver           # SCD dims (customer/product), facts (orders, web_events)
/gold               # daily metric marts
/models/tests       # SQL DQ checks (uniqueness, not-null, freshness)
/orchestration      # Airflow DAG + backfill guide
/queries            # reviewer queries (GMV top-5, funnel, late-events impact)
/docs               # architecture PDF (diagram + design notes)
/data               # local synthetic data for demo runs
```

---

## Data Sources & Contracts

* **Operational DB** (orders/customers/items): late inserts and status updates can occur.
* **Catalog snapshots** (daily Parquet/CSV): some days may be missing; use last-known-good until next snapshot.
* **Web events**: duplicates can occur; late events up to **48h**.
* **FX rates** (daily EUR↔USD): used to normalize order amounts to USD.

---

## Modeling Details

### Bronze

* Raw landing tables per source; partitioned (event/ingest time).
* Loading strategy idempotent at the partition/day level.

### Silver

* **`dim_customer` (SCD2):** close previous version via `MERGE` when `status/country` change.
* **`dim_product` (SCD2):** daily snapshots → latest record per SKU up to `run_date`; close prev version on change.
* **`fact_orders`:** derive `item_count` from order_items; normalize amounts to USD using daily FX (`DATE(order_ts)`); idempotent upsert by `order_id`.
* **`fact_web_events`:** dedup by `event_id`; exclude events older than watermark window; idempotent upsert by `event_id`.

### Gold (daily)

* `orders`, `gmv_usd`, `aov_usd`, `add_to_cart_rate`, `conversion_rate`.

---

## Incrementality & Late Data

* **Partitioned tables + `MERGE`** for idempotent upserts.
* **Watermark = 48h** in events staging: balances freshness and completeness.
* Backfills expand watermark or re-run by date range.

---

## Data Quality (DQ)

* **PK uniqueness:** `order_id` in `fact_orders`, `event_id` in `fact_web_events`.
* **Not-null:** key IDs/timestamps in facts.
* **Freshness:** gold partitions should not be stale beyond threshold (simple daily sanity).

---

## Orchestration (Airflow / Composer)

**DAG stages (daily):**

1. Ensure bronze DDL
2. Land/load bronze partitions
3. Build silver dims (SCD)
4. Build silver facts (orders, web_events)
5. Build gold daily metrics
6. Run DQ checks (fail fast on violations)

**Config:** `PROJECT_ID`, `BQ_DATASET`, schedule `@daily`, 2 retries, watermark 48h.

---

## Backfills

* **Partition backfills:** re-compute specific date partitions via parameterized queries and `MERGE`.
* **Late events:** temporarily increase watermark (e.g., to 168h) or run historic windows via `dagrun.conf`.

---

## Getting Started

```bash
# Environment
export PROJECT_ID=bp-nw-next-yerkin
export BQ_DATASET=northwind_next
export BQ_LOCATION=EU
```

### 1) BigQuery dataset (EU)

```bash
bq --location=${BQ_LOCATION} mk -d --description "Northwind-Next demo" ${PROJECT_ID}:${BQ_DATASET}
```

### 2) Create bronze tables

```bash
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/bronze/ddl_bronze.sql
```

### 3) (Optional) Generate local synthetic data

```bash
python3 ingestion/generate_orders.py --days 10 --seed 42
python3 ingestion/generate_catalog.py --days 10 --seed 42
python3 ingestion/publish_web_events.py --days 10 --seed 42
python3 ingestion/fetch_fx.py --start 2025-09-01 --end 2025-09-30
```

### 4) Load raw data to bronze

* Use your preferred path (GCS → BigQuery, or local → `bq load`).
* The repo includes an example script that **prints** safe `bq load` commands by partition date:

```bash
bash ingestion/load_to_bq.sh ${PROJECT_ID} ${BQ_DATASET}
```

### 5) Build silver & gold

```bash
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/silver/dim_customer_scd2.sql
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/silver/dim_product_scd2.sql
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/silver/fact_orders.sql
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/silver/fact_web_events.sql
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/gold/daily_metrics.sql
```

### 6) Run DQ checks

```bash
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < models/tests/dq_checks.sql
```

---

## Validation & Reviewer Queries

Run sample analytics:

```bash
# Top-5 categories by GMV (last 7 days)
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < queries/top5_categories_gmv_last7d.sql

# Conversion funnel (last 14 days)
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < queries/conversion_funnel_last14d.sql

# Late-events share & impact (>6h)
bq --location=${BQ_LOCATION} query --use_legacy_sql=false < queries/late_events_impact.sql
```

---

## Assumptions & Limitations

* Synthetic, PII-free data. Security/governance controls (IAM roles, VPC-SC, CMEK) considered but not implemented here.
* FX is daily; intra-day FX not modeled.
* Event watermark is a freshness vs. completeness trade-off (48h by default).

---

## Security & Governance (brief)

* Principle of least privilege on service accounts.
* Audit logs for BQ and GCS.
* Column-level policies/tagging for PII (not in scope for synthetic demo).

---

## Cost & Performance Notes

* Partition pruning + `MERGE` for targeted updates.
* Prefer Parquet for catalog snapshots (columnar, compressed).
* Clustering on frequent filters/joins (e.g., `order_ts`, `sku`) — optional add-on.


Хочешь — сразу добавим это в твой репозиторий как первый коммит. Или идём дальше к Этапу 0: создаём структуру папок и кладём туда README?
