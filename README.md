# BridgerPay — Northwind-Next (Data Engineering Take-Home)

**Goal.** Build an end-to-end, production-style data pipeline that lands multi-source data, models it with a layered approach (**bronze → silver → gold**) in a cloud warehouse, and exposes daily product/commerce metrics with basic data quality and backfill strategy.

**Why this matters.** The solution demonstrates practical skills in scalable ingestion, warehouse modeling (incl. SCDs), incremental processing, orchestration, and operational guardrails (DQ, late/duplicate handling).

## Key Capabilities

* Multi-source ingestion: operational orders/customers/items, product catalog snapshots, web events, and FX rates.
* Layered modeling in the warehouse: conformed dimensions (SCD), normalized facts, and metric marts.
* Incremental & idempotent loads using partitioning + `MERGE`.
* Late/duplicate event handling with a time watermark.
* Orchestration via Airflow (Cloud Composer ready).
* Lightweight data-quality checks and simple observability.
* Backfill procedure for specific dates/ranges.

## Architecture (overview)

* **Landing/Bronze:** Raw, schema-tolerant tables partitioned by event/ingest time.
* **Silver:** Business-ready dims & facts:

  * `dim_customer` — SCD Type 2 (status/country history).
  * `dim_product` — Type 1 or Type 2 (documented choice).
  * `fact_orders` — normalized to USD with FX; item_count; status.
  * `fact_web_events` — dedup by `event_id`, watermark for late arrivals.
* **Gold:** Daily metrics: orders, GMV, AOV, add-to-cart rate, conversion rate.
* **DQ:** PK uniqueness, not-nulls, freshness of daily partitions.
* **Backfills:** Partition-aware `MERGE`; adjustable watermark for events.

> See `/docs/northwind_next_architecture.pdf` for a diagram and design notes. *(Optional but recommended.)*

## Tech Stack

* **Cloud:** Google Cloud Platform (GCS, BigQuery, Cloud Composer/Airflow).
* **Transformations:** Standard SQL (BigQuery).
* **Orchestration:** Airflow 2 DAG (Composer-compatible).
* **Data format:** JSONL (opDB & events), Parquet/CSV (catalog/FX).
* **Language/Utils:** Python for generation/aux scripts.

## Repository Layout

```
/ingestion          # generators & loaders (opDB, catalog snapshots, web events, FX)
/models
  /bronze           # DDL for bronze landing
  /silver           # SCD dims, facts (orders, web_events)
/gold               # daily metric marts
/models/tests       # SQL DQ checks (uniqueness, not-null, freshness)
/orchestration      # Airflow DAG + backfill guide
/queries            # reviewer queries (GMV top-5, funnel, late-events impact)
/docs               # architecture PDF or notes
```

## Data Contracts (sources)

* **Operational DB** (orders/customers/items): late inserts & status updates may happen.
* **Catalog snapshots** (daily Parquet/CSV in GCS): days may be missing; use last-known-good until next snapshot.
* **Web events**: duplicates possible; late events up to **48h**.
* **FX rates**: daily EUR↔USD; used to normalize order amounts to USD.

## Modeling Details

* **SCD choices**

  * `dim_customer`: Type 2 — track historical status/country (`valid_from`, `valid_to`, `is_current`).
  * `dim_product`: Type 1 or 2. **TODO:** State your choice and rationale (e.g., “chose Type 2 to preserve historical unit_cost/category affecting GMV backfills”).
* **Facts**

  * `fact_orders`: `order_ts`, `status`, `item_count`, `currency`, `total_amount_source`, `total_amount_usd` (via FX on `DATE(order_ts)`).
  * `fact_web_events`: de-duplicated (`event_id`), filtered to watermark window; partitioned by `DATE(ts)`.

## Incrementality & Late Data

* Partitioned tables, `MERGE` upserts, idempotent by natural/business keys.
* **Watermark** for `fact_web_events` = **48h** by default.
  Backfill expands watermark or re-runs over a date range.

## Data Quality Checks

* PK uniqueness: `order_id` in `fact_orders`, `event_id` in `fact_web_events`.
* Not-null for key fields (IDs, timestamps).
* Freshness: detect stale days in gold daily metrics.

## Orchestration (Airflow/Composer)

DAG stages (daily):

1. Create/ensure bronze tables
2. Land/load bronze partitions
3. Build silver dims (SCD)
4. Build silver facts (orders, web_events)
5. Build gold daily metrics
6. Run DQ checks and fail on violations

**Config:** environment variables for project/dataset, retries, schedule.

## Backfill Guide (short)

* Recompute specific partitions with parameterized queries or DAG `dagrun.conf`.
* For events, temporarily raise watermark (e.g., 48h → 168h) or run historical windows.
* Always use `MERGE` into partitioned targets to stay idempotent.

## How Reviewers Can Validate

* Inspect repo structure & modeling rationale (this README + `/docs`).
* Run SQL in `/models` in order (bronze → silver → gold).
* Execute DQ SQL in `/models/tests`.
* Run the sample analytics in `/queries`:

  * **Top-5 categories by GMV (7d)**
  * **Conversion funnel (14d)**
  * **Late-events share & impact (>6h)**


Want me to fill the `TODO` fields (project ID, dataset, SCD choice) with your actual values now?
