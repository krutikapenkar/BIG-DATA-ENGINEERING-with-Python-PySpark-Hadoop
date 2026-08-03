# Enterprise Project 1 — E-Commerce Customer Behaviour & Revenue Analytics Platform

**Trainer-led live demonstration.** Students observe, discuss, review the
code, and receive the full codebase — the goal is exposure to the
complexity, design decisions, debugging, and performance tuning of a real
production Big Data system, which academic exercises alone can't give.

| Attribute | Details |
|---|---|
| Domain | E-Commerce / Retail Analytics |
| Dataset (syllabus target) | ~50 GB, 500M+ rows of clickstream + transactional data |
| Dataset (this repo, live-demo scale) | ~1-2 GB, ~5M clickstream events + ~450K orders — see [Scale: demo vs. production](#scale-demo-vs-production) |
| Duration | 5 hours across 2-3 sessions — see [session_plan.md](session_plan.md) |
| Difficulty | Intermediate to Advanced |
| Technologies | PySpark, Spark SQL, Delta Lake, Kafka + Structured Streaming, Airflow, Parquet |

## Business problem

A large e-commerce platform generates 500 million+ events daily — product
views, add-to-cart actions, checkout steps, and purchases across 40
million users. The analytics platform built here:

1. Processes raw clickstream logs (batch from files, live from Kafka)
2. Builds a Customer 360 view: purchase history, session behaviour, RFM scoring
3. Identifies high-value customer segments with unsupervised ML (K-Means)
4. Detects revenue leakage: abandoned carts, failed payments, returns
5. Produces Gold-layer tables ready for a BI dashboard

## Architecture

```
raw files (CSV/JSON)              live orders
        |                              |
        v                              v
  01_ingest_to_bronze.py       06_kafka_order_producer.py
        |                              |
        v                              v
   BRONZE (Delta)              Kafka topic: ecommerce.orders.live
        |                              |
        v                              v
02_bronze_to_silver.py     07_structured_streaming_to_delta.py
  - dedup, null/type fixes         - MERGE upsert into bronze/streaming_orders
  - referential integrity          - live windowed metrics to console
  - session stitching
        |
        v
    SILVER (Delta)
        |
        +----------------------+----------------------------+
        v                                                    v
03_silver_to_gold_rfm.py                          04_revenue_leakage.py
  - RFM scoring per customer                        - abandoned carts
  - daily category revenue rollup                   - failed payments
        |                                            - returns
        v                                                    |
    GOLD (Delta)  <----------------------------------------- +
        |
        v
05_customer_segmentation_kmeans.py
  - feature engineering + K-Means
        |
        v
    GOLD (Delta): customer_segments_kmeans
        |
        v
08_export_gold_for_bi.py  -->  Parquet + CSV, Power BI / Tableau ready
```

An Airflow DAG ([airflow/dags/ecommerce_pipeline_dag.py](airflow/dags/ecommerce_pipeline_dag.py))
orchestrates the batch half of this (everything except Kafka/Structured
Streaming, which runs as its own always-on job — see the DAG's docstring
for why).

## Repo layout

| Path | What it does |
|---|---|
| [`config.py`](config.py) | Shared paths, scale constants, the Spark+Delta session builder |
| [`data_generation/`](data_generation/) | Synthetic data generator (Faker for reference data, pure Spark for the fact tables) |
| [`bronze/`](bronze/) | Raw -> Delta, provenance columns only, no cleaning |
| [`silver/`](silver/) | Dedup, null/type handling, referential integrity, session stitching, Delta MERGE upserts |
| [`gold/`](gold/) | RFM scoring, category rollups, revenue leakage detection |
| [`ml/`](ml/) | K-Means customer segmentation (MLlib) |
| [`streaming/`](streaming/) | Kafka producer + Structured Streaming consumer |
| [`exports/`](exports/) | Gold -> Parquet/CSV for BI tools |
| [`dashboard/`](dashboard/) | Streamlit app reading the CSV exports, for a quick in-browser look without Power BI/Tableau |
| [`airflow/dags/`](airflow/dags/) | Daily orchestration DAG |
| [`docker-compose.yml`](docker-compose.yml) | Single-broker Kafka + Zookeeper for the streaming demo |
| [`notes.md`](notes.md) | Concepts explained: Delta Lake upserts/time travel, skew & shuffle, RFM, K-Means at scale, DAG design |
| [`session_plan.md`](session_plan.md) | Trainer's session-by-session script for the 5-hour delivery |

## How to run

Assumes [00-environment-setup](../../00-environment-setup/) is done (Java, Python, and either
local PySpark or the WSL2 Hadoop cluster, depending which mode you use).

```bash
cd enterprise-projects/project-1-ecommerce-analytics
pip install -r requirements.txt

# 1. Generate synthetic data (demo scale: ~5M events, a few minutes on a laptop)
python data_generation/generate_ecommerce_data.py

# 2. Batch pipeline, in order
python bronze/01_ingest_to_bronze.py
python silver/02_bronze_to_silver.py
python gold/03_silver_to_gold_rfm.py
python gold/04_revenue_leakage.py
python ml/05_customer_segmentation_kmeans.py
python exports/08_export_gold_for_bi.py

# 3. View the results in a browser dashboard
streamlit run dashboard/app.py

# 4. Streaming demo (optional, needs Docker)
docker compose up -d
python streaming/06_kafka_order_producer.py --rate 3     # terminal A
python streaming/07_structured_streaming_to_delta.py     # terminal B
```

Each script prints row counts and sample output as it runs — that's
deliberate, so a live demo has visible checkpoints rather than a silent
five-minute wait.

**Quick smoke test before a live class:** set `ECOM_TINY_RUN=1` to shrink
the dataset to a few thousand rows so the whole pipeline finishes in under
a minute — useful for verifying the environment works before running the
real demo scale in front of students.

```bash
ECOM_TINY_RUN=1 python data_generation/generate_ecommerce_data.py
ECOM_TINY_RUN=1 python bronze/01_ingest_to_bronze.py
# ...and so on
```

## Scale: demo vs. production

The syllabus specifies ~50GB / 500M+ rows — not something that generates
or processes usefully live in a classroom on a laptop. `config.py` has
both scales defined (`DEMO_SCALE`, `FULL_SCALE`); **the pipeline code
itself does not change between them** — that's the point being
demonstrated. To actually run at production scale:

1. Point `RUN_MODE=cluster` (reads/writes HDFS paths instead of local `data/`)
2. Swap `SCALE = DEMO_SCALE` for `SCALE = FULL_SCALE` in `config.py`
3. Run via `spark-submit --master yarn` against a real multi-node cluster,
   with `spark.sql.shuffle.partitions` raised accordingly (see
   [notes.md](notes.md) for the shuffle/skew discussion)

## Prerequisites

- Everything from [00-environment-setup](../../00-environment-setup/)
- Docker Desktop, only if running the Kafka streaming portion
- 8GB+ RAM recommended for the demo-scale run (the K-Means step in
  particular holds the customer feature table in memory)
