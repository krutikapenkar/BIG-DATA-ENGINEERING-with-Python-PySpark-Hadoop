# Session Script — 3-Hour Compressed Delivery (No ML)

Companion to [session_plan.md](session_plan.md), for slots where you only
have 3 hours instead of 5 and are dropping the K-Means segmentation
module entirely. This is a talk-track, not just a timetable — say the
bracketed lines close to as written, they're calibrated to land specific
points, not just fill time.

**Cut entirely (mention only in wrap-up):** ML/K-Means segmentation.
**Kept, trimmed:** everything else, including a shortened streaming demo
and a single-pass (no live run) Airflow walkthrough.
**Not cut, per session_plan.md's own priority order:** the Delta
time-travel demo — it's the one moment students can't get from reading
docs alone.

Run `ECOM_TINY_RUN=1` against every script the night before to confirm
the environment works. A `pip install` failure live burns time you don't
have in a 3-hour slot.

Total runtime: 180 minutes including one 10-minute break.

---

## Part A — Problem framing & architecture (35 min)

### 0:00–0:15 — Business problem

**Say:** "This platform handles 500 million events a day — product
views, add-to-carts, checkouts, purchases — from 40 million users.
Before we look at a single line of Spark, I want you to tell me what
breaks if I hand you all of that as one giant CSV file."

Write `500M events/day`, `40M users` on the board. Let the room answer —
push toward two conclusions, don't just state them:

- "It doesn't fit on one machine" (leads into why we need Spark/HDFS)
- "You can't wait for the whole file before seeing anything" (leads into
  why part of this is streaming, not just batch)

**Say:** "Kaggle has e-commerce datasets already — why didn't we just
download one and use it?" Answer to land: a few hundred MB doesn't force
you to think about partitioning, broadcast joins, or shuffle. The whole
point of this project is the production-scale problems, not the retail
domain itself.

### 0:15–0:35 — Architecture walkthrough

Open `README.md`, show the architecture diagram.

**Say:** "Bronze, Silver, Gold isn't a Delta Lake feature — it's a
discipline for keeping three different kinds of judgment call separate."
Walk the three layers using [notes.md § Medallion architecture](notes.md#medallion-architecture-why-three-layers-not-one):

- Bronze = faithful copy, no judgment calls
- Silver = judgment calls nobody reasonably argues with (dedup, broken
  foreign keys)
- Gold = judgment calls that *are* debatable — give the RFM example:
  "should a returned order still count as a 'frequency' for that
  customer? This project says yes for frequency, no for monetary — that's
  a business call, not an engineering one, and it lives in Gold on
  purpose."

Trace the diagram left to right: raw files → Bronze → Silver → Gold →
export, and note the second branch (Kafka → Structured Streaming →
Bronze) joining in from the side.

---

## Part B — Bronze (35 min)

### 0:35–0:50 — Live demo: data generation

Run:
```bash
python data_generation/generate_ecommerce_data.py
```

**While it runs, say:** "This isn't just filler data — it's a funnel:
view → cart → checkout → payment → purchase, with realistic drop-off at
each stage." Ask: "If 35% of viewers add to cart, and 55% of those check
out, what fraction of all sessions reach checkout?" (≈19%). **Say:**
"Hold that number — Part D's revenue leakage report turns exactly this
kind of drop-off into a dollar figure a VP would ask about."

### 0:50–1:05 — Live demo: Bronze ingest

Run:
```bash
python bronze/01_ingest_to_bronze.py
```

Open the script, point at `_ingested_at` and `_source_file`.

**Ask:** "We're going to clean this data in Silver anyway — why keep
these extra columns in Bronze at all?" Answer to land: audit trail. When
a report is wrong six weeks from now, "which file, ingested when" is the
first question anyone asks, and Bronze is the only layer that can answer
it.

### 1:05–1:10 — Discussion: raw messiness

Open a raw CSV, point at a null email, a duplicate `order_id`.

**Ask:** "What does it cost us if we DIDN'T have a Bronze layer, and this
pipeline breaks inside Silver next week?" Land on: without Bronze, you'd
have to go back to the source system — which may have already rotated
its logs.

---

## ☕ Break (10 min) — 1:10–1:20

---

## Part C — Silver & Delta (45 min)

### 1:20–1:40 — Live demo: Silver transformation

Run:
```bash
python silver/02_bronze_to_silver.py
```

Walk through `dedup_latest()`, the referential-integrity joins, and
`build_silver_sessions()`.

**Ask:** "Why `left_semi` instead of a normal inner join here?" Answer:
`left_semi` keeps only the fact-table columns — we're filtering rows,
not enriching them, and an inner join would needlessly duplicate
dimension columns onto every row.

### 1:40–1:55 — Delta Lake time travel (do not cut this)

Re-run the same script live a second time, then run:
```python
spark.sql("DESCRIBE HISTORY delta.`data/silver/users`").show(truncate=False)
spark.read.format("delta").option("versionAsOf", 0).load("data/silver/users").show()
```

**Say, slowly, and mean it:** "This is the answer to '3am, someone
dropped a bad file into the pipeline, production is wrong, what do we
do.' You don't restore from a backup. You roll back to the version right
before the bad write, in minutes." This is the single highest-value five
minutes of the whole session — don't rush it.

### 1:55–2:05 — Data skew and shuffle (concept only, no salting whiteboard)

**Say:** "At our demo scale — a few million rows — almost nothing here
will visibly break. That's exactly why I have to *tell* you about it
instead of showing you: the failure only shows up at production scale,
and by then it's expensive to discover for the first time."

Cover, briefly, from [notes.md § Data skew and shuffle](notes.md#data-skew-and-shuffle-at-billion-row-scale):

- **Broadcast joins**: point at `F.broadcast(...)` in the Silver script —
  sends the small dimension table to every executor instead of shuffling
  the 500M-row fact table across the network.
- **Skew**: one product responsible for 40% of clickstream events creates
  a straggler partition the whole job waits on. Name the fix (AQE skew
  join handling, `spark.sql.adaptive.skewJoin.enabled`, already on in
  `config.py`) but skip working the manual salting example on the
  whiteboard — mention that salting exists as the manual fallback when
  AQE isn't available, and move on.

**Say:** "You won't see this fail today. You will see it fail on your
first real 100-million-row job — when it does, come back to this five
minutes."

---

## Part D — Gold (25 min)

### 2:05–2:20 — Live demo: RFM scoring

Run:
```bash
python gold/03_silver_to_gold_rfm.py
```

Show the segment distribution output.

**Ask:** "Why quintiles (`ntile(5)`) instead of a hardcoded rule like
'more than 10 orders = frequency score 5'?" Answer: quintiles are
self-adjusting to whatever the current customer base looks like —
a fixed threshold breaks silently the moment the business grows.

### 2:20–2:30 — Live demo: revenue leakage

Run:
```bash
python gold/04_revenue_leakage.py
```

Show the three leakage types and dollar totals.

**Say:** "Remember the ~19% checkout-conversion number from Part B? This
is where that percentage becomes an actual number of dollars a VP would
ask about in a meeting."

---

## Part E — Streaming, orchestration, wrap-up (30 min)

### 2:30–2:45 — Live demo: streaming (trimmed)

```bash
docker compose up -d
python streaming/06_kafka_order_producer.py --rate 3     # terminal A
python streaming/07_structured_streaming_to_delta.py     # terminal B
```

Run both terminals side by side so the room watches orders land in
near-real-time.

**Ask:** "What happens to this table if the streaming job crashes and
restarts from its last checkpoint, and we *hadn't* used a MERGE?" Answer:
duplicate rows on replay — the MERGE upsert is what makes restart-safe
replay possible.

### 2:45–2:55 — Airflow DAG walkthrough (single pass, do not run live)

Open `airflow/dags/ecommerce_pipeline_dag.py` and walk it once, no
execution:

- Fan-out/fan-in: `transform_silver >> [build_gold_rfm,
  detect_revenue_leakage]` — these two don't depend on each other, so
  they run in parallel instead of serializing work that doesn't need to
  be serial.
- `catchup=False` — otherwise Airflow backfills every scheduled run
  between `start_date` and now the first time the DAG is turned on.
- Retries with a delay, not instant retry — gives a flaky HDFS read or an
  executor OOM a moment to recover.
- Streaming is deliberately *not* in this DAG — Airflow schedules tasks
  that start and finish; a Structured Streaming query runs forever and
  belongs in its own long-lived deployment instead.

### 2:55–3:00 — Wrap-up

Hand over the repo. Point at `README.md`'s "Scale: demo vs. production"
section.

**Say, as the closing line:** "Everything you watched today ran in
minutes on a laptop. The same code, completely unchanged, is what runs
for hours across a cluster of hundreds of machines at the real scale in
the syllabus — 50GB, 500 million rows a day. The code doesn't get harder
at that scale. The debugging does."

**Name what's in the repo but wasn't shown live today:** the K-Means
customer segmentation module (`ml/05_customer_segmentation_kmeans.py`)
and its elbow-method/silhouette-score walkthrough. Point them at
[notes.md § K-Means at scale](notes.md#k-means-at-scale) if they want to
read it on their own.

---

## If you're still running long

Cut in this order, cheapest-first:
1. Airflow walkthrough → skip entirely, just say the file exists and
   point at the fan-out/fan-in idea verbally
2. Streaming demo → cut to producer only, skip the consumer terminal,
   just describe what Structured Streaming + MERGE would do
3. Skew/shuffle discussion → compress to the one line: "broadcast small
   tables, let AQE handle skew, ask me about salting after class"

**Never cut:** the Delta time-travel demo. If you're out of time
anywhere else, take it from Part E, not from here.
