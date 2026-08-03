# Enterprise Project 1 — Concepts

This is the "why", to go with the code. Read alongside the scripts, not
instead of them — every concept below has a corresponding line in the
codebase, referenced inline.

## Medallion architecture: why three layers, not one

Bronze/Silver/Gold isn't a Delta Lake feature — it's a discipline for
separating three concerns that get tangled together in a single "cleaned
data" table:

- **Bronze** ([`bronze/01_ingest_to_bronze.py`](bronze/01_ingest_to_bronze.py)) is a faithful, append-only copy of
  what arrived. No judgment calls. If a downstream bug is discovered
  months later, Bronze lets you replay history without going back to a
  source system that may have rotated its logs by then.
- **Silver** ([`silver/02_bronze_to_silver.py`](silver/02_bronze_to_silver.py)) applies judgment calls that
  are almost universally correct: dedup, type enforcement, dropping rows
  with broken foreign keys. Nobody reasonably argues these should be
  undone.
- **Gold** ([`gold/`](gold/)) applies judgment calls that *are* debatable:
  should a returned order still count toward "frequency" in RFM? (This
  project says no for Monetary, but still yes for Frequency — an order
  did happen. That's a business decision, not an engineering one, and
  Gold is where those decisions live, isolated from the mechanical
  cleanup in Silver.)

## Delta Lake upserts and time travel

A plain Parquet write is all-or-nothing: `overwrite` replaces everything,
`append` can duplicate rows if you accidentally re-run a job. Delta adds
a transaction log (`_delta_log/`) on top of Parquet files, which is what
makes two things possible that plain Parquet can't do:

**MERGE (upsert)** — [`silver/02_bronze_to_silver.py`](silver/02_bronze_to_silver.py)'s `upsert_dimension()`
and [`streaming/07_structured_streaming_to_delta.py`](streaming/07_structured_streaming_to_delta.py)'s `upsert_batch()`
both do this: insert a new key, update an existing one, in a single
atomic operation.

```python
DeltaTable.forPath(spark, target_path).alias("t") \
    .merge(updates_df.alias("s"), "t.user_id = s.user_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

Without MERGE, an upsert is "read the whole table, union with the new
rows, dedup by key, overwrite the whole table" — correct, but it rewrites
every file even when 99.9% of rows didn't change. MERGE only touches the
files that actually contain matching keys.

**Time travel** — every write is a new log entry, not a destructive
overwrite of the previous state (until you run `VACUUM`). Demo this live:

```python
# after re-running silver/02_bronze_to_silver.py a second time
spark.read.format("delta").option("versionAsOf", 0).load("data/silver/users").show()
spark.sql("DESCRIBE HISTORY delta.`data/silver/users`").show(truncate=False)
```

This is the answer to "a bad job corrupted our table, what do we do" —
roll back to a prior version instead of restoring from a backup.

## Data skew and shuffle at billion-row scale

At demo scale (a few million rows) almost nothing you do will visibly
skew or thrash — that's exactly why this needs to be *taught*, not just
demonstrated: the failure mode only shows up at production scale, and by
then it's expensive to discover for the first time.

**Broadcast joins** — [`silver/02_bronze_to_silver.py`](silver/02_bronze_to_silver.py) joins the large
`orders`/`clickstream` fact tables against small `users`/`products`
dimension tables using `F.broadcast(...)`. A normal join shuffles *both*
sides across the network so matching keys land on the same executor —
expensive when one side has 500M rows. Broadcasting sends the small side
(a few MB to a few hundred MB) to every executor instead, so the large
side never moves. Spark's optimizer does this automatically under
`spark.sql.autoBroadcastJoinThreshold` (default 10MB), but at the sizes
in this project it's made explicit so the decision is visible in the
code, not buried in a query plan.

**Skew** — even with a good join strategy, if one key dominates (e.g. one
product responsible for 40% of all clickstream events, or a `null`
category absorbing every unmatched row), the partition holding that key
becomes a straggler that the whole job waits on. Two mitigations, in
order of preference:
1. **Adaptive Query Execution's skew join handling** — enabled in
   [`config.py`](config.py) via `spark.sql.adaptive.skewJoin.enabled`.
   Spark detects the oversized partition at runtime and splits it
   automatically. Try this first; it's free.
2. **Salting** — if AQE isn't available (older Spark) or the skew is
   severe: append a random suffix (`salt = rand() % N`) to the skewed
   join key on both sides, splitting the hot key into N sub-keys spread
   across N partitions, then aggregate the partial results back together.
   More code, but it's the manual version of what AQE does for you.

**Shuffle partition count** — `spark.sql.shuffle.partitions` defaults to
200 regardless of data size. Too few partitions on a huge dataset means
each partition is too big to fit comfortably in executor memory; too many
on a small dataset (this demo) wastes time on scheduling overhead for
near-empty partitions. [`config.py`](config.py) sets it to 8 for the demo scale —
on the full-scale cluster run this should scale with the cluster
(commonly 2-3x total executor cores) or be left to AQE's
`spark.sql.adaptive.coalescePartitions.enabled`.

## RFM scoring

Recency, Frequency, Monetary — a decades-old direct-marketing technique
that still works because it needs no external data, just the orders
table itself:

- **Recency**: `datediff(as_of_date, max(order_timestamp))` — how many
  days since this customer's last order. Lower is better.
- **Frequency**: `count(order_id)` — how many orders. Higher is better.
- **Monetary**: `sum(net_amount)` — total revenue from this customer.
  Higher is better.

Each dimension is bucketed 1-5 with `ntile(5)` (quintiles, not fixed
thresholds — this makes the scoring self-adjusting to whatever the
current customer base actually looks like, rather than hardcoding "more
than 10 orders = frequency score 5" which breaks the moment the business
grows). The three scores combine into a segment: see
[`gold/03_silver_to_gold_rfm.py`](gold/03_silver_to_gold_rfm.py)'s `rfm_segment` thresholds.

RFM is transparent (anyone can read the rule and argue with it) but
rigid — it can't discover a pattern nobody thought to encode as a rule.
That's the segue into K-Means.

## K-Means at scale

[`ml/05_customer_segmentation_kmeans.py`](ml/05_customer_segmentation_kmeans.py) clusters customers on RFM *plus*
behavioral features (session count, average session duration, cart
abandonment rate) that a hand-written rule would likely miss combining.

Three things that matter more here than in a textbook K-Means example:

1. **Feature scaling is not optional.** `monetary` ranges into the tens
   of thousands; `cart_abandonment_rate` ranges 0-1. Euclidean distance
   (what K-Means minimizes) would be completely dominated by `monetary`
   without `StandardScaler` — the model would just be re-deriving "who
   spent the most," ignoring every other feature. Always scale before
   clustering, never after.
2. **Choosing k isn't a guess.** `pick_k_with_elbow()` prints WSSSE
   (within-cluster sum of squared errors — lower is tighter clusters) and
   silhouette score (higher is better-separated clusters) across a range
   of k. The "elbow" is where adding another cluster stops meaningfully
   reducing WSSSE. This project settles on k=4 for a segmentation that's
   still human-interpretable; the elbow plot is what you'd actually show
   a stakeholder to justify that choice.
3. **Cluster IDs are arbitrary; labels are derived, not hardcoded.**
   K-Means assigns cluster `0, 1, 2, 3` in no meaningful order — it can
   flip between runs. The code ranks clusters by average `monetary`
   *after* fitting and labels them accordingly, so "High-Value Loyalists"
   always means the top-spending cluster regardless of which integer
   K-Means happened to assign it.

MLlib's `KMeans` runs Lloyd's algorithm distributed across partitions —
each executor computes local distance sums to the current centroids, the
driver aggregates and updates centroids, repeat until convergence. This
is why it scales to millions of customers: the expensive part
(distance calculation) is embarrassingly parallel: it's the same reason
a `groupBy().agg()` scales, just with an iterative loop around it.

## Airflow DAG design for recurring pipelines

[`airflow/dags/ecommerce_pipeline_dag.py`](airflow/dags/ecommerce_pipeline_dag.py) demonstrates a few decisions
that matter more once a DAG runs unattended every night than they do when
you're clicking "run" by hand:

- **Fan-out / fan-in, not a straight chain.** `gold_rfm` and
  `revenue_leakage` both only depend on Silver and don't depend on each
  other — the DAG runs them in parallel (`transform_silver >>
  [build_gold_rfm, detect_revenue_leakage]`) rather than serializing work
  that doesn't need to be serial.
- **`catchup=False`.** Without this, Airflow will try to backfill every
  scheduled run between `start_date` and now the first time the DAG is
  turned on — almost never what you want for a pipeline that only cares
  about "yesterday's data," and a classic first-day surprise.
- **Retries with a delay, not immediate retry.** A transient failure
  (an executor OOM, a flaky HDFS read) often needs the cluster a minute
  to recover; retrying instantly just fails the same way again.
- **Streaming is deliberately excluded from this DAG.** Airflow schedules
  tasks that start and finish; a Structured Streaming query runs forever.
  Trying to manage a forever-running job as an Airflow task either blocks
  the scheduler slot indefinitely or requires awkward workarounds. The
  streaming job in [`streaming/`](streaming/) is meant to be deployed and monitored
  as its own long-lived application (systemd service, YARN long-running
  application, or a Kubernetes deployment), independent of the DAG here.
