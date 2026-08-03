# Session Plan — Enterprise Project 1

**Format reminder (per HOD note):** this is trainer-executed. You drive
the keyboard; students watch the terminal, read the code with you on
screen, and ask questions. At the end, everyone gets the full repo —
the value isn't "did they type it," it's "did they see production
decisions get made and defended."

Run `ECOM_TINY_RUN=1` against every script once, the night before, to
confirm the environment works end-to-end before doing it live at demo
scale. Nothing kills a live demo faster than a `pip install` failing in
front of the room.

Total: 5 hours, split across 3 sessions below (2hr / 2hr / 1hr). If your
slot is 2 sessions instead, merge Session 3 into the end of Session 2.

---

## Session 1 (2 hours) — Problem framing, architecture, Bronze

**Objective:** students leave understanding *why* this platform has the
shape it does, before seeing a line of Spark code.

| Time | Activity |
|---|---|
| 0:00–0:20 | Business problem walkthrough. Put the numbers on the board: 500M events/day, 40M users. Ask: *"If this were one giant CSV, what breaks first?"* — let them arrive at "it doesn't fit on one machine" and "you can't wait for the whole file to process before seeing anything" themselves. |
| 0:20–0:45 | Architecture diagram walkthrough ([README.md](README.md)). Explain Bronze/Silver/Gold as a discipline, not a Delta feature — see [notes.md § Medallion architecture](notes.md#medallion-architecture-why-three-layers-not-one). |
| 0:45–1:15 | **Live demo:** run `data_generation/generate_ecommerce_data.py` at demo scale. While it runs, walk through the funnel drop-off logic in the code (view→cart→checkout→payment→purchase) and ask students to predict: *"If 35% add to cart and 55% of those check out, what fraction of all sessions reach checkout?"* Tie this directly to the abandoned-cart leakage they'll compute in Session 2. |
| 1:15–1:40 | **Live demo:** run `bronze/01_ingest_to_bronze.py`. Point out `_ingested_at` / `_source_file` — ask *"why keep these if we're just going to clean the data in Silver anyway?"* (answer: audit trail — when a report is wrong, "which file, ingested when" is the first question asked). |
| 1:40–2:00 | Discussion: show real messiness in the raw files (open a raw CSV, point out a null email, a duplicate order_id). Ask: *"What's the cost of NOT having a Bronze layer if this pipeline breaks in Silver next week?"* |

**Discussion prompts to hold in reserve:** *"Kaggle datasets for e-commerce are usually a few hundred MB — why are we generating our own instead of just using one?"* (Answer: scale. A few hundred MB doesn't force you to think about partitioning, broadcast joins, or shuffle — the entire point of this project is the production-scale problems, not the domain.)

---

## Session 2 (2 hours) — Silver, Gold, and the two big engineering ideas

**Objective:** the two learning outcomes that don't show up in a normal
course project: Delta upserts/time travel, and data skew/shuffle tuning.

| Time | Activity |
|---|---|
| 0:00–0:30 | **Live demo:** run `silver/02_bronze_to_silver.py`. Walk through `dedup_latest()`, the referential-integrity `left_semi` joins, and session stitching (`build_silver_sessions`). Ask: *"Why left_semi instead of a normal inner join here?"* (keeps only fact-table columns — we're filtering, not enriching). |
| 0:30–1:00 | **Delta Lake deep dive.** Re-run the silver script a second time live. Show `DESCRIBE HISTORY delta.\`data/silver/users\`` — two versions now exist. Show `VERSION AS OF 0` returning the pre-rerun state. This is the moment to say out loud: *"This is what makes a 3am 'someone dropped a bad file into the pipeline' incident recoverable in minutes instead of hours."* See [notes.md § Delta Lake upserts and time travel](notes.md#delta-lake-upserts-and-time-travel). |
| 1:00–1:30 | **Skew and shuffle.** This is conceptual, not visibly demoable at demo scale (that's the teaching challenge — name it explicitly: *"You won't see this fail today because our dataset is small. You WILL see it fail on your first real 100M-row job, and when you do, come back to this slide."*) Walk through broadcast joins in the Silver script, then explain salting on the whiteboard with a worked example (one product = 40% of clickstream; show the manual salt-key trick). See [notes.md § Data skew and shuffle](notes.md#data-skew-and-shuffle-at-billion-row-scale). |
| 1:30–1:50 | **Live demo:** run `gold/03_silver_to_gold_rfm.py`. Show the segment distribution output. Ask: *"Why quintiles (ntile) instead of hardcoded thresholds like 'more than 10 orders = score 5'?"* |
| 1:50–2:00 | **Live demo:** run `gold/04_revenue_leakage.py`. Show the three leakage types and dollar totals. Tie back to Session 1's funnel math — this is where that abandoned-cart percentage becomes an actual revenue number a VP would care about. |

---

## Session 3 (1 hour) — ML segmentation, streaming, orchestration, wrap-up

**Objective:** show the two "advanced" pieces (K-Means, Kafka) as live
demos even though there's not enough time to teach them from scratch —
the goal here is exposure and code-reading, not mastery.

| Time | Activity |
|---|---|
| 0:00–0:20 | **Live demo:** run `ml/05_customer_segmentation_kmeans.py`. Show the elbow-method output on screen and ask: *"Looking at these WSSSE and silhouette numbers, would you pick k=4 or a different value? Why?"* Show the RFM segment vs. K-Means segment side by side for a few sample customers — this contrast (rule-based vs. discovered) is the single most important takeaway of the ML component. |
| 0:20–0:40 | **Live demo:** streaming. Start `docker compose up -d`, then in split terminals run the Kafka producer and the Structured Streaming consumer side by side so students watch orders appear in near-real-time. Point out the `foreachBatch` + Delta MERGE pattern — ask *"what would happen to this table if the streaming job crashed and restarted from the last checkpoint, without the merge?"* (answer: duplicate rows — the merge is what makes replay safe.) |
| 0:40–0:55 | Walk through the Airflow DAG file without running it live (a full Airflow install is out of scope for the room). Explain the fan-out/fan-in shape and why streaming isn't in this DAG — see [notes.md § Airflow DAG design](notes.md#airflow-dag-design-for-recurring-pipelines). |
| 0:55–1:00 | Wrap-up: hand over the repo, point at `README.md`'s "Scale: demo vs. production" section, and close with the honest framing — *"Everything you just watched ran in minutes on a laptop. The same code, unchanged, is what runs for hours across a cluster of hundreds of machines at the real scale in the syllabus. The code doesn't get harder at that scale — the debugging does."* |

## If you're short on time

Cut in this order: (1) the salting whiteboard example — mention it,
don't work the full example, (2) the BI export script — just show the
output files exist, (3) the second Airflow walkthrough pass — one lap
through the DAG shape is enough.

Do **not** cut: the Delta time-travel demo (Session 2) and the RFM vs.
K-Means contrast (Session 3) — those are the two moments where students
see something they can't get from reading documentation alone.
