# Detailed Student-Facing Script — 3-Hour Delivery (No ML)

This is the full narration to speak, not a bullet outline — companion to
[session_plan_3hr_no_ml.md](session_plan_3hr_no_ml.md), which has the
timings and question prompts. Use that one to stay on schedule; use this
one for the actual words when you want the explanation fully spelled
out, e.g. the first few times you deliver this session.

Everything here is grounded in the real code in this repo — line
references point at the actual scripts, not paraphrased pseudocode.

---

## Part A — Problem framing & architecture (35 min)

### 0:00–0:15 — Business problem

"Before we open a single file, I want to give you a scenario, and I want
you to argue with me.

Imagine you're the data engineer at a large e-commerce company. Every
day, your platform logs 500 million events — someone views a product,
adds it to a cart, starts checkout, attempts payment, completes a
purchase, or abandons at any of those steps. That's happening across 40
million users. Every one of those actions is a row you need to capture,
store, and eventually turn into something a business analyst or a
machine learning model can use.

Here's my question: if I handed you all of that as one giant CSV file —
just one file, 500 million rows a day — what breaks?

*[Let the room answer. Don't fill the silence too fast. Push toward two
specific conclusions if they don't get there on their own:]*

First — it doesn't fit in memory on one machine, and honestly it
probably doesn't fit comfortably on one machine's disk either once you
accumulate a few days of it. Whatever tool reads this file needs to
split the work across many machines. That's the whole reason distributed
computing frameworks like Spark exist, and why we store this on HDFS
instead of a regular filesystem — HDFS splits a file into blocks and
spreads them across a cluster of machines, and Spark reads those blocks
in parallel.

Second — a CSV file is something that exists only after it's completely
finished being written. If a customer completes a purchase right now, and
your business wants to know about it in the next five minutes — for
fraud detection, for inventory, for a live dashboard — you cannot wait
for tonight's batch file to land. That's why part of this platform is
going to be streaming, not just batch — we'll build both today.

One more question before we move on: Kaggle has e-commerce datasets you
could download right now, for free, in about thirty seconds. Why are we
generating our own data with a Python script instead?

*[Answer to land, if they don't get there:]* Scale. A typical Kaggle
e-commerce dataset is a few hundred megabytes. At that size, you never
have to think about how Spark partitions your data, when a join should
broadcast a small table instead of shuffling both sides across the
network, or what happens when one product accounts for forty percent of
all your traffic. Those problems only show up at real scale, and this
entire project exists to put you in front of them deliberately, in a
controlled setting, before you meet them for the first time in
production at 2am."

### 0:15–0:35 — Architecture walkthrough

*Open README.md, show the architecture diagram.*

"What you're looking at is called a medallion architecture — Bronze,
Silver, Gold. I want to be very clear about one thing: this is not a
Delta Lake feature. Delta Lake doesn't know or care what you name your
folders. Bronze, Silver, and Gold are a *discipline* — a way of keeping
three fundamentally different kinds of decision-making separated from
each other, so they don't get tangled into one giant 'cleaned data'
table that nobody can safely change.

**Bronze** is a faithful, append-only copy of whatever arrived. No
judgment calls at all. If a null email address shows up in the raw file,
it lands in Bronze as a null email address. Why keep garbage data
around? Because six weeks from now, when someone discovers that a report
was wrong, the first question anyone asks is: which file, ingested when,
looked like what? If you've already cleaned and discarded the messy
version, you can't answer that question — and worse, the only way to
re-derive the truth is to go back to the source system, which in a real
company may have already rotated its logs and no longer has the data.
Bronze is your insurance policy against that.

**Silver** is where we apply judgment calls that are almost universally
correct — the kind nobody would reasonably argue with. Deduplicating a
row that appeared twice because an ingestion job re-ran. Dropping an
order that references a user ID that doesn't exist anywhere in our users
table. Converting a string that says '19.99' into an actual double.
Nobody's going to say 'actually, I wanted the duplicate rows.'

**Gold** is where the debatable business decisions live — and I mean
genuinely debatable, reasonable people could disagree. Here's a concrete
example from this exact project: when we compute RFM scores later today,
should a returned order still count toward a customer's 'frequency'
score? This codebase's answer is: yes for frequency — an order did
happen, the customer did place it — but no for monetary value, because
that revenue was given back. That's not an engineering decision. That's
a conversation you'd have with a business stakeholder, and Gold is where
that kind of decision gets encoded, deliberately kept separate from the
mechanical cleanup happening in Silver.

Now trace the diagram with me left to right. Raw files come in two
shapes — CSV and JSON — and get landed into Bronze completely as-is.
From there they flow into Silver, where we clean, deduplicate, and
enforce referential integrity. Silver splits into two Gold outputs: one
branch computes RFM customer scores and category revenue rollups, the
other detects revenue leakage — abandoned carts, failed payments,
returns. Separately, off to the side, there's a second entry point:
Kafka. Live order events flow in through a producer, get consumed by a
Structured Streaming job, and get merged directly into Bronze — so
Bronze isn't purely a batch concept, it's just 'the raw landing zone,'
regardless of whether the data arrived as a file or as a stream."

---

## Part B — Bronze (35 min)

### 0:35–0:50 — Live demo: data generation

*Run: `python data_generation/generate_ecommerce_data.py`*

"While this runs, let me explain what it's actually building, because
this isn't just random noise — it's simulating a funnel.

Every session in this dataset probabilistically walks through five
stages: view, add to cart, checkout start, payment attempt, purchase.
Not every session makes it all the way through — most don't, and that
drop-off is deliberately realistic, because the entire second half of
today's session — the revenue leakage report — depends on that drop-off
being there to detect.

Here's a question for you: if 35% of people who view a product add it to
their cart, and 55% of those people go on to start checkout, what
fraction of *all* sessions reach checkout?

*[Work it: 0.35 × 0.55 ≈ 19%.]*

Hold onto that nineteen percent. In about ninety minutes, we're going to
run a script that turns exactly this kind of drop-off into an actual
dollar figure — the kind of number a VP of E-commerce would ask about in
a Monday morning meeting. That's not a coincidence; it's the whole point
of simulating a funnel instead of just generating random rows."

*Optionally point at `config.py`'s `DEMO_SCALE` dict — 200K users, 1.5M
sessions, ~5M events, ~450K orders — and contrast with `FULL_SCALE`:
40M users, 500M events. Say:* "Every script we run today runs completely
unchanged at either scale. The only thing that changes is this constant
and where the data physically lives — local disk today, HDFS on a real
cluster. That's deliberate: the code doesn't get harder at scale, only
the operational challenges around it do — and that's the lesson for the
rest of the afternoon."

### 0:50–1:05 — Live demo: Bronze ingest

*Run: `python bronze/01_ingest_to_bronze.py`. Open the file.*

"Look at `land_as_bronze()` — it's deliberately almost nothing. Read the
file, add two columns, write it out as Delta with `mode('overwrite')`.
That's it. No filtering, no deduplication, no type casting beyond what
the CSV/JSON reader infers automatically.

The two columns it adds are the only 'logic' in this entire file:
`_ingested_at`, a timestamp of when this load ran, and `_source_file`,
literally the path of the file this row came from, via
`F.input_file_name()`.

Question: we're about to clean this exact data in the next script
anyway. Why bother keeping these two extra columns in Bronze at all?

*[Answer to land:]* Audit trail. Imagine a report is wrong. The
business asks: 'where did this number come from?' If all you have is the
cleaned Silver table, your honest answer is 'I don't know which raw file
produced this row, or when it arrived.' With these two columns, you can
answer precisely: this row came from `orders_2024_03_15.csv`, ingested at
14:32 UTC. That's the difference between a five-minute investigation and
a five-hour one."

### 1:05–1:10 — Discussion: raw messiness

*Open a raw CSV file directly, point at a null email field, a duplicate
`order_id` if one's visible.*

"This is what real upstream data looks like — not the tidy Kaggle
version. There's a null email right there. Somewhere in this file, if
you look hard enough, you'll find a duplicate order ID, because the
system that generated this file re-sent a batch after a network hiccup.

Question: what does it cost us if we hadn't built a Bronze layer, and
this pipeline breaks somewhere inside Silver next week?

*[Land on:]* Without Bronze, 'reprocessing' means going back to the
source system and re-extracting the data — and in a real company, that
source system might not even have the data anymore. Log retention
policies, database purges, API rate limits on historical pulls — all of
that stands between you and re-deriving the truth. Bronze means
reprocessing is just re-running a script against data you already have
safely stored."

---

## ☕ Break (10 min) — 1:10–1:20

---

## Part C — Silver & Delta (45 min)

### 1:20–1:40 — Live demo: Silver transformation

*Run: `python silver/02_bronze_to_silver.py`. Open the file alongside
the output.*

"This script is doing four distinct things, and I want to name each one
so you don't lose track of which piece of logic is solving which
problem.

**First: deduplication.** Look at `dedup_latest()` — it's a window
function. We partition by the natural key, order by `_ingested_at`
descending, number the rows, and keep only row number 1. In plain
English: if the same `user_id` shows up in Bronze three times because we
ran the ingest job three times, we keep only the most recent one. This
works because Bronze is append-only — every ingest run adds rows, never
overwrites — so duplicates are expected and this is how we resolve them.

**Second: type and null handling.** Look at `build_silver_users()` —
we lowercase and trim the email, cast `signup_date` to an actual date
type, and filter out any row where `user_id` is null. Small, boring,
essential — these are the fixes nobody would ever argue with.

**Third — and this is the one I want you to really absorb — referential
integrity, using a broadcast join.** Look at line 110:
`.join(F.broadcast(users.select('user_id')), 'user_id', 'left_semi')`.

Two things are happening in that one line. `left_semi` is a join type
that keeps only the columns from the *left* side — orders — for every
row where a match exists on the right. We're not enriching orders with
user columns here; we're using the join purely as a filter, to drop any
order that references a `user_id` that doesn't exist in our clean users
table. That happens constantly with real upstream systems — a
clickstream event fires before the user record finishes syncing, or a
product gets delisted mid-session while someone still has it in an open
tab.

The `F.broadcast(...)` part is the other half, and it's about
performance, not correctness. Normally, when Spark joins two large
tables, it has to shuffle both sides across the network so that matching
keys land on the same executor — expensive, especially when one side has
hundreds of millions of rows. `broadcast()` tells Spark: this users table
is small, just send a full copy of it to every executor instead. Then
the giant orders table never has to move across the network at all — only
the small table does. This is the single most important performance
decision in this entire script, and we'll come back to exactly why in
about twenty minutes.

**Fourth: session stitching.** Look at `build_silver_sessions()`. Raw
clickstream data is one row per *event* — a view, an add-to-cart, a
checkout start. Nobody downstream wants to reason about individual
events; they want to reason about *sessions*. This function groups every
event by `session_id`, finds the highest funnel stage that session
reached using a rank map — view is stage 1, purchase is stage 5 — and
labels the session accordingly: `purchased`, `payment_attempted`,
`checkout_started`, `cart_abandoned`, or `browse_only`. This
`sessions` table, not the raw clickstream, is what both of today's Gold
scripts actually read from."

### 1:40–1:55 — Delta Lake time travel (do not rush or cut this)

*Re-run `silver/02_bronze_to_silver.py` a second time, live, then run:*

```python
spark.sql("DESCRIBE HISTORY delta.`data/silver/users`").show(truncate=False)
spark.read.format("delta").option("versionAsOf", 0).load("data/silver/users").show()
```

"I want you to watch what just happened, because it's the single most
important five minutes of today.

I ran the exact same script twice. Look at `upsert_dimension()` back in
the code — for the users and products tables, we don't `overwrite` the
Delta table. We use `DeltaTable.forPath(...).merge(...)`, with
`whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()`. That's an
upsert: if a `user_id` already exists, update it; if it's new, insert it.
A single atomic operation.

Compare that to a plain Parquet write, which is all-or-nothing.
`overwrite` destroys everything that was there before. `append` can
silently create duplicate rows if a job accidentally re-runs. Delta adds
a transaction log — that `_delta_log` folder sitting right next to the
data files — and that log is what makes both an atomic MERGE *and* what
I'm about to show you next possible.

Look at the output of `DESCRIBE HISTORY`. There are now two versions of
this table — version 0 from the first run, version 1 from the second.
Every write to a Delta table is a new entry in that log, not a
destructive overwrite of the previous state. That means I can ask Spark
for the table exactly as it looked *before* my second run —
`versionAsOf(0)` — and get it back, row for row, right now, without a
backup, without asking anyone.

Here's why this matters more than almost anything else you'll see today:
imagine it's 3am, a bad file got dropped into the pipeline, and this
table now has garbage in production. Without Delta, your only recovery
path is restoring from a backup — if one exists, if it's recent enough,
if someone remembers where it lives. With Delta, you roll back to the
version right before the bad write, and you're done in minutes, not
hours. That's the difference between a minor incident and a resume-
generating one."

### 1:55–2:05 — Data skew and shuffle (concept only)

"Quick honesty check before I explain this: at the scale we're running
today — a few million rows — almost nothing I'm about to describe will
visibly go wrong. That's exactly why I have to *tell* you about it
instead of showing you a failure. This problem only shows up at real
production scale, and by the time you meet it for the first time, it's
usually already expensive — a job that used to take twenty minutes is
now taking six hours and nobody knows why.

We already saw the first tool: broadcast joins, back in the Silver
script. When one side of a join is small — a dimension table like users
or products — send the whole thing to every executor instead of
shuffling the giant fact table across the network. That's the first
thing to reach for, always.

But broadcast joins don't solve everything. Imagine one single product
is responsible for forty percent of all clickstream events — a flash
sale item, something that went viral. Even with a perfect join strategy,
whatever partition ends up holding that one product's key becomes what's
called a straggler — it's doing forty times the work of a typical
partition, and the entire job has to wait for it to finish before moving
on. That's data skew.

The first fix, and the one already turned on in this project's
`config.py` — look at `spark.sql.adaptive.skewJoin.enabled` — is
Adaptive Query Execution. Spark actually detects the oversized partition
at runtime and automatically splits it into smaller pieces. Try this
first. It's free — you don't write any extra code.

The second fix, for when AQE isn't available or the skew is severe, is
called salting: you manually append a random number to the skewed key on
both sides of the join, which artificially splits that one hot key into,
say, ten sub-keys spread across ten partitions, and then you aggregate
the partial results back together at the end. It's more code and more
complexity — think of it as the manual version of what AQE does for you
automatically.

I'm not going to work a full salting example on the board today — ask me
afterward if you want to see it. What I want you to leave with is this:
you will not see this fail today. You will see it fail on your first
real hundred-million-row job. When that happens, come back to these five
minutes."

---

## Part D — Gold (25 min)

### 2:05–2:20 — Live demo: RFM scoring

*Run: `python gold/03_silver_to_gold_rfm.py`. Show the segment
distribution output.*

"RFM stands for Recency, Frequency, Monetary — and it's a decades-old
direct-marketing technique, older than any of the tools we're using
today. It's still used constantly because it needs no external data
source at all — just your own orders table.

Look at `build_customer_rfm()`. Recency is `datediff` between today and
this customer's most recent order — fewer days is better. Frequency is a
simple count of completed orders — more is better. Monetary is the sum
of `net_amount` across completed orders — more is better.

Notice the filter right at the top: `orders.filter(order_status ==
'completed')`. That's the debatable Gold-layer decision I mentioned an
hour ago — a returned order doesn't count toward monetary value, because
that revenue was refunded. It still counts toward frequency, because the
order genuinely did happen. That's a business call encoded directly in
this line of code.

Now look at how each dimension gets scored: `F.ntile(5)` — quintiles.
Question for you: why quintiles, computed dynamically from this specific
customer base, instead of a hardcoded rule like 'more than 10 orders
this year equals a frequency score of 5'?

*[Answer to land:]* A hardcoded threshold breaks silently the moment
your business grows. If '10 orders' was a great score five years ago
when you had ten thousand customers, it might be a mediocre score today
when you have ten million and your best customers order fifty times a
year. `ntile(5)` re-derives the top twenty percent, whatever that
actually looks like, every single time this job runs. It's
self-adjusting by construction.

Look at the segment output on screen — Champions, Loyal Customers,
Potential Loyalists, At Risk, Lost/Hibernating. Each is just a threshold
on the summed R+F+M score, but now every customer in your business has a
label a marketing team can actually act on."

### 2:20–2:30 — Live demo: revenue leakage

*Run: `python gold/04_revenue_leakage.py`. Show the output totals.*

"This script detects three distinct kinds of lost revenue, and I want
you to notice that all three are derived from tables Silver already
built — no new raw data needed.

**Abandoned carts** — look at the filter:
`funnel_stage_reached == 'cart_abandoned'`. That's a session that added a
product to cart but never started checkout. We estimate the lost revenue
as that product's list price.

**Failed payments** — a session reached `payment_attempted` but
`payment_status == 'failed'`.

**Returns** — an order that completed, then got reversed, filtered
directly from the orders table on `order_status == 'returned'`.

Remember the nineteen percent checkout-conversion number from ninety
minutes ago? Look at this abandoned-cart total on screen right now. This
is that same funnel drop-off, except now it's not a percentage on a
whiteboard — it's an actual dollar figure. This is the number a VP of
E-commerce would ask about in a Monday morning meeting, and it exists
because we chose to simulate a realistic funnel back in Part B instead
of generating random rows."

---

## Part E — Streaming, orchestration, wrap-up (30 min)

### 2:30–2:45 — Live demo: streaming (trimmed)

```bash
docker compose up -d
python streaming/06_kafka_order_producer.py --rate 3     # terminal A
python streaming/07_structured_streaming_to_delta.py     # terminal B
```

"Run both of these side by side so you can watch it happen. Terminal A
is producing simulated live orders onto a Kafka topic called
`ecommerce.orders.live` — three a second. Terminal B is a Structured
Streaming job, continuously consuming that topic and writing into Delta.

Watch the console output in terminal B — those are windowed metrics
updating in near real time, not a batch job you have to wait for.

Under the hood, terminal B is using the exact same MERGE upsert pattern
we saw in Silver an hour ago — look at `upsert_batch()` in
`07_structured_streaming_to_delta.py` if you want to check afterward.

Question: what would happen to this table if the streaming job crashed
right now and restarted from its last checkpoint, if we *hadn't* used a
MERGE — say we'd just used a plain append instead?

*[Answer to land:]* Duplicate rows. On restart, Structured Streaming
replays some amount of data from the last checkpoint to guarantee nothing
was missed — that's correct and necessary — but a plain append would
re-insert rows that already made it into the table before the crash. The
MERGE is what makes that replay safe: matching keys get updated in
place, not duplicated."

### 2:45–2:55 — Airflow DAG walkthrough (no live run)

*Open `airflow/dags/ecommerce_pipeline_dag.py`, walk through it without
executing.*

"I'm not going to run this live — a full Airflow install is out of scope
for today's room — but I want you to see the shape of it, because four
decisions in here only matter once a pipeline runs unattended every
night, not when you're clicking 'run' by hand.

First: `transform_silver >> [build_gold_rfm, detect_revenue_leakage]` —
notice that's a list, not a single next step. RFM scoring and revenue
leakage detection both only depend on Silver, and they don't depend on
each other, so the DAG runs them in parallel instead of serializing work
that has no reason to be serial. That's a fan-out.

Second: `catchup=False`. Without this flag, the very first time you turn
this DAG on, Airflow will try to backfill every single scheduled run
between the DAG's `start_date` and right now — which for a daily
pipeline defined months ago could mean it tries to run months of
backfills all at once. Almost never what you actually want for a
pipeline that only cares about 'yesterday's data.' This is a classic
first-day surprise if you don't set it explicitly.

Third: retries with a delay, not an immediate retry. A transient failure
— an executor running out of memory, a flaky HDFS read — often just
needs the cluster a minute to recover. Retrying instantly usually just
fails the same way again.

Fourth, and this one's a design decision as much as a technical one:
streaming is deliberately *not* in this DAG at all. Airflow is built
around tasks that start and finish. A Structured Streaming query runs
forever, by design — it never finishes. Trying to manage a
forever-running job as an Airflow task either blocks a scheduler slot
indefinitely or forces you into awkward workarounds. The streaming job we
just ran lives on its own, deployed as a long-running service — a
systemd service, a YARN long-running application, a Kubernetes
deployment — completely independent of this DAG."

### 2:55–3:00 — Wrap-up

"Here's the repo — everything we just ran, plus a few things we
deliberately didn't touch today, including customer segmentation with
K-Means clustering, in `ml/05_customer_segmentation_kmeans.py`. If
you're curious, `notes.md` walks through why feature scaling matters
before clustering, and how the elbow method picks the number of
clusters — read it on your own, and come find me with questions.

I want to close with the honest framing, because I think it's the most
important thing to take away from today. Everything you watched run in
this room happened in minutes, on a single laptop, against a few million
rows. Open `README.md` — look at the 'Scale: demo vs. production'
section. The exact same code, completely unchanged, is what runs for
hours across a cluster of hundreds of machines at the actual scale
described in the syllabus — fifty gigabytes, five hundred million events
a day. Flip one constant in `config.py` from `DEMO_SCALE` to
`FULL_SCALE`, point the paths at HDFS instead of local disk, and submit
it with `spark-submit --master yarn` instead of running it directly.

The code does not get harder at that scale. The debugging does. That's
the entire reason this project exists."

---

## If you're running long

Same priority order as the timing companion doc — cut the Airflow
walkthrough first (just name the four decisions verbally, don't open the
file), then trim streaming to the producer only, then compress the
skew/shuffle section to a single sentence. Never cut or rush the Delta
time-travel demo — if something has to give, it comes from Part E.
