# Build-Along Tutorial — E-Commerce Analytics Platform, From an Empty Folder

This is a **typing-along tutorial**, not a "watch the trainer run it" script.
Students create every file themselves, in the order below, and by the end
they've reconstructed this entire project with their own hands.

This complements the other docs in this repo — don't duplicate them, point
to them:
- [`README.md`](README.md) — architecture diagram, the finished picture
- [`session_plan.md`](session_plan.md) / [`session_plan_3hr_no_ml.md`](session_plan_3hr_no_ml.md) — timing/pacing if you run this as a live class
- [`student_script_3hr_detailed.md`](student_script_3hr_detailed.md) — full narration script for *explaining* the concepts once code exists
- [`notes.md`](notes.md) — deep-dive on Delta Lake, skew, RFM, K-Means

Use *this* document for the "hands on keyboard, build it" phase. Use the
other two for the "now let's talk about why" phase, once each layer is built.

**Every code block below is the real, working code from this repo** — not
simplified pseudocode. Copy-paste-accurate, so a student who types along
ends up with a running pipeline, not something that "looks like" it.

---

## 0. Before you start (trainer checklist)

Confirm on your machine / the lab machines before class:

- [ ] Python 3.10 or 3.11 installed (`python --version`)
- [ ] Java 8 or 11 installed (`java -version`) — Spark needs a JVM
- [ ] ~8GB RAM free (the K-Means step holds the customer table in memory)
- [ ] Docker Desktop installed, **only** if you're doing the streaming part
- [ ] Internet access for `pip install` (first run only)

If any of this isn't done yet, it's covered in `00-environment-setup` one
directory up from `enterprise-projects/`.

---

## 1. Create the project folder

Everything lives under one root folder. Open a terminal where you want the
project to live and run:

```powershell
mkdir ecommerce-analytics
cd ecommerce-analytics
```

Now create the subfolders that mirror the medallion architecture
(Bronze → Silver → Gold) plus the supporting pieces:

```powershell
mkdir data_generation, bronze, silver, gold, ml, streaming, exports, dashboard
mkdir airflow, airflow\dags
```

**Ask the class:** why do the folder names match Bronze/Silver/Gold instead
of something like `raw/`, `cleaned/`, `final/`? Land on: this *is* the
medallion architecture — Bronze/Silver/Gold is a naming convention the whole
industry recognizes, so a new engineer joining the team already knows what
each folder is for before reading a line of code.

At the end of this step your tree looks like:

```
ecommerce-analytics/
├── data_generation/
├── bronze/
├── silver/
├── gold/
├── ml/
├── streaming/
├── exports/
├── dashboard/
└── airflow/
    └── dags/
```

---

## 2. Create and activate a virtual environment

Never install project dependencies into the system Python — a venv keeps
this project's exact PySpark/Delta versions isolated from anything else on
the machine.

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

(macOS/Linux equivalent, for reference: `python3 -m venv venv && source venv/bin/activate`)

You'll know it worked because the prompt now shows `(venv)` at the start of
the line. **Every command from here on assumes the venv is active** — if a
student closes their terminal, they must re-run the activate line before
continuing.

---

## 3. Create `requirements.txt` and install

Create `requirements.txt` in the project root:

```txt
# Pinned to match 00-environment-setup's Spark 3.5.1 install.
# If you pip installed a newer pyspark globally, either use a venv for this
# project or make sure SPARK_HOME is unset (a mismatched SPARK_HOME env var
# will make pyspark load the wrong version's jars regardless of this file).
pyspark==3.5.1
delta-spark==3.2.0
faker==26.0.0
numpy
pandas
kafka-python==2.0.2

# Streamlit dashboard (dashboard/app.py) - reads the CSVs from
# exports/08_export_gold_for_bi.py, no Spark dependency of its own.
streamlit
plotly

# Airflow is intentionally not pinned here - install it in its own venv
# per https://airflow.apache.org/docs/apache-airflow/stable/installation/,
# using the constraints file for your Python version. Mixing Airflow's
# dependency set into the same environment as PySpark is a common source
# of version conflicts.
```

Install:

```powershell
pip install -r requirements.txt
```

**Ask the class:** why pin `pyspark==3.5.1` and `delta-spark==3.2.0` to
exact versions instead of just `pyspark`? Land on: Delta Lake's Java jars
are matched to a specific Spark version — installing a newer PySpark than
the Delta build supports is one of the most common "works on my machine"
bugs in the field, and pinning avoids it for everyone in the room at once.

---

## 4. `config.py` — the foundation every script imports

This is the **first real file**, and every script from here on starts with
`from config import ...`. Build it in three pieces so students see *why*
each piece exists before typing it.

### 4a. Scale constants

Create `config.py` in the project root and start with:

```python
"""
Shared paths and scale settings for every script in this project.

DEMO_SCALE is what runs live in class on a laptop in local[*] mode.
FULL_SCALE is the number quoted in the syllabus (500M+ events / 50GB) —
the same scripts run unchanged on a real cluster with these constants
bumped, reading/writing HDFS paths instead of local ones. That's the
whole point of the exercise: the code doesn't change, only the inputs
and the cluster underneath it do.
"""

import os

# ---- Flip this for a cluster run -------------------------------------
# "local"   -> data/ folder on this machine, local[*] Spark
# "cluster" -> HDFS paths, spark-submit --master yarn
RUN_MODE = os.environ.get("ECOM_RUN_MODE", "local")

# ---- Scale --------------------------------------------------------------
# num_sessions drives everything else: each session probabilistically
# advances through view -> add_to_cart -> checkout_start -> payment_attempt
# -> purchase (see data_generation/generate_ecommerce_data.py for the
# funnel/drop-off rates). num_events and num_orders below are the expected
# *output* sizes, not inputs — they're here for the printed summary only.
DEMO_SCALE = {
    "num_users": 200_000,
    "num_products": 5_000,
    "num_sessions": 1_500_000,
    "approx_num_events": 5_000_000,
    "approx_num_orders": 450_000,
}

FULL_SCALE = {
    "num_users": 40_000_000,
    "num_products": 500_000,
    "num_sessions": 150_000_000,
    "approx_num_events": 500_000_000,
    "approx_num_orders": 45_000_000,
}

SCALE = DEMO_SCALE

# For a quick classroom smoke-test run before committing to the full demo
# scale: ECOM_TINY_RUN=1 shrinks everything to a few thousand rows so the
# whole pipeline finishes in seconds.
if os.environ.get("ECOM_TINY_RUN") == "1":
    SCALE = {
        "num_users": 500,
        "num_products": 100,
        "num_sessions": 3_000,
        "approx_num_events": 8_000,
        "approx_num_orders": 300,
    }
```

**Say out loud:** "`SCALE` is one dictionary. Every other script only ever
reads `config.SCALE`. That means switching the entire pipeline from 5
million rows to 500 million rows is a one-line change — swap `SCALE =
DEMO_SCALE` for `SCALE = FULL_SCALE` — nothing else in this project changes."

### 4b. Paths

Append to `config.py`:

```python
# ---- Paths --------------------------------------------------------------
if RUN_MODE == "cluster":
    BASE_PATH = "hdfs://localhost:9000/data"
else:
    BASE_PATH = os.environ.get(
        "ECOM_BASE_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
    )

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_ORDERS_TOPIC = "ecommerce.orders.live"
```

Point out: every one of `RAW_PATH`, `BRONZE_PATH`, `SILVER_PATH`,
`GOLD_PATH` is built off the same `BASE_PATH`. Nobody hardcodes a path
string anywhere else in this project — that discipline is what makes the
`local` → `cluster` switch (HDFS instead of a local folder) a one-line
change too.

### 4c. The shared Spark session builder

Append the last piece:

```python
def get_spark(app_name: str, shuffle_partitions: int = 8, extra_packages=None):
    """
    One shared builder so every script in the project configures Delta Lake
    the same way. shuffle_partitions defaults low (8) because the demo
    dataset is small — on the full-scale cluster run this should be raised
    (typically ~2-3x total executor cores) or left on adaptive execution.

    extra_packages: additional Maven coordinates needed on top of Delta,
    e.g. the Kafka connector for the streaming scripts.
    """
    from pyspark.sql import SparkSession

    extra_packages = extra_packages or []

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )

    if RUN_MODE != "cluster":
        builder = builder.master("local[*]")

    try:
        from delta import configure_spark_with_delta_pip

        spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    except ImportError:
        # delta-spark not installed as a wheel dependency yet -> fall back to
        # spark.jars.packages resolution (needs internet on first run).
        packages = ",".join(["io.delta:delta-spark_2.12:3.2.0"] + extra_packages)
        builder = builder.config("spark.jars.packages", packages)
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
```

**Ask the class:** why does *every* script call `get_spark(...)` instead of
each one writing its own `SparkSession.builder...` block? Land on: without
this, the two Delta config lines (`spark.sql.extensions` and
`spark.sql.catalog.spark_catalog`) would need to be copy-pasted into all
twelve scripts, and if one script's copy drifts or gets forgotten, that
script silently can't read/write Delta tables correctly. One function, one
place to fix it.

**Checkpoint:** `config.py` is done. Nothing runs yet — it has no
`if __name__ == "__main__":` block, on purpose. It's a library every other
script imports.

---

## 5. `data_generation/generate_ecommerce_data.py` — build the raw data

This script plays the role of "the upstream systems a real company already
has" — a checkout service's order logs, a clickstream collector's event
logs. Real pipelines don't write this script; it exists only because we
need something realistic to ingest.

### 5a. Reference data (Faker, on the driver)

Create `data_generation/generate_ecommerce_data.py`:

```python
"""
Enterprise Project 1 - synthetic data generator.

Run with: python generate_ecommerce_data.py

Two different generation strategies on purpose, to make a teaching point:

  - users/products are "reference data" - small enough to build with plain
    Faker on the driver, then hand to Spark as a DataFrame.
  - clickstream sessions are the "fact" data that has to scale to hundreds
    of millions of rows in production, so they're built entirely with Spark
    SQL functions (rand(), when(), arrays, explode) - vectorized, no Python
    UDF, no row-by-row work. That's what makes it possible to point
    config.SCALE at FULL_SCALE and run the exact same code on a cluster.

Output lands in RAW_PATH as CSV/JSON - deliberately messy-shaped, like real
upstream systems - for the bronze layer to ingest.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, RAW_PATH, SCALE

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Headphones", "Televisions", "Cameras"],
    "Clothing": ["Men", "Women", "Kids", "Footwear", "Accessories"],
    "Home & Kitchen": ["Cookware", "Furniture", "Decor", "Appliances"],
    "Books": ["Fiction", "Non-Fiction", "Academic", "Comics"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Cycling"],
    "Beauty": ["Skincare", "Makeup", "Haircare", "Fragrance"],
    "Grocery": ["Staples", "Snacks", "Beverages", "Organic"],
}

CITIES = [
    ("Mumbai", "Maharashtra"), ("Delhi", "Delhi"), ("Bangalore", "Karnataka"),
    ("Hyderabad", "Telangana"), ("Chennai", "Tamil Nadu"), ("Kolkata", "West Bengal"),
    ("Pune", "Maharashtra"), ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"), ("Kochi", "Kerala"), ("Indore", "Madhya Pradesh"),
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]
```

Point at the `sys.path.insert(...)` line specifically — this is the same
first three lines every script in the project will start with. **Say it
once, clearly:** "This makes `config.py`, which lives in the project root,
importable from a script sitting one folder down, like `bronze/`. Without
it, `from config import ...` fails because Python doesn't know to look one
directory up."

Now add the users/products generator:

```python
def generate_reference_data(spark):
    """Users and products - built with Faker on the driver (small enough)."""
    from faker import Faker

    fake = Faker("en_IN")
    Faker.seed(42)

    num_users = SCALE["num_users"]
    num_products = SCALE["num_products"]

    print(f"Generating {num_users:,} users with Faker...")
    users_rows = []
    for i in range(1, num_users + 1):
        city, state = CITIES[i % len(CITIES)]
        users_rows.append((
            f"USR{i:08d}",
            fake.name(),
            fake.free_email(),
            city,
            state,
            fake.date_between(start_date="-3y", end_date="today").isoformat(),
            DEVICE_TYPES[i % len(DEVICE_TYPES)],
        ))

    users_schema = StructType([
        StructField("user_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("signup_date", StringType()),
        StructField("preferred_device", StringType()),
    ])
    users_df = spark.createDataFrame(users_rows, schema=users_schema)
    users_df.write.mode("overwrite").option("header", True).csv(f"{RAW_PATH}/users")
    print(f"  wrote users -> {RAW_PATH}/users")

    print(f"Generating {num_products:,} products...")
    category_list = list(CATEGORIES.items())
    products_rows = []
    for i in range(1, num_products + 1):
        category, sub_categories = category_list[i % len(category_list)]
        sub_category = sub_categories[i % len(sub_categories)]
        base_price = round(fake.random.uniform(99, 89999), 2)
        products_rows.append((
            f"PROD{i:07d}",
            f"{sub_category} {fake.word().capitalize()} {i}",
            category,
            sub_category,
            fake.company(),
            base_price,
        ))

    products_schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("sub_category", StringType()),
        StructField("brand", StringType()),
        StructField("list_price", StringType()),
    ])
    products_df = spark.createDataFrame(products_rows, schema=products_schema)
    products_df.write.mode("overwrite").option("header", True).csv(f"{RAW_PATH}/products")
    print(f"  wrote products -> {RAW_PATH}/products")

    return num_users, num_products
```

**Ask:** "This is a plain Python `for` loop appending to a list, up to
200,000 times. Would this still work if `SCALE` pointed at `FULL_SCALE` —
40 million users?" Land on: technically yes, but painfully slowly, and all
on the driver — this is deliberately the *small* reference data (users,
products), which is realistically bounded even at real scale. Contrast with
the next function, which is the *fact* data and cannot be built this way.

### 5b. The fact data — sessions, events, orders (pure Spark, no loop)

This is the part worth slowing down for. Add:

```python
def generate_clickstream_and_orders(spark, num_users, num_products):
    """
    The fact table. Built entirely in Spark so it scales - no Python loop
    touches a single row here.

    Funnel per session (drop-off rates chosen to produce realistic-looking
    revenue-leakage patterns for the gold layer to detect later):
      view            100%  - every session views >=1 product
      add_to_cart      35%  of sessions
      checkout_start   55%  of the carts that were added   (-> 45% cart abandonment)
      payment_attempt  85%  of checkouts started
      payment success  90%  of payment attempts            (-> 10% failed payments)
      return           7%   of successful purchases, a few days later
    """
    num_sessions = SCALE["num_sessions"]
    print(f"Generating {num_sessions:,} sessions (Spark-native, vectorized)...")

    sessions = spark.range(0, num_sessions).withColumnRenamed("id", "session_seq")

    sessions = (
        sessions
        .withColumn("session_id", F.concat(F.lit("SESS"), F.lpad(F.col("session_seq").cast("string"), 10, "0")))
        .withColumn("user_id", F.concat(F.lit("USR"), F.lpad((F.rand(seed=1) * num_users).cast("int").cast("string"), 8, "0")))
        .withColumn("primary_product_id", F.concat(F.lit("PROD"), F.lpad((F.rand(seed=2) * num_products).cast("int").cast("string"), 7, "0")))
        .withColumn("device_type", F.element_at(F.array(*[F.lit(d) for d in DEVICE_TYPES]), (F.rand(seed=3) * len(DEVICE_TYPES)).cast("int") + 1))
        .withColumn("session_start_ts", F.expr("timestampadd(SECOND, cast(-rand(4) * 86400 * 90 as int), current_timestamp())"))
        .withColumn("cart_roll", F.rand(seed=5))
        .withColumn("checkout_roll", F.rand(seed=6))
        .withColumn("payment_roll", F.rand(seed=7))
        .withColumn("purchase_roll", F.rand(seed=8))
        .withColumn("return_roll", F.rand(seed=9))
        .withColumn("reached_cart", F.col("cart_roll") < 0.35)
        .withColumn("reached_checkout", F.col("reached_cart") & (F.col("checkout_roll") < 0.55))
        .withColumn("reached_payment", F.col("reached_checkout") & (F.col("payment_roll") < 0.85))
        .withColumn("payment_success", F.col("reached_payment") & (F.col("purchase_roll") < 0.90))
        .withColumn("is_returned", F.col("payment_success") & (F.col("return_roll") < 0.07))
        .withColumn("payment_method", F.element_at(F.array(*[F.lit(p) for p in PAYMENT_METHODS]), (F.rand(seed=10) * len(PAYMENT_METHODS)).cast("int") + 1))
    )
```

**Say:** "`spark.range(0, num_sessions)` is the whole trick — it hands
Spark a number, and Spark itself builds a distributed sequence of that many
rows, split across partitions from the start. There is no Python list of a
million and a half elements sitting on the driver anywhere in this
function." Point at `F.rand(seed=N)` — a different seed per column so the
random rolls aren't correlated with each other.

Ask before revealing: "35% add-to-cart, 55% of *those* start checkout — what
fraction of all sessions reach checkout?" (0.35 × 0.55 ≈ 19%). Hold onto
that number — it resurfaces later in the revenue-leakage report as an
actual dollar figure.

Now the funnel explosion — one events row becomes up to five clickstream
rows:

```python
    # ---- Explode the funnel into individual clickstream events ----------
    event_struct = F.struct(
        F.col("event_type"), F.col("event_ts"), F.col("product_id"), F.col("payment_status")
    )

    events = (
        sessions
        .withColumn("view_ts", F.col("session_start_ts"))
        .withColumn("cart_ts", F.expr("timestampadd(SECOND, cast(rand(11)*120 as int), session_start_ts)"))
        .withColumn("checkout_ts", F.expr("timestampadd(SECOND, cast(rand(12)*180+120 as int), session_start_ts)"))
        .withColumn("payment_ts", F.expr("timestampadd(SECOND, cast(rand(13)*60+300 as int), session_start_ts)"))
        .withColumn(
            "event_list",
            F.array(
                F.struct(F.lit("view").alias("event_type"), F.col("view_ts").alias("event_ts"),
                         F.col("primary_product_id").alias("product_id"), F.lit(None).cast(StringType()).alias("payment_status")),
                F.when(F.col("reached_cart"),
                       F.struct(F.lit("add_to_cart").alias("event_type"), F.col("cart_ts").alias("event_ts"),
                                F.col("primary_product_id").alias("product_id"), F.lit(None).cast(StringType()).alias("payment_status"))),
                F.when(F.col("reached_checkout"),
                       F.struct(F.lit("checkout_start").alias("event_type"), F.col("checkout_ts").alias("event_ts"),
                                F.col("primary_product_id").alias("product_id"), F.lit(None).cast(StringType()).alias("payment_status"))),
                F.when(F.col("reached_payment"),
                       F.struct(F.lit("payment_attempt").alias("event_type"), F.col("payment_ts").alias("event_ts"),
                                F.col("primary_product_id").alias("product_id"),
                                F.when(F.col("payment_success"), F.lit("success")).otherwise(F.lit("failed")).alias("payment_status"))),
                F.when(F.col("payment_success"),
                       F.struct(F.lit("purchase").alias("event_type"), F.col("payment_ts").alias("event_ts"),
                                F.col("primary_product_id").alias("product_id"), F.lit("success").alias("payment_status"))),
            ),
        )
        .withColumn("event_list", F.filter("event_list", lambda x: x.isNotNull()))
        .withColumn("event", F.explode("event_list"))
        .select(
            F.concat(F.col("session_id"), F.lit("-"), F.col("event.event_type")).alias("event_id"),
            "session_id", "user_id", "device_type",
            F.col("event.event_type").alias("event_type"),
            F.col("event.event_ts").alias("event_timestamp"),
            F.col("event.product_id").alias("product_id"),
            F.col("event.payment_status").alias("payment_status"),
        )
    )

    events_out = f"{RAW_PATH}/clickstream"
    (
        events
        .withColumn("event_date", F.to_date("event_timestamp"))
        .write.mode("overwrite").partitionBy("event_date").json(events_out)
    )
    approx_events = events.count()
    print(f"  wrote ~{approx_events:,} clickstream events -> {events_out}")
```

**Explain the shape, not just the syntax:** each session builds an *array*
of up to 5 possible events, `None` for stages it never reached, filters the
`None`s out, then `explode()`s that array so one session row becomes
1-to-5 output rows — one per funnel stage actually reached. This is why a
`browse_only` session produces exactly 1 row and a `purchased` session
produces 5.

Finally, the orders table:

```python
    # ---- Orders table: one row per successful purchase -------------------
    orders = (
        sessions.filter("payment_success")
        .withColumn("order_id", F.concat(F.lit("ORD"), F.lpad(F.col("session_seq").cast("string"), 10, "0")))
        .withColumn("quantity", (F.rand(seed=14) * 4 + 1).cast("int"))
        .withColumn("unit_price", F.round(F.rand(seed=15) * 50000 + 99, 2))
        .withColumn("discount_pct", F.round(F.rand(seed=16) * 0.3, 2))
        .withColumn(
            "order_status",
            F.when(F.col("is_returned"), F.lit("returned")).otherwise(F.lit("completed")),
        )
        .withColumn(
            "order_timestamp",
            F.expr("timestampadd(SECOND, cast(rand(17)*60+300 as int), session_start_ts)"),
        )
        .withColumn(
            "return_timestamp",
            F.when(F.col("is_returned"),
                   F.expr("timestampadd(DAY, cast(rand(18)*10+1 as int), order_timestamp)")),
        )
        .select(
            "order_id", "session_id", "user_id",
            F.col("primary_product_id").alias("product_id"),
            "order_timestamp", "quantity", "unit_price", "discount_pct",
            "payment_method", "order_status", "return_timestamp",
        )
    )

    orders_out = f"{RAW_PATH}/orders"
    orders.write.mode("overwrite").option("header", True).csv(orders_out)
    num_orders = orders.count()
    print(f"  wrote {num_orders:,} orders -> {orders_out}")
```

Close the file:

```python
if __name__ == "__main__":
    spark = get_spark("EcomProject_DataGeneration")
    print(f"Run mode scale: {SCALE}\n")

    num_users, num_products = generate_reference_data(spark)
    generate_clickstream_and_orders(spark, num_users, num_products)

    print("\nDone. Raw data is now sitting where a real upstream system would")
    print("drop it - ready for 01_ingest_to_bronze.py to pick up.")
    spark.stop()
```

**Run it now** so students see real output before moving on:

```powershell
python data_generation\generate_ecommerce_data.py
```

If it's the first run of the session and you want a fast sanity check
instead of the full multi-minute demo-scale run:

```powershell
$env:ECOM_TINY_RUN=1
python data_generation\generate_ecommerce_data.py
```

**Checkpoint:** a `data/raw/` folder now exists with `users/`, `products/`
(CSV) and `clickstream/` (JSON, partitioned by date), plus `orders/` (CSV).
This is exactly what a real upstream system would have handed you.

---

## 6. `bronze/01_ingest_to_bronze.py` — land raw files as Delta, untouched

The whole teaching point of Bronze: **almost no logic**. Build it in one
pass, then spend the discussion time on *why* it's this thin.

```python
"""
Bronze layer: land raw files as Delta tables, unchanged except for
provenance columns. No business logic here on purpose - Bronze exists so
that if Silver/Gold logic turns out wrong, you can re-derive everything
without going back to the source systems (which, in production, may not
even have the data anymore by the time you notice the bug).

Run with: python 01_ingest_to_bronze.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, RAW_PATH, BRONZE_PATH
from pyspark.sql import functions as F


def land_as_bronze(spark, source_path, target_table, reader):
    df = reader(spark, source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )
    target = f"{BRONZE_PATH}/{target_table}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"  {target_table}: {df.count():,} rows -> {target}")


def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def read_json(spark, path):
    return spark.read.json(path)


if __name__ == "__main__":
    spark = get_spark("EcomProject_BronzeIngest")

    print("Ingesting raw -> bronze (Delta):")
    land_as_bronze(spark, f"{RAW_PATH}/users", "users", read_csv)
    land_as_bronze(spark, f"{RAW_PATH}/products", "products", read_csv)
    land_as_bronze(spark, f"{RAW_PATH}/orders", "orders", read_csv)
    land_as_bronze(spark, f"{RAW_PATH}/clickstream", "clickstream", read_json)

    print("\nBronze tables are append-only history of what landed and when.")
    print("Nothing here is deduplicated or cleaned - that's Silver's job.")
    spark.stop()
```

**Point at the two added columns:** `_ingested_at` and `_source_file`. Ask:
"we're about to clean this exact data in the very next script — why keep
these two extra columns at all?" Land on: audit trail. If a number in a
report is ever wrong, these two columns are the difference between
answering "which file, ingested when" in five minutes versus not being able
to answer it at all.

**Run it:**

```powershell
python bronze\01_ingest_to_bronze.py
```

**Checkpoint:** `data/bronze/` now has `users`, `products`, `orders`,
`clickstream` as Delta tables (each is a folder containing Parquet files
plus a `_delta_log/` folder — open one in a file explorer and point at
`_delta_log` now, it becomes important in the next step).

---

## 7. `silver/02_bronze_to_silver.py` — make it trustworthy

This is the longest script and does four distinct jobs. Build it job by
job so students never lose track of which block solves which problem.

### 7a. Dedup + the upsert helper

```python
"""
Bronze -> Silver: the "make it trustworthy" layer.

  - dedup (bronze is append-only, so the same natural key can appear
    more than once across ingestion runs - keep the latest)
  - null/type handling on the columns downstream logic depends on
  - referential integrity: drop fact rows pointing at a user/product
    that doesn't exist (orphaned FKs happen constantly with real
    upstream systems - clickstream fired before the user record synced,
    a product delisted mid-session, etc.)
  - session stitching: group raw clickstream events into one row per
    session, which is what the Gold RFM/segmentation logic actually
    consumes

Dimension tables (users, products) are upserted with a Delta MERGE
instead of overwritten, on purpose - this is the "Delta Lake upserts in
a production context" learning outcome. Re-run this script after
changing a user's city in the raw data and then run:

    SELECT * FROM delta.`data/silver/users` VERSION AS OF 0

to show time travel recovering the pre-change row.

Run with: python 02_bronze_to_silver.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, BRONZE_PATH, SILVER_PATH
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable


def dedup_latest(df, key_cols, order_col="_ingested_at"):
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


def upsert_dimension(spark, updates_df, target_path, key_col):
    """MERGE upsert into a Delta dimension table - insert new keys, update changed ones."""
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(updates_df.alias("s"), f"t.{key_col} = s.{key_col}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        updates_df.write.format("delta").mode("overwrite").save(target_path)
    print(f"  upserted -> {target_path}")
```

**Explain `dedup_latest`:** "This is a window function, not a `DISTINCT`.
We partition by the natural key — say, `user_id` — order every row with
that key by `_ingested_at` descending, number them, and keep only row
number 1. Same `user_id` landed three times because we ran ingest three
times? We keep the newest one." This is *why* Bronze's `_ingested_at`
column from the last script matters — Silver depends on it directly.

### 7b. Dimension tables — users, products

```python
def build_silver_users(spark):
    users = spark.read.format("delta").load(f"{BRONZE_PATH}/users")
    users = dedup_latest(users, ["user_id"])
    users = (
        users
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("signup_date", F.to_date("signup_date"))
        .filter(F.col("user_id").isNotNull())
        .select("user_id", "name", "email", "city", "state", "signup_date", "preferred_device")
    )
    upsert_dimension(spark, users, f"{SILVER_PATH}/users", "user_id")
    return users


def build_silver_products(spark):
    products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
    products = dedup_latest(products, ["product_id"])
    products = (
        products
        .withColumn("list_price", F.col("list_price").cast("double"))
        .withColumn("category", F.trim(F.col("category")))
        .filter(F.col("product_id").isNotNull() & F.col("list_price").isNotNull())
        .select("product_id", "product_name", "category", "sub_category", "brand", "list_price")
    )
    upsert_dimension(spark, products, f"{SILVER_PATH}/products", "product_id")
    return products
```

Call out: lowercase+trim the email, cast `signup_date` to a real date,
drop nulls on the key. "Nobody would ever argue these fixes are wrong —
that's what makes this Silver logic, not Gold logic."

### 7c. Fact tables — orders, clickstream (the broadcast join)

```python
def build_silver_orders(spark, users, products):
    orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
    orders = dedup_latest(orders, ["order_id"])

    orders = (
        orders
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_pct", F.col("discount_pct").cast("double"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("return_timestamp", F.to_timestamp("return_timestamp"))
        .filter(
            F.col("order_id").isNotNull()
            & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
            & F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
        )
        # referential integrity: broadcast the small dimension tables into the
        # join instead of shuffling the (much larger) fact table - this is the
        # standard fix for a large-fact/small-dimension join, and the first
        # thing to reach for before considering salting a skewed key.
        .join(F.broadcast(users.select("user_id")), "user_id", "left_semi")
        .join(F.broadcast(products.select("product_id")), "product_id", "left_semi")
        .withColumn(
            "net_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2),
        )
    )

    orders.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/orders")
    print(f"  orders: {orders.count():,} rows -> {SILVER_PATH}/orders")
    return orders


def build_silver_clickstream(spark, users, products):
    events = spark.read.format("delta").load(f"{BRONZE_PATH}/clickstream")
    events = dedup_latest(events, ["event_id"])

    events = (
        events
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .filter(
            F.col("session_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("event_timestamp").isNotNull()
        )
        .join(F.broadcast(users.select("user_id")), "user_id", "left_semi")
        .join(F.broadcast(products.select("product_id")), "product_id", "left_semi")
    )

    events.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/clickstream")
    print(f"  clickstream: {events.count():,} rows -> {SILVER_PATH}/clickstream")
    return events
```

**Slow down on `left_semi` + `broadcast`** — this is the highest-value five
minutes in the whole build. Two separate ideas stacked in one line:
- `left_semi` is a filter-only join: keep left-side (orders) rows that have
  a match on the right, but return **only the left side's columns**. We're
  not enriching orders with user data here — we're using the join purely to
  drop any order whose `user_id` doesn't exist in the clean users table.
- `F.broadcast(...)` is a performance hint: instead of shuffling both the
  giant orders table *and* the users table across the network to align
  matching keys, send a full copy of the small `users` table to every
  executor. The giant table never moves.

### 7d. Session stitching — the table Gold actually reads from

```python
FUNNEL_STAGES = ["view", "add_to_cart", "checkout_start", "payment_attempt", "purchase"]


def build_silver_sessions(spark, events):
    """Session stitching: one row per session, funnel stage + timing derived
    from the raw event stream. This is the table RFM and revenue-leakage
    queries in Gold actually read from."""
    funnel_rank = F.create_map(
        *[x for i, stage in enumerate(FUNNEL_STAGES, start=1) for x in (F.lit(stage), F.lit(i))]
    )
    ranked = events.withColumn("stage_rank", funnel_rank[F.col("event_type")])

    sessions = ranked.groupBy("session_id", "user_id").agg(
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.count("*").alias("num_events"),
        F.max("stage_rank").alias("max_stage_rank"),
        F.first("device_type").alias("device_type"),
        F.first("product_id").alias("first_product_id"),
        F.max(F.when(F.col("event_type") == "payment_attempt", F.col("payment_status"))).alias(
            "payment_status"
        ),
    )

    sessions = (
        sessions
        .withColumn(
            "funnel_stage_reached",
            F.when(F.col("max_stage_rank") == 5, "purchased")
             .when(F.col("max_stage_rank") == 4, "payment_attempted")
             .when(F.col("max_stage_rank") == 3, "checkout_started")
             .when(F.col("max_stage_rank") == 2, "cart_abandoned")
             .otherwise("browse_only"),
        )
        .withColumn(
            "session_duration_secs",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
        .drop("max_stage_rank")
    )

    sessions.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/sessions")
    print(f"  sessions: {sessions.count():,} rows -> {SILVER_PATH}/sessions")
    return sessions
```

**Explain:** raw clickstream is one row per *event*. Nobody downstream
wants to reason event-by-event — they want to reason about sessions. This
groups by `session_id`, finds the highest funnel stage each session reached
using the rank map, and labels the session accordingly. Both Gold scripts
you build next read `sessions`, not raw `clickstream`.

### 7e. Wire it together

```python
if __name__ == "__main__":
    spark = get_spark("EcomProject_SilverTransform")

    print("Building silver dimension tables (Delta MERGE upsert):")
    users = build_silver_users(spark)
    products = build_silver_products(spark)

    print("\nBuilding silver fact tables (dedup + referential integrity + broadcast join):")
    build_silver_orders(spark, users, products)
    events = build_silver_clickstream(spark, users, products)

    print("\nStitching clickstream events into sessions:")
    build_silver_sessions(spark, events)

    spark.stop()
```

**Run it, then run it again** to show Delta MERGE and time travel live:

```powershell
python silver\02_bronze_to_silver.py
python silver\02_bronze_to_silver.py
```

Then, in a `pyspark` shell (or a scratch `.py` file) with `config.get_spark`:

```python
spark.sql("DESCRIBE HISTORY delta.`data/silver/users`").show(truncate=False)
spark.read.format("delta").option("versionAsOf", 0).load("data/silver/users").show()
```

This is the moment to walk through `notes.md`'s Delta Lake section and the
"3am incident" framing from `student_script_3hr_detailed.md` (search that
file for "Delta Lake time travel") — don't rush it.

**Checkpoint:** `data/silver/` has `users`, `products`, `orders`,
`clickstream`, `sessions` as Delta tables.

---

## 8. `gold/03_silver_to_gold_rfm.py` — Customer 360 + RFM scoring

```python
"""
Silver -> Gold: Customer 360 with RFM scoring.

RFM = Recency (days since last order), Frequency (order count), Monetary
(net revenue) - one row per customer, scored 1-5 on each dimension with
ntile(), then combined into a segment label. This table is what the
K-Means step in ml/ engineers features from, and what a dashboard would
query directly.

Run with: python 03_silver_to_gold_rfm.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, SILVER_PATH, GOLD_PATH
from pyspark.sql import functions as F, Window


def build_customer_rfm(spark):
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    users = spark.read.format("delta").load(f"{SILVER_PATH}/users")

    # Monetary only counts completed orders - a returned order's revenue was
    # given back, so it should not make a customer look more valuable than
    # they are. This is exactly the kind of definition that has to be agreed
    # with the business stakeholder up front, not decided quietly in code.
    completed = orders.filter(F.col("order_status") == "completed")

    as_of_date = orders.agg(F.max("order_timestamp")).first()[0]

    per_customer = completed.groupBy("user_id").agg(
        F.datediff(F.lit(as_of_date), F.max("order_timestamp")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("net_amount"), 2).alias("monetary"),
    )

    # ntile(5) needs all rows in one partition per the (empty) window spec -
    # fine at this scale (one row per customer, not per event). At full
    # scale this still holds: the fact table is orders/events, not customers,
    # so this table stays small even at 40M users.
    r_window = Window.orderBy(F.col("recency_days").asc())
    f_window = Window.orderBy(F.col("frequency").desc())
    m_window = Window.orderBy(F.col("monetary").desc())

    scored = (
        per_customer
        .withColumn("r_score", F.ntile(5).over(r_window))
        .withColumn("f_score", F.ntile(5).over(f_window))
        .withColumn("m_score", F.ntile(5).over(m_window))
    )
    scored = scored.withColumn(
        "rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")
    )

    scored = scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 10, "Loyal Customers")
         .when(F.col("rfm_score") >= 7, "Potential Loyalists")
         .when(F.col("rfm_score") >= 5, "At Risk")
         .otherwise("Lost / Hibernating"),
    )

    customer_360 = (
        users.join(scored, "user_id", "left")
        .fillna({"recency_days": 9999, "frequency": 0, "monetary": 0.0, "rfm_score": 0})
        .fillna({"rfm_segment": "Never Purchased"})
        .select(
            "user_id", "name", "city", "state", "preferred_device",
            "recency_days", "frequency", "monetary",
            "r_score", "f_score", "m_score", "rfm_score", "rfm_segment",
        )
    )

    out = f"{GOLD_PATH}/customer_360"
    customer_360.write.format("delta").mode("overwrite").save(out)
    print(f"customer_360: {customer_360.count():,} rows -> {out}")

    print("\nSegment distribution:")
    customer_360.groupBy("rfm_segment").count().orderBy(F.col("count").desc()).show()

    return customer_360


def build_category_rollup(spark):
    """Daily revenue rollup per product category - what the BI dashboard's
    'Revenue by Category' tile reads from."""
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")

    rollup = (
        orders.filter(F.col("order_status") == "completed")
        .join(F.broadcast(products.select("product_id", "category", "sub_category")), "product_id")
        .withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date", "category", "sub_category")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
        )
    )

    out = f"{GOLD_PATH}/daily_category_rollup"
    rollup.write.format("delta").mode("overwrite").partitionBy("order_date").save(out)
    print(f"daily_category_rollup: {rollup.count():,} rows -> {out}")
    return rollup


if __name__ == "__main__":
    spark = get_spark("EcomProject_GoldRFM")

    build_customer_rfm(spark)
    print()
    build_category_rollup(spark)

    spark.stop()
```

**Two things to ask before running:**
1. "Why filter `order_status == 'completed'` before computing Monetary,
   given that we kept Frequency counting *all* placed orders including
   returned ones?" — this is the debatable Gold-layer decision, the exact
   example used in `student_script_3hr_detailed.md`'s architecture section.
2. "Why `F.ntile(5)` instead of a hardcoded rule like 'more than 10 orders
   = a 5'?" — land on: a hardcoded threshold breaks silently as the
   business grows; `ntile(5)` re-derives the top 20% fresh every run.

**Run it:**

```powershell
python gold\03_silver_to_gold_rfm.py
```

**Checkpoint:** `data/gold/customer_360` and `data/gold/daily_category_rollup`
exist. The segment distribution prints straight to the console.

---

## 9. `gold/04_revenue_leakage.py` — turn drop-off into dollars

```python
"""
Silver -> Gold: revenue leakage detection.

Three leakage patterns, all derivable from the sessions/orders tables
Silver already built:

  1. Abandoned carts   - session added a product to cart but never started
                          checkout. Lost revenue = what that product would
                          have been worth.
  2. Failed payments   - session reached payment_attempt but the payment
                          failed. Lost revenue = the attempted order value.
  3. Returns           - order completed, then reversed. Lost revenue =
                          the net_amount that was refunded.

Run with: python 04_revenue_leakage.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, SILVER_PATH, GOLD_PATH
from pyspark.sql import functions as F


def build_revenue_leakage(spark):
    sessions = spark.read.format("delta").load(f"{SILVER_PATH}/sessions")
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    sessions_priced = sessions.join(
        F.broadcast(products.select("product_id", "list_price").withColumnRenamed("product_id", "first_product_id")),
        "first_product_id",
        "left",
    )

    abandoned_carts = (
        sessions_priced.filter(F.col("funnel_stage_reached") == "cart_abandoned")
        .withColumn("event_date", F.to_date("session_start"))
        .groupBy("event_date")
        .agg(
            F.lit("abandoned_cart").alias("leakage_type"),
            F.count("*").alias("num_incidents"),
            F.round(F.sum("list_price"), 2).alias("estimated_revenue_lost"),
        )
    )

    failed_payments = (
        sessions_priced.filter(
            (F.col("funnel_stage_reached") == "payment_attempted")
            & (F.col("payment_status") == "failed")
        )
        .withColumn("event_date", F.to_date("session_start"))
        .groupBy("event_date")
        .agg(
            F.lit("failed_payment").alias("leakage_type"),
            F.count("*").alias("num_incidents"),
            F.round(F.sum("list_price"), 2).alias("estimated_revenue_lost"),
        )
    )

    returns = (
        orders.filter(F.col("order_status") == "returned")
        .withColumn("event_date", F.to_date("return_timestamp"))
        .groupBy("event_date")
        .agg(
            F.lit("return").alias("leakage_type"),
            F.count("*").alias("num_incidents"),
            F.round(F.sum("net_amount"), 2).alias("estimated_revenue_lost"),
        )
    )

    leakage = abandoned_carts.unionByName(failed_payments).unionByName(returns)

    out = f"{GOLD_PATH}/revenue_leakage_daily"
    leakage.write.format("delta").mode("overwrite").partitionBy("leakage_type").save(out)
    print(f"revenue_leakage_daily: {leakage.count():,} rows -> {out}")

    print("\nTotal estimated revenue lost by type:")
    leakage.groupBy("leakage_type").agg(
        F.sum("num_incidents").alias("total_incidents"),
        F.round(F.sum("estimated_revenue_lost"), 2).alias("total_revenue_lost"),
    ).orderBy(F.col("total_revenue_lost").desc()).show()

    return leakage


if __name__ == "__main__":
    spark = get_spark("EcomProject_GoldRevenueLeakage")
    build_revenue_leakage(spark)
    spark.stop()
```

**Say before running:** "Notice all three of these blocks read only from
tables Silver already built — no new raw data. This is the payoff of the
19% checkout-conversion number from the data generation step — watch the
`abandoned_cart` total that's about to print. That's the same drop-off,
except now it's a dollar figure, not a percentage on a whiteboard."

**Run it:**

```powershell
python gold\04_revenue_leakage.py
```

**Checkpoint:** `data/gold/revenue_leakage_daily` exists, partitioned by
`leakage_type`.

---

## 10. `ml/05_customer_segmentation_kmeans.py` — unsupervised segmentation

Frame this before typing a line: RFM segments (just built) are fixed
business rules anyone can read and argue with. K-Means finds groupings
from the data's own structure. Same customers, two different lenses.

```python
"""
Gold -> ML: unsupervised customer segmentation with K-Means.

This is deliberately a *different* segmentation from the rule-based RFM
segment in gold/03_silver_to_gold_rfm.py, and that contrast is the
teaching point: RFM segments are fixed business rules anyone can read and
argue with; K-Means finds groupings from the data's own structure, adding
behavioral features (session frequency, cart abandonment) that a simple
RFM rule doesn't consider. In a real deployment you'd show both to
stakeholders and reconcile the two.

Run with: python 05_customer_segmentation_kmeans.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, SILVER_PATH, GOLD_PATH
from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

FEATURE_COLS = [
    "recency_days", "frequency", "monetary",
    "num_sessions", "avg_session_duration_secs", "cart_abandonment_rate",
]


def build_feature_table(spark):
    customer_360 = spark.read.format("delta").load(f"{GOLD_PATH}/customer_360")
    sessions = spark.read.format("delta").load(f"{SILVER_PATH}/sessions")

    session_features = sessions.groupBy("user_id").agg(
        F.count("*").alias("num_sessions"),
        F.avg("session_duration_secs").alias("avg_session_duration_secs"),
        F.avg(F.when(F.col("funnel_stage_reached") == "cart_abandoned", 1.0).otherwise(0.0)).alias(
            "cart_abandonment_rate"
        ),
    )

    # only customers with at least one completed order - clustering "never
    # purchased" visitors together with buyers would just rediscover
    # "bought something vs. didn't", which RFM already tells us for free.
    features = (
        customer_360.filter(F.col("frequency") > 0)
        .join(session_features, "user_id", "left")
        .fillna(0.0, subset=["avg_session_duration_secs", "cart_abandonment_rate"])
    )
    return features


def pick_k_with_elbow(spark, scaled_df, k_range=range(2, 7)):
    """Print inertia (WSSSE) and silhouette per k - what you'd show the
    class to justify the chosen k rather than picking it out of thin air."""
    print("Elbow method - within-cluster sum of squared errors per k:")
    for k in k_range:
        km = KMeans(featuresCol="scaled_features", k=k, seed=42)
        model = km.fit(scaled_df)
        wssse = model.summary.trainingCost
        predictions = model.transform(scaled_df)
        evaluator = ClusteringEvaluator(featuresCol="scaled_features")
        silhouette = evaluator.evaluate(predictions)
        print(f"  k={k}: WSSSE={wssse:,.1f}  silhouette={silhouette:.3f}")


def run_kmeans(spark, features, k=4):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    assembled = assembler.transform(features)

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="scaled_features", withMean=True, withStd=True
    )
    scaled = scaler.fit(assembled).transform(assembled)

    pick_k_with_elbow(spark, scaled)

    kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=k, seed=42)
    model = kmeans.fit(scaled)
    clustered = model.transform(scaled)

    # Rank clusters by average monetary value so labels mean something
    # regardless of the arbitrary cluster ids K-Means assigns.
    cluster_ranks = (
        clustered.groupBy("cluster")
        .agg(F.avg("monetary").alias("avg_monetary"), F.avg("frequency").alias("avg_frequency"))
        .orderBy(F.col("avg_monetary").desc())
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("avg_monetary").desc())))
    )

    labels = ["High-Value Loyalists", "Growing Spenders", "Occasional Buyers", "Low-Engagement / Price-Sensitive"]
    label_col = F.array(*[F.lit(l) for l in labels])
    cluster_ranks = cluster_ranks.withColumn("segment_label", label_col[F.col("rank") - 1])

    print("\nCluster profile (ranked by average monetary value):")
    cluster_ranks.show(truncate=False)

    result = (
        clustered.join(cluster_ranks.select("cluster", "segment_label"), "cluster")
        .select("user_id", "cluster", "segment_label", *FEATURE_COLS)
    )

    out = f"{GOLD_PATH}/customer_segments_kmeans"
    result.write.format("delta").mode("overwrite").save(out)
    print(f"\ncustomer_segments_kmeans: {result.count():,} rows -> {out}")

    print("\nSegment sizes:")
    result.groupBy("segment_label").count().orderBy(F.col("count").desc()).show()

    return result


if __name__ == "__main__":
    spark = get_spark("EcomProject_KMeansSegmentation")
    features = build_feature_table(spark)
    run_kmeans(spark, features, k=4)
    spark.stop()
```

**Two concepts worth a pause:**
- `StandardScaler(..., withMean=True, withStd=True)` — K-Means measures
  distance; `monetary` (thousands of rupees) and `cart_abandonment_rate`
  (0–1) live on wildly different scales, so without scaling, monetary would
  dominate the distance calculation and the other features would barely
  matter.
- `pick_k_with_elbow` — this is how you justify a chosen `k` instead of
  guessing. Point at `notes.md` for the full elbow-method discussion.

**Run it:**

```powershell
python ml\05_customer_segmentation_kmeans.py
```

**Checkpoint:** `data/gold/customer_segments_kmeans` exists.

---

## 11. Streaming — Kafka producer + Structured Streaming consumer

This is the only part of the project that needs Docker. Skip this whole
section if the room doesn't have Docker Desktop.

### 11a. `docker-compose.yml`

Create in the project root:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

Say: "One broker, one Zookeeper — the smallest possible Kafka cluster,
enough to prove the streaming pattern without needing real infrastructure."

Start it:

```powershell
docker compose up -d
```

### 11b. `streaming/06_kafka_order_producer.py` — the live feed

```python
"""
Simulates the "live orders" feed a real checkout service would publish to
Kafka. Standalone Python (no Spark needed to produce) - this is meant to
run in one terminal while 07_structured_streaming_to_delta.py runs in
another, so the class can watch rows land in the Delta table in near
real-time as this script fires events.

Prerequisite: `docker compose up -d` from the project root (starts
Zookeeper + a single-broker Kafka on localhost:9092).

Run with: python 06_kafka_order_producer.py [--rate 2] [--count 200]
"""

import sys
import os
import json
import time
import random
import argparse
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_ORDERS_TOPIC

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]


def make_order_event():
    payment_success = random.random() > 0.10  # same 10% failure rate as the batch generator
    return {
        "order_id": f"LIVE{uuid.uuid4().hex[:12].upper()}",
        "user_id": f"USR{random.randint(1, 200_000):08d}",
        "product_id": f"PROD{random.randint(1, 5_000):07d}",
        "quantity": random.randint(1, 4),
        "unit_price": round(random.uniform(99, 50000), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": "success" if payment_success else "failed",
        "order_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing to '{KAFKA_ORDERS_TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS} at {args.rate}/sec ...")
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            event = make_order_event()
            producer.send(KAFKA_ORDERS_TOPIC, key=event["order_id"], value=event)
            sent += 1
            print(f"  sent {sent}: {event['order_id']} {event['payment_status']} Rs.{event['unit_price']}")
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"Total events sent: {sent}")


if __name__ == "__main__":
    main()
```

Note this one is plain Python — no Spark, no `config.get_spark()`. It only
needs `KAFKA_BOOTSTRAP_SERVERS`/`KAFKA_ORDERS_TOPIC` from `config.py`.

### 11c. `streaming/07_structured_streaming_to_delta.py` — the consumer

```python
"""
Structured Streaming: consume the live Kafka order feed and land it in
Delta, plus a live windowed aggregation so the class can watch numbers
move on screen while 06_kafka_order_producer.py is running.

Two queries run concurrently:
  1. raw landing  - every Kafka message, parsed, appended to a Bronze
                    Delta table (`bronze/streaming_orders`). Uses
                    foreachBatch + a Delta MERGE keyed on order_id, so
                    replaying the same Kafka offsets twice (e.g. after a
                    restart) doesn't duplicate rows - that idempotency is
                    the actual point of the exercise, not the schema.
  2. live metrics - a 30-second tumbling window of order count and
                    revenue, printed to the console, standing in for
                    what a real-time dashboard tile would show.

Prerequisite: `docker compose up -d`, and 06_kafka_order_producer.py
running in another terminal so there's something to consume.

Run with: python 07_structured_streaming_to_delta.py
Stop with Ctrl+C.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    get_spark, BRONZE_PATH, CHECKPOINT_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_ORDERS_TOPIC,
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)
from delta.tables import DeltaTable

ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("payment_status", StringType()),
    StructField("order_timestamp", StringType()),
])


def read_kafka_orders(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_ORDERS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(F.col("value").cast("string").alias("json_str"), "timestamp")
        .withColumn("data", F.from_json("json_str", ORDER_EVENT_SCHEMA))
        .select("data.*", F.col("timestamp").alias("kafka_ingest_ts"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("net_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )
    return parsed


def upsert_batch(target_path):
    """Returns a foreachBatch function that MERGEs each micro-batch into a
    Delta table on order_id - the streaming equivalent of the dimension
    upsert in silver/02_bronze_to_silver.py, and what makes re-processing
    safe after a checkpoint restart."""

    def _merge(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        spark = batch_df.sparkSession
        if DeltaTable.isDeltaTable(spark, target_path):
            target = DeltaTable.forPath(spark, target_path)
            (
                target.alias("t")
                .merge(batch_df.alias("s"), "t.order_id = s.order_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            batch_df.write.format("delta").mode("overwrite").save(target_path)
        print(f"  [batch {batch_id}] merged {batch_df.count()} live orders")

    return _merge


def run(spark):
    orders_stream = read_kafka_orders(spark)
    target_path = f"{BRONZE_PATH}/streaming_orders"

    landing_query = (
        orders_stream.writeStream
        .foreachBatch(upsert_batch(target_path))
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/streaming_orders")
        .outputMode("update")
        .start()
    )

    windowed = (
        orders_stream
        .withWatermark("kafka_ingest_ts", "1 minute")
        .groupBy(F.window("kafka_ingest_ts", "30 seconds"))
        .agg(
            F.count("*").alias("orders_in_window"),
            F.round(F.sum("net_amount"), 2).alias("revenue_in_window"),
            F.round(F.avg(F.when(F.col("payment_status") == "failed", 1.0).otherwise(0.0)), 3).alias(
                "failure_rate"
            ),
        )
        .orderBy("window")
    )

    metrics_query = (
        windowed.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/streaming_metrics")
        .start()
    )

    print(f"Streaming from '{KAFKA_ORDERS_TOPIC}' -> {target_path}")
    print("Watching for live orders... (Ctrl+C to stop)\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark = get_spark(
        "EcomProject_StructuredStreaming",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
    )
    try:
        run(spark)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        spark.stop()
```

Point at `extra_packages=[...]` in the `get_spark(...)` call — this is
exactly the parameter you added to `config.py` back in step 4c, used for
the first time here to pull in the Kafka connector jar.

**Run both, in two terminals** (venv active in both):

```powershell
# Terminal A
python streaming\06_kafka_order_producer.py --rate 3

# Terminal B
python streaming\07_structured_streaming_to_delta.py
```

**Ask before starting:** "if this job crashes and restarts from its
checkpoint without the MERGE — just a plain append instead — what happens
to this table?" Land on: duplicate rows, because Structured Streaming
replays some data after a restart to guarantee nothing was missed; MERGE is
what makes that replay safe.

**Checkpoint:** watch terminal B's console print windowed metrics updating
every batch. Stop both with Ctrl+C when done, then `docker compose down` if
wrapping up for the day.

---

## 12. `exports/08_export_gold_for_bi.py` — the last mile to BI tools

```python
"""
Gold -> BI export. Dashboards (Power BI / Tableau) don't read Delta well
out of the box, so the last mile is a plain Parquet + CSV drop of each
Gold table - CSV for anyone who just wants to open it in Excel, Parquet
for the BI tool's native connector.

Run with: python 08_export_gold_for_bi.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, GOLD_PATH, BASE_PATH

EXPORT_TABLES = ["customer_360", "daily_category_rollup", "revenue_leakage_daily", "customer_segments_kmeans"]


def export_table(spark, table_name):
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{table_name}")
    export_root = f"{BASE_PATH}/exports/{table_name}"

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{export_root}_csv")
    df.write.mode("overwrite").parquet(f"{export_root}_parquet")
    print(f"  {table_name}: {df.count():,} rows -> {export_root}_{{csv,parquet}}")


if __name__ == "__main__":
    spark = get_spark("EcomProject_ExportForBI")

    print("Exporting gold tables for Power BI / Tableau:")
    for table in EXPORT_TABLES:
        export_table(spark, table)

    spark.stop()
```

**Point at `.coalesce(1)`** on the CSV write only, not the Parquet write.
Ask: "why coalesce only for CSV?" Land on: a human opening this in Excel
expects one file; a BI tool's Parquet connector is happy reading many part
files in parallel, so there's no reason to force it down to one and lose
the parallelism.

**Run it:**

```powershell
python exports\08_export_gold_for_bi.py
```

**Checkpoint:** `data/exports/` now has four `*_csv` and four `*_parquet`
folders.

---

## 13. `dashboard/app.py` — a quick in-browser look

This one is intentionally **not** a Spark script — plain pandas + Streamlit
reading the CSVs the previous step just wrote.

```python
"""
BI dashboard for the Gold-layer exports produced by
exports/08_export_gold_for_bi.py.

This is NOT part of the Spark pipeline - it's a plain Streamlit app that
reads the CSVs the pipeline already wrote to data/exports/*_csv/ and
renders them as charts. Run the pipeline (through the export step) first,
then launch this.

Run with: streamlit run dashboard/app.py
"""

import glob
import os

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "exports")

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")


@st.cache_data
def load_table(table_name: str) -> pd.DataFrame:
    """Spark's coalesce(1).write.csv() drops one part-*.csv file inside a
    folder named after the table - glob for it since the exact filename
    is randomly generated."""
    pattern = os.path.join(EXPORTS_DIR, f"{table_name}_csv", "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()
    return pd.read_csv(files[0])


st.title("E-Commerce Customer Behaviour & Revenue Analytics")

customer_360 = load_table("customer_360")
category_rollup = load_table("daily_category_rollup")
revenue_leakage = load_table("revenue_leakage_daily")
kmeans_segments = load_table("customer_segments_kmeans")

if customer_360.empty and category_rollup.empty and revenue_leakage.empty and kmeans_segments.empty:
    st.error(
        f"No exported data found under `{EXPORTS_DIR}`.\n\n"
        "Run the pipeline first, ending with:\n\n"
        "```\npython exports/08_export_gold_for_bi.py\n```"
    )
    st.stop()

# ---- KPI row --------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Customers", f"{len(customer_360):,}")
col2.metric("Total Revenue", f"${customer_360['monetary'].sum():,.0f}" if not customer_360.empty else "-")
col3.metric(
    "Revenue Lost (leakage)",
    f"${revenue_leakage['estimated_revenue_lost'].sum():,.0f}" if not revenue_leakage.empty else "-",
)
col4.metric("Champions", f"{(customer_360['rfm_segment'] == 'Champions').sum():,}" if not customer_360.empty else "-")

tab1, tab2, tab3, tab4 = st.tabs(
    ["RFM Segments", "Revenue by Category", "Revenue Leakage", "K-Means Segments"]
)

with tab1:
    st.subheader("Customer distribution by RFM segment")
    if not customer_360.empty:
        seg_counts = customer_360["rfm_segment"].value_counts().reset_index()
        seg_counts.columns = ["rfm_segment", "count"]
        fig = px.bar(seg_counts, x="rfm_segment", y="count", color="rfm_segment")
        st.plotly_chart(fig, width='stretch')

        st.subheader("Customer 360 (filterable)")
        segment_filter = st.multiselect(
            "Filter by segment", options=sorted(customer_360["rfm_segment"].unique())
        )
        filtered = (
            customer_360[customer_360["rfm_segment"].isin(segment_filter)]
            if segment_filter
            else customer_360
        )
        st.dataframe(filtered, width='stretch')
    else:
        st.info("customer_360 export not found.")

with tab2:
    st.subheader("Daily revenue by product category")
    if not category_rollup.empty:
        category_rollup["order_date"] = pd.to_datetime(category_rollup["order_date"])
        daily_by_cat = category_rollup.groupby(["order_date", "category"], as_index=False)["revenue"].sum()
        fig = px.line(daily_by_cat, x="order_date", y="revenue", color="category")
        st.plotly_chart(fig, width='stretch')

        st.subheader("Total revenue by category")
        totals = category_rollup.groupby("category", as_index=False)["revenue"].sum().sort_values(
            "revenue", ascending=False
        )
        fig2 = px.bar(totals, x="category", y="revenue")
        st.plotly_chart(fig2, width='stretch')
    else:
        st.info("daily_category_rollup export not found.")

with tab3:
    st.subheader("Revenue leakage by type")
    if not revenue_leakage.empty:
        totals = revenue_leakage.groupby("leakage_type", as_index=False)[
            ["num_incidents", "estimated_revenue_lost"]
        ].sum()
        fig = px.bar(totals, x="leakage_type", y="estimated_revenue_lost", color="leakage_type")
        st.plotly_chart(fig, width='stretch')

        revenue_leakage["event_date"] = pd.to_datetime(revenue_leakage["event_date"])
        trend = revenue_leakage.groupby(["event_date", "leakage_type"], as_index=False)[
            "estimated_revenue_lost"
        ].sum()
        fig2 = px.line(trend, x="event_date", y="estimated_revenue_lost", color="leakage_type")
        st.plotly_chart(fig2, width='stretch')
    else:
        st.info("revenue_leakage_daily export not found.")

with tab4:
    st.subheader("K-Means customer segments (vs. RFM, side by side)")
    if not kmeans_segments.empty:
        seg_counts = kmeans_segments["segment_label"].value_counts().reset_index()
        seg_counts.columns = ["segment_label", "count"]
        fig = px.bar(seg_counts, x="segment_label", y="count", color="segment_label")
        st.plotly_chart(fig, width='stretch')

        fig2 = px.scatter(
            kmeans_segments,
            x="frequency",
            y="monetary",
            color="segment_label",
            hover_data=["user_id", "recency_days", "cart_abandonment_rate"],
        )
        st.plotly_chart(fig2, width='stretch')
        st.dataframe(kmeans_segments, width='stretch')
    else:
        st.info("customer_segments_kmeans export not found.")
```

Point out `@st.cache_data` and the `glob.glob(...)` in `load_table` —
Spark's `coalesce(1).write.csv()` writes into a *folder* named after the
table with an auto-generated filename inside (`part-00000-<uuid>.csv`), not
a single named file — that's why this can't just do
`pd.read_csv("customer_360.csv")`.

**Run it:**

```powershell
streamlit run dashboard\app.py
```

A browser tab opens automatically. Walk through the four tabs.

---

## 14. `airflow/dags/ecommerce_pipeline_dag.py` — orchestration (walkthrough, not a live run)

A full Airflow install is its own setup problem — build this file so
students see the shape of production orchestration, without necessarily
running it live (see `student_script_3hr_detailed.md`'s "Airflow DAG
walkthrough" section for the four discussion points to hit).

```python
"""
Orchestrates the daily batch pipeline: bronze ingest -> silver transform
-> gold (RFM + revenue leakage) -> ML segmentation -> BI export.

This DAG does NOT touch the Kafka/Structured Streaming path - that's a
long-running job Airflow doesn't manage well (it's not a "runs and
finishes" task, it's a "runs forever" service). In production it would be
its own always-on Spark application, started once and monitored
separately, while Airflow owns the recurring batch side shown here.

Each task is a plain spark-submit BashOperator, deliberately - a
SparkSubmitOperator (from apache-airflow-providers-apache-spark) is the
more idiomatic choice on a real Airflow deployment with a configured
Spark connection, but BashOperator keeps this DAG runnable by anyone who
just has `spark-submit` on PATH, which is what this classroom setup has
via 00-environment-setup.

Drop this file into $AIRFLOW_HOME/dags/ to schedule it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/enterprise-projects/project-1-ecommerce-analytics"
SPARK_SUBMIT = "spark-submit --master local[*]"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_customer_analytics_daily",
    description="Bronze -> Silver -> Gold -> ML -> BI export for the e-commerce analytics platform",
    default_args=default_args,
    schedule="0 2 * * *",  # 2 AM daily, after the previous day's data has fully landed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["enterprise-project-1", "ecommerce", "analytics"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/bronze/01_ingest_to_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/silver/02_bronze_to_silver.py",
    )

    build_gold_rfm = BashOperator(
        task_id="build_gold_rfm_and_rollups",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/gold/03_silver_to_gold_rfm.py",
    )

    detect_revenue_leakage = BashOperator(
        task_id="detect_revenue_leakage",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/gold/04_revenue_leakage.py",
    )

    run_customer_segmentation = BashOperator(
        task_id="run_kmeans_segmentation",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/ml/05_customer_segmentation_kmeans.py",
    )

    export_for_bi = BashOperator(
        task_id="export_gold_for_bi",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/exports/08_export_gold_for_bi.py",
    )

    # gold_rfm and revenue_leakage both only need silver, and don't depend
    # on each other - fan them out in parallel rather than chaining them.
    ingest_bronze >> transform_silver >> [build_gold_rfm, detect_revenue_leakage]
    build_gold_rfm >> run_customer_segmentation
    [run_customer_segmentation, detect_revenue_leakage] >> export_for_bi
```

**The four discussion points, briefly** (full script in
`student_script_3hr_detailed.md`):
1. `>> [build_gold_rfm, detect_revenue_leakage]` is a fan-out — both only
   need Silver and don't depend on each other, so they run in parallel.
2. `catchup=False` — without it, turning this DAG on for the first time
   tries to backfill every scheduled run since `start_date`, which for a
   months-old DAG could mean months of backfills firing at once.
3. `retries=2, retry_delay=timedelta(minutes=5)` — a delayed retry, not an
   instant one, because transient failures (an OOM executor, a flaky HDFS
   read) usually need the cluster a minute to recover.
4. Streaming is deliberately **not** in this DAG — a Structured Streaming
   query runs forever, and Airflow is built around tasks that start and
   finish.

---

## 15. Run the whole thing end-to-end, in order

Once every file above exists, the full pipeline is:

```powershell
# 1. Generate synthetic raw data
python data_generation\generate_ecommerce_data.py

# 2. Batch pipeline, strictly in this order
python bronze\01_ingest_to_bronze.py
python silver\02_bronze_to_silver.py
python gold\03_silver_to_gold_rfm.py
python gold\04_revenue_leakage.py
python ml\05_customer_segmentation_kmeans.py
python exports\08_export_gold_for_bi.py

# 3. View results in a browser
streamlit run dashboard\app.py

# 4. Streaming demo (optional, needs Docker)
docker compose up -d
python streaming\06_kafka_order_producer.py --rate 3     # terminal A
python streaming\07_structured_streaming_to_delta.py     # terminal B
```

**Ask the class to notice the order is not arbitrary** — it's the exact
dependency chain drawn in the architecture diagram in `README.md`: each
script only reads tables the previous script already wrote.

---

## Where to go next

- Re-run the pipeline with `ECOM_TINY_RUN=1` set, so students can iterate
  fast on their own machines without waiting minutes per script.
- Break students into pairs and have each pair own one script — have them
  explain their script back to the room. That's a stronger retention check
  than watching one build-along end to end.
- Once the code exists, switch to `student_script_3hr_detailed.md` for the
  narrated "why" discussion on each layer, and `notes.md` for the deeper
  Delta Lake / skew / RFM / K-Means concepts.
- Read `README.md`'s "Scale: demo vs. production" section together to close
  the loop: this exact code, unchanged, is what runs at the syllabus's
  50GB/500M-row scale on a real cluster.
