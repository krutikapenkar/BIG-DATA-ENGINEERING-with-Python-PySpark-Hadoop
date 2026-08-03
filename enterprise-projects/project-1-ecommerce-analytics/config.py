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
