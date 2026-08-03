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


if __name__ == "__main__":
    spark = get_spark("EcomProject_DataGeneration")
    print(f"Run mode scale: {SCALE}\n")

    num_users, num_products = generate_reference_data(spark)
    generate_clickstream_and_orders(spark, num_users, num_products)

    print("\nDone. Raw data is now sitting where a real upstream system would")
    print("drop it - ready for 01_ingest_to_bronze.py to pick up.")
    spark.stop()
