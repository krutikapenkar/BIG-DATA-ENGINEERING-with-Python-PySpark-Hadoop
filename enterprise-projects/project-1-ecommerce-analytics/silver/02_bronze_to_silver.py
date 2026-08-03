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
