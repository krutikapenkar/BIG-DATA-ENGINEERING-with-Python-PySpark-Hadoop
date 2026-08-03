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
