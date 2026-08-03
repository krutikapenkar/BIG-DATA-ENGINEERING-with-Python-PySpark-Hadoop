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
