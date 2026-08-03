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
