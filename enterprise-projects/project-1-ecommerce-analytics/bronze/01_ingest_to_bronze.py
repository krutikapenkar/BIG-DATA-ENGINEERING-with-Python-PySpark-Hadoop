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
