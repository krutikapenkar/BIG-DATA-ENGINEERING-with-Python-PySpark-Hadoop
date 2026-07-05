"""
Module 3 — DataFrames, joins, and window functions.
Run with: python 02_dataframes_joins_windows.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, sum as spark_sum, round as spark_round

spark = SparkSession.builder.appName("Module3_DataFramesJoins").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

orders = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")
customers = spark.read.option("header", True).option("inferSchema", True).csv("customers.csv")

print("Orders schema:")
orders.printSchema()

# ---- Join ----
joined = orders.join(customers, on="customer_id", how="inner")
print("\nJoined data:")
joined.select("order_id", "customer", "product", "amount", "status").show()

# ---- Aggregation: total spend per customer ----
print("\nTotal spend per customer:")
joined.groupBy("customer").agg(
    spark_round(spark_sum("amount"), 2).alias("total_spend")
).orderBy(col("total_spend").desc()).show()

# ---- Window function: rank orders within each city by amount ----
city_window = Window.partitionBy("city").orderBy(col("amount").desc())
ranked = joined.withColumn("rank_in_city", rank().over(city_window))

print("\nOrders ranked by amount within each city:")
ranked.select("city", "customer", "amount", "rank_in_city").show()

# ---- Write as Parquet, then read back to verify ----
output_path = "output/orders_enriched_parquet"
ranked.write.mode("overwrite").parquet(output_path)

roundtrip = spark.read.parquet(output_path)
print(f"\nWrote and re-read {roundtrip.count()} rows from {output_path}")

spark.stop()
