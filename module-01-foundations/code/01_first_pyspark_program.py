"""
Module 1 — First PySpark program.
Run with: python 01_first_pyspark_program.py
(No cluster needed — runs local[*].)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("Module1FirstProgram") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)
print("Spark UI     :", spark.sparkContext.uiWebUrl)

# ---- Read ----
df = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")

print("\nSchema:")
df.printSchema()

print("\nRow count:", df.count())
df.show()

# ---- Filter ----
print("\nDelivered orders over 50,000:")
df.filter((col("status") == "Delivered") & (col("amount") > 50000)).show()

# ---- Group by ----
print("\nTotal revenue by city:")
df.groupBy("city").sum("amount").orderBy("sum(amount)", ascending=False).show()

# ---- Derived column ----
categorized = df.withColumn(
    "category",
    when(col("amount") >= 50000, "Premium")
    .when(col("amount") >= 20000, "Standard")
    .otherwise("Budget"),
)

print("\nOrders with category:")
categorized.select("order_id", "customer", "amount", "category").show()

spark.stop()
