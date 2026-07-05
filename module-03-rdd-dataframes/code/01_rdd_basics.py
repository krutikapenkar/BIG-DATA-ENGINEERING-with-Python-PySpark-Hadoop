"""
Module 3 — RDD fundamentals.
Run with: python 01_rdd_basics.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Module3_RDDBasics").master("local[*]").getOrCreate()
sc = spark.sparkContext

# ---- Creating RDDs ----
rdd_from_list = sc.parallelize(range(1, 11))
print("Partitions:", rdd_from_list.getNumPartitions())

# ---- Transformations (lazy) ----
doubled = rdd_from_list.map(lambda x: x * 2)
evens = rdd_from_list.filter(lambda x: x % 2 == 0)

# ---- Actions (trigger execution) ----
print("Doubled       :", doubled.collect())
print("Evens         :", evens.collect())
print("Count of evens:", evens.count())
print("Sum of all    :", rdd_from_list.reduce(lambda a, b: a + b))

# ---- word count, the classic RDD example ----
lines = sc.parallelize([
    "big data engineering with pyspark",
    "pyspark runs on top of spark",
    "spark is faster than mapreduce",
])
word_counts = (
    lines.flatMap(lambda line: line.split(" "))
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
)
print("\nWord counts:")
for word, count in sorted(word_counts.collect()):
    print(f"  {word}: {count}")

spark.stop()
