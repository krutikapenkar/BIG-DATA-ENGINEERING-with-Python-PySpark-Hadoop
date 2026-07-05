# Module 3 — PySpark Core: RDDs & DataFrames

## RDDs — the low-level API

**RDD = Resilient Distributed Dataset.**
- **Resilient** — if a partition is lost, Spark recomputes just that partition from its lineage (the sequence of transformations that created it), not the whole job.
- **Distributed** — data lives across multiple machines/partitions, not one.
- **Dataset** — a collection of records; unlike a DataFrame, an RDD has no schema (no column names or types).

You rarely write raw RDD code day to day — DataFrames and Spark SQL are built on top of it — but it's worth knowing because some low-level operations only exist at this layer, and understanding it explains *why* DataFrames behave the way they do.

```python
sc = spark.sparkContext

rdd1 = sc.parallelize([1, 2, 3, 4, 5])          # from a Python list
rdd2 = sc.textFile("orders.csv")                 # from a local/HDFS file
rdd3 = spark.read.csv("orders.csv").rdd           # from a DataFrame

print(rdd1.getNumPartitions())
```

### Core RDD transformations & actions

```python
nums = sc.parallelize(range(1, 11))

# Transformations (lazy)
doubled = nums.map(lambda x: x * 2)
evens = nums.filter(lambda x: x % 2 == 0)

# Actions (trigger execution)
print(doubled.collect())     # [2, 4, 6, ..., 20]
print(evens.count())         # 5
print(nums.reduce(lambda a, b: a + b))   # 55
```

## DataFrames — the high-level API

A DataFrame is an RDD with a schema attached, plus a query optimizer (Catalyst) sitting in front of it. Prefer DataFrames for almost everything — they're faster (optimized execution plans) and easier to read (SQL-like operations instead of raw lambdas).

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")

df.select("customer", "amount").show()
df.filter(df.amount > 20000).show()
df.groupBy("city").count().show()

# DataFrame <-> RDD
rdd_from_df = df.rdd
df_from_rdd = rdd_from_df.toDF()
```

### Joins

```python
orders = spark.read.option("header", True).csv("orders.csv")
customers = spark.read.option("header", True).csv("customers.csv")

joined = orders.join(customers, on="customer_id", how="inner")
# how: "inner", "left", "right", "outer", "left_semi", "left_anti"
```

### Window functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

w = Window.partitionBy("city").orderBy(col("amount").desc())
ranked = orders.withColumn("rank_in_city", rank().over(w))
```

## Reading & writing formats

| Format | Read | Write | Notes |
|---|---|---|---|
| CSV | `spark.read.option("header", True).csv(path)` | `df.write.option("header", True).csv(path)` | Human-readable, no schema stored |
| Parquet | `spark.read.parquet(path)` | `df.write.parquet(path)` | Columnar, compressed, schema embedded — default choice for analytics |
| JSON | `spark.read.json(path)` | `df.write.json(path)` | One JSON object per line by default |

```python
df.write.mode("overwrite").parquet("output/orders_parquet")
```

`mode` options: `"overwrite"`, `"append"`, `"error"` (default — fails if path exists), `"ignore"`.

## Practical exercises

See [`code/`](code/) for runnable versions of everything above. Try:
1. Load `orders.csv` and `customers.csv`, join them, and find total spend per customer.
2. Rank orders within each city by amount using a window function.
3. Write the joined result as Parquet, then read it back and confirm the row count matches.
