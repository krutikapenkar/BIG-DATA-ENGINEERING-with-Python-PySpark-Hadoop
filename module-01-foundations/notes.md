# Module 1 — Foundations of Big Data & Hadoop Ecosystem

Full theory (the 5 Vs, why RDBMS breaks at scale, Batch→Stream→Lambda/Kappa, industry use cases, Hadoop ecosystem, Spark vs MapReduce) is in **`Module1_BigData_Lecture.pptx`** in this folder. This file is the practical companion — what you actually type.

## The one-paragraph version

A single machine has a ceiling: one disk, one CPU, one point of failure. Big Data problems (Volume, Velocity, Variety, Veracity, Value) show up once your data or your query rate exceeds that ceiling. The fix is horizontal scaling — spread the data and the computation across many ordinary machines instead of buying one bigger one. Hadoop's HDFS handles the "spread the data" part; YARN handles "spread and schedule the computation"; Spark is the engine that actually runs distributed transformations fast (in-memory, unlike classic MapReduce which round-trips to disk at every step).

## SparkSession — the entry point

Every PySpark program starts here:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Module1FirstProgram") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)
print(spark.sparkContext.appName)
```

`.master("local[*]")` means "run entirely on this machine, using all CPU cores" — no cluster involved. From Module 2 onward, this becomes `.master("yarn")`.

## Reading a CSV and basic exploration

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")

df.show()            # print rows
df.printSchema()      # column names + types
df.count()            # row count
print(df.columns)     # column names as a list
```

`inferSchema=True` makes Spark scan the file twice to guess types — convenient for learning, but in production you'd define the schema explicitly instead.

## select / filter / groupBy / withColumn

```python
from pyspark.sql.functions import col, when

# SELECT specific columns
df.select("customer", "city", "amount").show()

# WHERE — keep matching rows
df.filter(col("status") == "Delivered").show()
df.filter(col("amount") > 50000).show()

# GROUP BY — aggregate
df.groupBy("city").sum("amount").orderBy("sum(amount)", ascending=False).show()

# Derived column
df.withColumn(
    "category",
    when(col("amount") >= 50000, "Premium")
    .when(col("amount") >= 20000, "Standard")
    .otherwise("Budget")
).show()
```

## Lazy evaluation — the one concept that explains Spark's speed

- **Transformations** (`select`, `filter`, `groupBy`, `withColumn`, `join`, `orderBy`) are lazy — Spark just builds a plan, nothing runs yet.
- **Actions** (`show`, `count`, `collect`, `write`, `first`, `take`) trigger actual execution.

This matters because it lets Spark see your *entire* plan before running anything, and optimize it as a whole — reorder filters, combine steps, skip unread columns — rather than executing each line the moment you write it.

## Spark UI

While a job is running (or just finished), open **http://localhost:4040** — Jobs, Stages, Storage, Executors, SQL/DAG tabs. This is where you'll debug performance from Module 2 onward; worth getting familiar with the layout now while the jobs are still small and fast.

## Practical exercise

See [`code/01_first_pyspark_program.py`](code/01_first_pyspark_program.py) — run it, then try:
1. Add 5 more rows to the sample data and confirm the new count.
2. Find the average `amount` grouped by `product`.
3. Filter for two cities at once using `&`.
4. Add a `discount_amount` column based on the `category` you computed above.
