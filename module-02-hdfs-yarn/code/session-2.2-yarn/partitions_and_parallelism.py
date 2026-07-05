# =============================================================================
# Session 2.2 — Partitions & Parallelism: Understanding Task Distribution
# Big Data Engineering Training — Module 02
# =============================================================================
# This script demonstrates HOW Spark distributes work across tasks and executors.
# Run with: spark-submit --master yarn --deploy-mode client partitions_and_parallelism.py
#
# Key concepts:
#   Partition  = one chunk of data processed by ONE task on ONE executor core
#   Task       = unit of work Spark schedules on a partition
#   Stage      = set of tasks that can run in parallel (no shuffle between them)
#   Job        = one action (count, show, write) triggers a job
#   Executor   = JVM process on a NodeManager running your tasks
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id, count
import time

spark = SparkSession.builder \
    .appName("Partitions_Parallelism_Demo") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("DEMO: Partitions and Parallelism in PySpark")
print(f"Default shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"Default parallelism (RDD): {spark.sparkContext.defaultParallelism}")
print("=" * 60)


# =============================================================================
# PART 1: Partitions when reading from HDFS
# =============================================================================
print("\n--- PART 1: Partitions from HDFS Read ---")

HDFS_PATH = "hdfs:///user/training/module02/raw_data/sample_employees.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(HDFS_PATH)

# How many partitions did Spark create?
# Spark creates 1 partition per HDFS block (128MB by default).
# Small files typically land in 1-2 partitions.
initial_partitions = df.rdd.getNumPartitions()
print(f"Partitions after CSV read: {initial_partitions}")

# See which partition each row belongs to
print("\nRow distribution across partitions:")
df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy("partition_id") \
  .show()


# =============================================================================
# PART 2: repartition() vs coalesce()
# =============================================================================
print("\n--- PART 2: repartition() vs coalesce() ---")

# repartition(n): FULL SHUFFLE — redistributes data evenly across n partitions
# Use when: increasing partitions OR need even distribution
t0 = time.time()
df_repartitioned = df.repartition(4)
print(f"repartition(4) partitions: {df_repartitioned.rdd.getNumPartitions()}")
print(f"  → triggers a full shuffle (expensive but balanced)")

# coalesce(n): NO SHUFFLE — merges adjacent partitions (can only decrease)
# Use when: reducing partitions before write (avoid small output files)
df_coalesced = df_repartitioned.coalesce(2)
print(f"coalesce(2) partitions: {df_coalesced.rdd.getNumPartitions()}")
print(f"  → no shuffle (cheap but may cause skew)")

# Repartition on a column (hash partitioning by value — useful before groupBy)
df_by_dept = df.repartition(4, col("department"))
print(f"repartition(4, 'department') partitions: {df_by_dept.rdd.getNumPartitions()}")
print(f"  → all rows for same department land in the same partition")

# Show distribution after column repartition
print("\nRow distribution by partition after repartition by department:")
df_by_dept.withColumn("partition_id", spark_partition_id()) \
          .groupBy("partition_id", "department") \
          .count() \
          .orderBy("partition_id") \
          .show()


# =============================================================================
# PART 3: Shuffle partitions (what happens during groupBy / join)
# =============================================================================
print("\n--- PART 3: Shuffle Partitions (groupBy creates a new partition set) ---")

# When you do a groupBy, Spark does a SHUFFLE:
# 1. Map stage: each task sends rows to the correct reducer by hash(key)
# 2. Reduce stage: new tasks (= spark.sql.shuffle.partitions) aggregate the data

# Default shuffle partitions = 200 (too many for small data — override it!)
print(f"\nDefault shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print("(We already set it to 4 in SparkSession — good for small datasets)")

dept_agg = df.groupBy("department").count()
dept_agg.show()

# Explain the physical plan — notice "Exchange" = shuffle step
print("\nPhysical plan for groupBy (look for 'Exchange hashpartitioning'):")
dept_agg.explain()

# Change shuffle partitions dynamically
spark.conf.set("spark.sql.shuffle.partitions", "2")
dept_agg_v2 = df.groupBy("department").count()
print(f"\nWith shuffle.partitions=2, groupBy result partitions: {dept_agg_v2.rdd.getNumPartitions()}")


# =============================================================================
# PART 4: Practical task distribution calculation
# =============================================================================
print("\n--- PART 4: Task Distribution Calculation ---")

print("""
HOW TASKS ARE DISTRIBUTED:

Setup:
  num-executors = 3
  executor-cores = 2
  Total parallel tasks = 3 * 2 = 6 tasks can run simultaneously

Example: reading 12 partitions
  - Spark launches 12 tasks total
  - YARN assigns: first 6 tasks to 6 cores (3 executors × 2 cores)
  - When any task finishes, next waiting task is scheduled
  - All 12 tasks complete in ~2 "waves"

If partitions = 6 and total cores = 6:  → perfect parallelism (1 wave)
If partitions = 1 and total cores = 6:  → only 1 core busy, 5 idle (wasted!)
If partitions = 100 and total cores = 6: → 17 waves (last wave has 4 tasks)

RULE OF THUMB:
  partitions = 2x to 4x total executor cores
""")

# Demonstrate: how many tasks ran for our operations
print("Check Spark UI at http://<driver-host>:4040 → Jobs → Stages")
print("Each row in 'Stages' = one stage, column 'Tasks' = how many ran in parallel")


# =============================================================================
# PART 5: Data skew — uneven partitions (common real-world problem)
# =============================================================================
print("\n--- PART 5: Skewed Partitions (one partition much larger than others) ---")

# Simulate skew by repartitioning on a column with one dominant value
# In real data: one city/country has 80% of rows → that task takes 10x longer
from pyspark.sql.functions import lit
import random

# Create a skewed dataset
data = [(i, "Mumbai" if i % 10 != 0 else "Delhi") for i in range(1000)]
skewed_df = spark.createDataFrame(data, ["id", "city"])

skewed_repartitioned = skewed_df.repartition(4, col("city"))

print("\nRow distribution with skew (repartition by city):")
skewed_repartitioned.withColumn("partition_id", spark_partition_id()) \
                    .groupBy("partition_id", "city") \
                    .count() \
                    .orderBy("partition_id") \
                    .show()

print("Observation: One partition has ~900 rows, others have ~100 → SKEW!")
print("Fix: Use salting technique or increase partition count to spread load")


# =============================================================================
# PART 6: Using Spark UI to monitor (conceptual walkthrough)
# =============================================================================
print("\n--- PART 6: Monitoring with YARN UI ---")
print("""
YARN ResourceManager UI:
  URL: http://<resourcemanager-host>:8088
  Shows: all running/finished applications, resources used

Spark Application UI (only while running, client mode):
  URL: http://localhost:4040
  Key tabs:
    Jobs    → each action (show/count/write) = 1 job
    Stages  → each job = multiple stages (separated by shuffles)
    Tasks   → each stage = N tasks (one per partition)
    Storage → cached DataFrames
    Executors → memory/CPU per executor, GC time, spill

YARN Cluster Mode logs:
  yarn logs -applicationId <app_id>
  yarn logs -applicationId <app_id> -log_files stdout
""")

# =============================================================================
# Final summary
# =============================================================================
print("=" * 60)
print("PARTITION CHEAT SHEET:")
print("=" * 60)
print("df.rdd.getNumPartitions()  → current partition count")
print("df.repartition(n)          → shuffle, even split, can increase/decrease")
print("df.coalesce(n)             → no shuffle, merge only, can only decrease")
print("df.repartition(n, col)     → hash partition by column value")
print("spark.sql.shuffle.partitions → controls groupBy/join output partitions")
print("Default is 200 — always tune it for your dataset size!")
print("=" * 60)

spark.stop()
print("\nDemo complete.")
