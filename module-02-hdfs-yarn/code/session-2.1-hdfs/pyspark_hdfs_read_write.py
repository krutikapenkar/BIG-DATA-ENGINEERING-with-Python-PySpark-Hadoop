# =============================================================================
# Session 2.1 — Reading & Writing HDFS Files from PySpark
# Big Data Engineering Training — Module 02
# =============================================================================
# Prerequisites:
#   - Hadoop + Spark running (standalone, pseudo-distributed, or cluster)
#   - HDFS path: /user/training/module02/raw_data/sample_employees.csv
#     (upload it first using: hdfs dfs -put sample_employees.csv /user/training/module02/raw_data/)
#   - Run with: spark-submit pyspark_hdfs_read_write.py
#     OR open in pyspark shell (type `pyspark` in terminal)
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# -----------------------------------------------------------------------------
# 1. Create SparkSession — entry point to PySpark
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("HDFS_Read_Write_Demo") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Set log level to reduce noise during learning
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SparkSession created successfully")
print(f"Spark version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")
print("=" * 60)


# -----------------------------------------------------------------------------
# 2. Define Schema explicitly (better practice than schema inference)
# -----------------------------------------------------------------------------
employee_schema = StructType([
    StructField("emp_id",     IntegerType(), nullable=False),
    StructField("name",       StringType(),  nullable=False),
    StructField("department", StringType(),  nullable=True),
    StructField("salary",     DoubleType(),  nullable=True),
    StructField("city",       StringType(),  nullable=True),
    StructField("join_date",  StringType(),  nullable=True),
])


# -----------------------------------------------------------------------------
# 3. Read CSV from HDFS
# -----------------------------------------------------------------------------
HDFS_INPUT_PATH = "hdfs:///user/training/module02/raw_data/sample_employees.csv"

print(f"\nReading CSV from HDFS: {HDFS_INPUT_PATH}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(HDFS_INPUT_PATH)

# With explicit schema (recommended in production):
# df = spark.read \
#     .option("header", "true") \
#     .schema(employee_schema) \
#     .csv(HDFS_INPUT_PATH)

print(f"Rows loaded: {df.count()}")
print(f"Columns: {df.columns}")
print("\nSchema:")
df.printSchema()

print("\nFirst 5 rows:")
df.show(5, truncate=False)


# -----------------------------------------------------------------------------
# 4. Basic transformations
# -----------------------------------------------------------------------------
print("\n--- Filtering: Engineering department ---")
eng_df = df.filter(col("department") == "Engineering")
eng_df.show()

print("\n--- Salary > 70000 ---")
high_salary_df = df.filter(col("salary") > 70000)
high_salary_df.select("name", "department", "salary", "city").show()


# -----------------------------------------------------------------------------
# 5. Aggregations — GroupBy
# -----------------------------------------------------------------------------
print("\n--- Average salary by department ---")
dept_stats = df.groupBy("department") \
    .agg(
        count("emp_id").alias("headcount"),
        avg("salary").alias("avg_salary"),
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary")
    ) \
    .orderBy("avg_salary", ascending=False)

dept_stats.show()


# -----------------------------------------------------------------------------
# 6. Add a derived column
# -----------------------------------------------------------------------------
from pyspark.sql.functions import when

df_with_grade = df.withColumn(
    "grade",
    when(col("salary") >= 80000, "A")
    .when(col("salary") >= 65000, "B")
    .otherwise("C")
)

print("\n--- Employee grade based on salary ---")
df_with_grade.select("name", "salary", "grade").show()


# -----------------------------------------------------------------------------
# 7. Write results back to HDFS in different formats
# -----------------------------------------------------------------------------

# --- Write as CSV ---
HDFS_CSV_OUTPUT = "hdfs:///user/training/module02/output/employees_filtered_csv"
high_salary_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_CSV_OUTPUT)
print(f"\nCSV output written to: {HDFS_CSV_OUTPUT}")

# --- Write as Parquet (columnar format, compressed, preferred for analytics) ---
HDFS_PARQUET_OUTPUT = "hdfs:///user/training/module02/output/employees_parquet"
df_with_grade.write \
    .mode("overwrite") \
    .parquet(HDFS_PARQUET_OUTPUT)
print(f"Parquet output written to: {HDFS_PARQUET_OUTPUT}")

# --- Write as JSON ---
HDFS_JSON_OUTPUT = "hdfs:///user/training/module02/output/dept_stats_json"
dept_stats.write \
    .mode("overwrite") \
    .json(HDFS_JSON_OUTPUT)
print(f"JSON output written to: {HDFS_JSON_OUTPUT}")


# -----------------------------------------------------------------------------
# 8. Read back the Parquet file to verify
# -----------------------------------------------------------------------------
print("\n--- Reading back Parquet file from HDFS ---")
parquet_df = spark.read.parquet(HDFS_PARQUET_OUTPUT)
parquet_df.show(5)
print(f"Parquet row count: {parquet_df.count()}")


# -----------------------------------------------------------------------------
# 9. Check partition count (how data is distributed across nodes)
# -----------------------------------------------------------------------------
print(f"\nNumber of partitions in main df: {df.rdd.getNumPartitions()}")
print(f"Number of partitions in filtered df: {high_salary_df.rdd.getNumPartitions()}")

# Repartition for better parallelism when writing large data
df_repartitioned = df.repartition(4)
print(f"After repartition(4): {df_repartitioned.rdd.getNumPartitions()}")


# -----------------------------------------------------------------------------
# 10. Using Spark SQL (alternative to DataFrame API)
# -----------------------------------------------------------------------------
df.createOrReplaceTempView("employees")

print("\n--- SQL Query: Top earners by department ---")
spark.sql("""
    SELECT department, name, salary
    FROM (
        SELECT department, name, salary,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
        FROM employees
    )
    WHERE rnk = 1
    ORDER BY salary DESC
""").show()


# -----------------------------------------------------------------------------
# 11. Stop SparkSession
# -----------------------------------------------------------------------------
spark.stop()
print("\nSparkSession stopped. Job complete.")
