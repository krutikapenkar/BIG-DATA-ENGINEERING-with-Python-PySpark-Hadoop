# =============================================================================
# Session 2.2 — PySpark Batch Job for spark-submit on YARN
# Big Data Engineering Training — Module 02
# =============================================================================
# This is a complete, self-contained PySpark job designed to be submitted via:
#
#   spark-submit --master yarn --deploy-mode cluster pyspark_batch_job.py \
#       --input hdfs:///user/training/module02/raw_data/sample_employees.csv \
#       --output hdfs:///user/training/module02/output/salary_report
#
# It reads employee data from HDFS, runs transformations, and writes output.
# =============================================================================

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, max as spark_max, min as spark_min,
    round as spark_round, when, lit, current_timestamp
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Employee Salary Analysis Batch Job")
    parser.add_argument("--input",  default="hdfs:///user/training/module02/raw_data/sample_employees.csv",
                        help="HDFS input path for employee CSV")
    parser.add_argument("--output", default="hdfs:///user/training/module02/output/salary_report",
                        help="HDFS output path for results")
    return parser.parse_args()


def create_spark_session(app_name):
    """Create SparkSession — in cluster mode, this connects to YARN AM."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def read_employees(spark, input_path):
    """Read CSV from HDFS with schema inference."""
    print(f"[INFO] Reading data from: {input_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    print(f"[INFO] Loaded {df.count()} records across {df.rdd.getNumPartitions()} partitions")
    return df


def transform_employees(df):
    """Apply business logic transformations."""

    # 1. Add salary grade
    df = df.withColumn(
        "salary_grade",
        when(col("salary") >= 85000, "Senior")
        .when(col("salary") >= 70000, "Mid")
        .otherwise("Junior")
    )

    # 2. Add department rank by salary using Window function
    window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
    df = df.withColumn("dept_rank", F.rank().over(window_spec))

    # 3. Add processing timestamp (useful for audit/lineage)
    df = df.withColumn("processed_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    return df


def compute_department_summary(df):
    """Aggregate stats by department."""
    return df.groupBy("department").agg(
        count("emp_id").alias("headcount"),
        spark_round(avg("salary"), 2).alias("avg_salary"),
        spark_max("salary").alias("max_salary"),
        spark_min("salary").alias("min_salary"),
        count(when(col("salary_grade") == "Senior", 1)).alias("senior_count"),
        count(when(col("salary_grade") == "Mid", 1)).alias("mid_count"),
        count(when(col("salary_grade") == "Junior", 1)).alias("junior_count"),
    ).orderBy("avg_salary", ascending=False)


def compute_city_summary(df):
    """Aggregate stats by city."""
    return df.groupBy("city").agg(
        count("emp_id").alias("employee_count"),
        spark_round(avg("salary"), 2).alias("avg_salary")
    ).orderBy("employee_count", ascending=False)


def write_results(df, output_path, format="parquet", mode="overwrite"):
    """Write DataFrame to HDFS in specified format."""
    print(f"[INFO] Writing to: {output_path} (format={format}, mode={mode})")
    writer = df.write.mode(mode)
    if format == "parquet":
        writer.parquet(output_path)
    elif format == "csv":
        writer.option("header", "true").csv(output_path)
    elif format == "json":
        writer.json(output_path)
    print(f"[INFO] Write complete: {output_path}")


def log_execution_plan(df, label=""):
    """Print execution plan — useful for optimization during learning."""
    print(f"\n--- Execution Plan: {label} ---")
    df.explain(mode="simple")


def main():
    args = parse_args()

    print("=" * 60)
    print("Starting Employee Salary Analysis Batch Job")
    print(f"Input : {args.input}")
    print(f"Output: {args.output}")
    print("=" * 60)

    # Initialize Spark
    spark = create_spark_session("EmployeeSalaryAnalysis")
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[INFO] Spark App ID  : {spark.sparkContext.applicationId}")
    print(f"[INFO] Spark UI      : {spark.sparkContext.uiWebUrl}")
    print(f"[INFO] Deploy Mode   : {spark.sparkContext.getConf().get('spark.submit.deployMode', 'client')}")
    print(f"[INFO] Master        : {spark.sparkContext.master}")

    try:
        # ---- READ ----
        raw_df = read_employees(spark, args.input)
        raw_df.printSchema()

        # ---- TRANSFORM ----
        transformed_df = transform_employees(raw_df)
        transformed_df.cache()  # cache since we use it multiple times

        print("\n[INFO] Sample transformed data:")
        transformed_df.show(10, truncate=False)

        # ---- AGGREGATE ----
        dept_summary = compute_department_summary(transformed_df)
        city_summary = compute_city_summary(transformed_df)

        print("\n[INFO] Department Summary:")
        dept_summary.show()

        print("\n[INFO] City Summary:")
        city_summary.show()

        # Show execution plan (learning exercise)
        log_execution_plan(dept_summary, "Department Aggregation")

        # ---- WRITE ----
        # Write full enriched data as Parquet (columnar, compressed — best for analytics)
        write_results(transformed_df, f"{args.output}/enriched_employees", format="parquet")

        # Write department summary as CSV (human-readable)
        write_results(dept_summary, f"{args.output}/dept_summary_csv", format="csv")

        # Write city summary as JSON
        write_results(city_summary, f"{args.output}/city_summary_json", format="json")

        print("\n[INFO] All outputs written successfully!")
        print("[INFO] Verify with:")
        print(f"  hdfs dfs -ls {args.output}/")

        # Unpersist cache
        transformed_df.unpersist()

    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\n[INFO] SparkSession stopped. Batch job complete.")


if __name__ == "__main__":
    main()
