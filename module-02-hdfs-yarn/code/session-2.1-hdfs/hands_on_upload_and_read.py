# =============================================================================
# Session 2.1 — Hands-On: Upload Dataset to HDFS, Read with PySpark
# Big Data Engineering Training — Module 02
# =============================================================================
# STEP-BY-STEP LAB EXERCISE
#
# Before running this script:
#   STEP 1 (Terminal): Create local sample file
#       cat > /home/training/employees.csv << 'EOF'
#       emp_id,name,department,salary,city
#       101,Ravi Kumar,Engineering,75000,Mumbai
#       ... (paste full CSV content)
#       EOF
#
#   STEP 2 (Terminal): Upload to HDFS
#       hdfs dfs -mkdir -p /user/training/lab02
#       hdfs dfs -put /home/training/employees.csv /user/training/lab02/
#       hdfs dfs -ls /user/training/lab02/
#
#   STEP 3: Run this script
#       spark-submit hands_on_upload_and_read.py
# =============================================================================

import subprocess
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

# =============================================================================
# UTILITY: Run HDFS shell commands from Python (useful in dev/learning mode)
# =============================================================================
def run_hdfs_command(cmd):
    """Run an HDFS CLI command and print output."""
    print(f"\n[CMD] {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"[STDERR] {result.stderr}")
    return result.returncode


# =============================================================================
# STEP A: Verify HDFS structure before starting
# =============================================================================
print("=" * 60)
print("LAB 2.1: Upload Dataset to HDFS and Read with PySpark")
print("=" * 60)

run_hdfs_command("hdfs dfs -ls /user/training/lab02/")


# =============================================================================
# STEP B: Create SparkSession configured for HDFS
# =============================================================================
spark = SparkSession.builder \
    .appName("Lab2.1_HDFS_Hands_On") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"\nSpark UI: http://localhost:4040")
print(f"Spark version: {spark.version}\n")


# =============================================================================
# STEP C: Read the uploaded CSV from HDFS
# =============================================================================
HDFS_FILE = "hdfs:///user/training/lab02/employees.csv"

try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(HDFS_FILE)

    print(f"Successfully read file from HDFS!")
    print(f"Total records: {df.count()}")
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    print()
    df.printSchema()
    df.show(truncate=False)

except Exception as e:
    print(f"ERROR reading from HDFS: {e}")
    print("Make sure you completed STEP 1 and STEP 2 above.")
    spark.stop()
    sys.exit(1)


# =============================================================================
# STEP D: Explore and transform the data
# =============================================================================
print("\n--- EXPLORATION ---")

# Basic stats
print("\nDescriptive statistics for salary:")
df.select("salary").describe().show()

# Department breakdown
print("\nEmployee count by department:")
df.groupBy("department").count().orderBy("count", ascending=False).show()

# City distribution
print("\nEmployee count by city:")
df.groupBy("city").count().orderBy("count", ascending=False).show()


# =============================================================================
# STEP E: Transform — calculate average salary per department
# =============================================================================
print("\n--- TRANSFORMATION ---")

dept_avg = df.groupBy("department") \
    .agg(spark_round(avg("salary"), 2).alias("avg_salary")) \
    .orderBy("avg_salary", ascending=False)

dept_avg.show()


# =============================================================================
# STEP F: Write processed data back to HDFS
# =============================================================================
HDFS_OUTPUT = "hdfs:///user/training/lab02/output/dept_avg_salary"

print(f"\nWriting aggregated results to HDFS: {HDFS_OUTPUT}")

dept_avg.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_OUTPUT)

print("Write complete!")

# Verify the output files in HDFS
run_hdfs_command(f"hdfs dfs -ls {HDFS_OUTPUT}")
run_hdfs_command(f"hdfs dfs -cat {HDFS_OUTPUT}/part-00000*")


# =============================================================================
# STEP G: Check block distribution using HDFS fsck
# =============================================================================
print("\n--- HDFS BLOCK INFO ---")
run_hdfs_command("hdfs fsck /user/training/lab02/employees.csv -files -blocks -locations")


# =============================================================================
# STEP H: Discuss what you observed
# =============================================================================
print("\n" + "=" * 60)
print("KEY OBSERVATIONS TO DISCUSS:")
print("=" * 60)
print(f"1. Default block size: 128 MB (small files fit in 1 block)")
print(f"2. Replication factor: 3 (3 copies across DataNodes)")
print(f"3. Partitions created: {df.rdd.getNumPartitions()} (matches HDFS blocks)")
print(f"4. Output written as part files — one per partition")
print(f"5. NameNode stores metadata; DataNodes store actual blocks")
print("=" * 60)


# =============================================================================
# Cleanup (optional — uncomment to delete test data after lab)
# =============================================================================
# run_hdfs_command("hdfs dfs -rm -r /user/training/lab02")
# print("Lab data cleaned up from HDFS.")


spark.stop()
print("\nLab 2.1 complete!")
