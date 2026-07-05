# =============================================================================
# Session 2.2 — YARN Architecture Concepts Demo
# Big Data Engineering Training — Module 02
# =============================================================================
# This script shows how to inspect YARN configuration and resource allocation
# from inside a PySpark job.
#
# Run: spark-submit --master yarn --deploy-mode client yarn_concepts_demo.py
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder \
    .appName("YARN_Concepts_Demo") \
    .master("yarn") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

# =============================================================================
# 1. Print YARN/Spark resource configuration
# =============================================================================
print("=" * 60)
print("YARN / SPARK RESOURCE CONFIGURATION")
print("=" * 60)

conf = sc.getConf()

key_configs = [
    "spark.master",
    "spark.submit.deployMode",
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.driver.memory",
    "spark.driver.cores",
    "spark.yarn.queue",
    "spark.sql.shuffle.partitions",
    "spark.dynamicAllocation.enabled",
]

for key in key_configs:
    value = conf.get(key, "(not set)")
    print(f"  {key:<45} = {value}")

print(f"\n  Application ID   : {sc.applicationId}")
print(f"  UI Web URL       : {sc.uiWebUrl}")
print(f"  Default Parallelism: {sc.defaultParallelism}")


# =============================================================================
# 2. YARN Component roles — conceptual explanation via code comments
# =============================================================================
print("\n" + "=" * 60)
print("YARN COMPONENT ROLES (Conceptual)")
print("=" * 60)
print("""
ResourceManager (RM):
  - Master daemon of YARN cluster (runs on master node)
  - Two components:
      Scheduler        : allocates cluster resources (CPU/RAM) across apps
      ApplicationsManager: accepts job submissions, monitors AM health
  - UI: http://<rm-host>:8088

NodeManager (NM):
  - Worker daemon on EACH data node
  - Reports available resources to RM (heartbeat every second)
  - Launches and monitors Containers (= resource slots for executors)
  - Monitors container health, sends metrics to RM

ApplicationMaster (AM):
  - Per-job process, launched by RM inside the cluster
  - For Spark: this IS the Spark Driver (in cluster mode)
  - Negotiates executor containers from RM
  - Tracks task progress, requests more resources if needed
  - Cleaned up when job ends

Container:
  - A reserved chunk of CPU + memory on a NodeManager
  - One Spark executor = one Container
  - AM itself runs in a Container too (the AppMaster container)
""")


# =============================================================================
# 3. Demonstrate resource negotiation flow (simulated via print)
# =============================================================================
print("=" * 60)
print("JOB SUBMISSION FLOW: spark-submit → YARN → Executors")
print("=" * 60)
print("""
  Step 1: You run spark-submit --master yarn --deploy-mode cluster job.py
          └─ SparkSubmit sends job to YARN ResourceManager

  Step 2: RM launches ApplicationMaster Container on a free NodeManager
          └─ AM = Spark Driver starts inside the cluster

  Step 3: AM (Spark Driver) requests N Executor Containers from RM
          └─ RM allocates containers on NodeManagers with free resources

  Step 4: NodeManagers start Executor JVMs inside their containers
          └─ Executors register back with the Driver (AM)

  Step 5: Driver sends tasks to executors based on data locality
          └─ Executor runs tasks, reports results back to Driver

  Step 6: On completion, AM releases containers and notifies RM
          └─ All containers are freed; job summary written to YARN logs
""")


# =============================================================================
# 4. Client Mode vs Cluster Mode — key difference
# =============================================================================
print("=" * 60)
print(f"CURRENT DEPLOY MODE: {conf.get('spark.submit.deployMode', 'client')}")
print("=" * 60)
print("""
CLIENT MODE:
  [Your Machine] → spark-submit
       Driver (Spark) runs HERE (on your machine / gateway node)
       └─ Sends task instructions to executors over the network
       └─ Collects results back
       └─ Problem: if your machine dies, job fails
       └─ Logs: visible directly in your terminal

CLUSTER MODE:
  [Your Machine] → spark-submit → YARN RM
       YARN launches ApplicationMaster on a cluster node
       Driver runs INSIDE the cluster
       └─ Your terminal can disconnect after submission
       └─ Logs: use `yarn logs -applicationId <id>`
       └─ Better fault tolerance
""")


# =============================================================================
# 5. Run a simple job and count stages (to see YARN in action)
# =============================================================================
print("=" * 60)
print("RUNNING SAMPLE JOB — watch YARN UI for stages/tasks")
print("=" * 60)

HDFS_PATH = "hdfs:///user/training/module02/raw_data/sample_employees.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_PATH)

print(f"\nLoaded {df.count()} rows")  # triggers Job 1 (count)

dept_df = df.groupBy("department").agg(
    count("*").alias("total"),
    avg("salary").alias("avg_salary")
)
dept_df.show()  # triggers Job 2 (shuffle + show)

dept_df.write.mode("overwrite").parquet(
    "hdfs:///user/training/module02/output/yarn_demo_output"
)  # triggers Job 3 (write)

print("\nCheck YARN UI: http://<rm-host>:8088")
print("You should see this application with 3 jobs completed.")


# =============================================================================
# 6. YARN queue and resource limits
# =============================================================================
print("\n" + "=" * 60)
print("YARN QUEUES")
print("=" * 60)
print("""
YARN uses queues to partition cluster capacity across teams/projects.

  Default queue: 'default' (100% of capacity)

  Common queue setup:
    root
    ├── engineering  (60% capacity)
    ├── analytics    (30% capacity)
    └── default      (10% capacity)

  Submit to a specific queue:
    spark-submit --master yarn --queue engineering job.py

  Or in SparkSession:
    .config("spark.yarn.queue", "engineering")
""")
print(f"  Current queue: {conf.get('spark.yarn.queue', 'default')}")


spark.stop()
print("\nYARN concepts demo complete.")
