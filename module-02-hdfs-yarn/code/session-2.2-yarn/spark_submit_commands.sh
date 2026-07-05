#!/bin/bash
# =============================================================================
# Session 2.2 — YARN & Cluster Execution: spark-submit Commands
# Big Data Engineering Training — Module 02
# =============================================================================
# These are the key spark-submit command patterns you must know.
# Run each block from your Hadoop cluster terminal.
# The PySpark scripts referenced here are in this same folder.
# =============================================================================


# =============================================================================
# 1. BASIC spark-submit (local mode — no YARN, for testing only)
# =============================================================================
spark-submit \
    --master local[2] \
    pyspark_batch_job.py


# =============================================================================
# 2. YARN CLIENT MODE
# =============================================================================
# - Driver runs on the MACHINE WHERE YOU RUN spark-submit (your laptop/gateway)
# - Executors run on YARN NodeManagers (cluster)
# - Good for: interactive debugging (driver logs appear on your terminal)
# - NOT suitable for production (driver depends on your local machine staying up)

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 2 \
    --executor-cores 2 \
    pyspark_batch_job.py


# =============================================================================
# 3. YARN CLUSTER MODE
# =============================================================================
# - Driver runs INSIDE the YARN cluster (as an ApplicationMaster)
# - Executors run on YARN NodeManagers
# - Good for: production batch jobs (your terminal can close after submission)
# - Logs available via: yarn logs -applicationId <app_id>

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 3 \
    --executor-cores 2 \
    pyspark_batch_job.py


# =============================================================================
# 4. spark-submit WITH ALL KEY ARGUMENTS (production template)
# =============================================================================
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "EmployeeSalaryReport_$(date +%Y%m%d)" \
    --driver-memory 2g \
    --driver-cores 1 \
    --executor-memory 4g \
    --executor-cores 4 \
    --num-executors 5 \
    --conf spark.sql.shuffle.partitions=20 \
    --conf spark.executor.memoryOverhead=512m \
    --conf spark.yarn.maxAppAttempts=2 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.dynamicAllocation.enabled=false \
    --files /home/training/config.properties \
    --py-files /home/training/utils.zip \
    pyspark_batch_job.py \
    --input hdfs:///user/training/module02/raw_data/sample_employees.csv \
    --output hdfs:///user/training/module02/output/salary_report


# =============================================================================
# 5. DYNAMIC RESOURCE ALLOCATION (let YARN decide executor count)
# =============================================================================
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=10 \
    --conf spark.dynamicAllocation.initialExecutors=2 \
    --conf spark.shuffle.service.enabled=true \
    pyspark_batch_job.py


# =============================================================================
# 6. MONITORING — check job status after submission
# =============================================================================

# List all YARN applications
yarn application -list

# List only running applications
yarn application -list -appStates RUNNING

# Check a specific application status
yarn application -status <application_id>
# Example:
yarn application -status application_1234567890_0001

# View logs for a completed job (cluster mode — driver logs are in YARN)
yarn logs -applicationId application_1234567890_0001

# View only stderr (driver output)
yarn logs -applicationId application_1234567890_0001 -log_files stderr

# Kill a running application
yarn application -kill application_1234567890_0001


# =============================================================================
# 7. COMPARE: CLIENT vs CLUSTER MODE at a glance
# =============================================================================
#
#  Feature              | CLIENT MODE              | CLUSTER MODE
#  ---------------------|--------------------------|---------------------------
#  Driver location      | Your gateway/local node  | YARN cluster (AM)
#  Deploy command       | --deploy-mode client     | --deploy-mode cluster
#  Logs visible where   | Your terminal directly   | yarn logs command
#  Use case             | Dev, debugging           | Production batch jobs
#  Gateway dependency   | Terminal must stay open  | Can close terminal
#  Suitable for         | Interactive testing      | Scheduled/automated jobs
#  spark-shell/pyspark  | Always uses client mode  | Not applicable


# =============================================================================
# 8. MEMORY SIZING CHEAT SHEET
# =============================================================================
#
#  --driver-memory       : JVM heap for driver (default: 1g)
#  --executor-memory     : JVM heap per executor (default: 1g)
#  --executor-cores      : cores per executor (default: 1)
#  --num-executors       : total executors (static allocation)
#
#  Total memory per executor = executor-memory + memoryOverhead
#  memoryOverhead = max(executorMemory * 0.1, 384 MB)
#
#  Total cluster memory used = num-executors * (executor-memory + overhead)
#
#  Example: 5 executors * 4g each + 512m overhead = 22.5g total
