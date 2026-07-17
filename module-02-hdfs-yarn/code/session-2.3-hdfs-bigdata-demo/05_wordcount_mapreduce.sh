#!/bin/bash
# =============================================================================
# Session 2.3 — Step 5: Run a real distributed job (MapReduce WordCount) over
# the big data
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# Steps 1-4 showed WHERE data lives (blocks, DataNodes). This step shows WHY
# that layout matters: a distributed job gets one map task per block. On a
# single-node lab all tasks happen to run on the same machine, but the
# framework doesn't know or care — this is exactly how it'd parallelize
# across real DataNodes in a multi-node cluster.
# =============================================================================

set -euo pipefail

HDFS_IN="/user/training/module02_bigdata/raw_data"
HDFS_OUT="/user/training/module02_bigdata/wordcount_output"
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"

echo "=== Clear any previous output (MapReduce refuses to overwrite) ==="
hdfs dfs -rm -r -f "$HDFS_OUT" || true

echo
echo "=== Running WordCount — one map task per HDFS block of the input ==="
EXAMPLES_JAR=$(find "$HADOOP_HOME/share/hadoop/mapreduce" -name "hadoop-mapreduce-examples-*.jar" | head -1)
hadoop jar "$EXAMPLES_JAR" wordcount "$HDFS_IN" "$HDFS_OUT"

echo
echo "=== Top 20 most frequent tokens in the output ==="
hdfs dfs -cat "$HDFS_OUT/part-r-00000" | sort -t$'\t' -k2 -nr | head -20

echo
echo "Point out to students: 'category', 'city', and product names (Laptop,"
echo "Phone, ...) should dominate the counts — that's the CSV's own repeated"
echo "values being tallied, proof the job actually read and processed the data."
