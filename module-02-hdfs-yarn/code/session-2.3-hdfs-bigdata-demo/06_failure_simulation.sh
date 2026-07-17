#!/bin/bash
# =============================================================================
# Session 2.3 — Step 6: Simulate a DataNode failure
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# This is the payoff moment for the whole session: "why does any of this
# block/replication stuff matter?" Answer: because machines die, and HDFS is
# built to survive that.
#
# CAVEAT (call this out explicitly to students): on this single-node lab,
# replication is capped at 1 (only one DataNode exists), so killing that one
# DataNode WILL make the data temporarily unreadable — that's expected, not
# a bug. It's the strongest possible argument for why production clusters
# use replication=3 across separate physical machines. If you have access to
# a multi-node cluster, repeat this exact script there with replication=3
# and the read will succeed even with one DataNode down.
# =============================================================================

set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
HDFS_FILE="/user/training/module02_bigdata/raw_data/transactions.csv"

echo "=== 1) Confirm the cluster is healthy before the test ==="
hdfs dfsadmin -report | grep -A2 "Live datanodes"

echo
echo "=== 2) Stop the DataNode daemon (simulates a server dying) ==="
"$HADOOP_HOME/sbin/hadoop-daemon.sh" stop datanode
sleep 3

echo
echo "=== 3) Try to read the file with no DataNode available ==="
if hdfs dfs -cat "$HDFS_FILE" 2>&1 | head -5; then
  echo "(unexpected: read succeeded — check if another DataNode is still up)"
else
  echo "-> Read failed: no DataNode holds a replica of these blocks."
  echo "   This is exactly why production HDFS uses replication=3."
fi

echo
echo "=== 4) Restart the DataNode to restore the cluster ==="
"$HADOOP_HOME/sbin/hadoop-daemon.sh" start datanode
sleep 3
hdfs dfsadmin -report | grep -A2 "Live datanodes"
