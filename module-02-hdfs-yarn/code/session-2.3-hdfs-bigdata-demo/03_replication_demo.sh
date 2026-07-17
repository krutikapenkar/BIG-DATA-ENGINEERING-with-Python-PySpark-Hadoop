#!/bin/bash
# =============================================================================
# Session 2.3 — Step 3: Replication factor demo
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# Replication is HDFS's fault-tolerance mechanism — every block is copied to
# multiple DataNodes (default 3 in production) so losing one machine doesn't
# lose data. On a single-node lab there's only one DataNode, so replication
# is capped at 1 — worth explicitly calling out as a LIMITATION of this lab,
# not a flaw in HDFS. If you later stand up a multi-node cluster, re-run this
# same script and watch the replica count actually reach 2/3.
# =============================================================================

set -euo pipefail

HDFS_FILE="/user/training/module02_bigdata/raw_data/transactions.csv"

echo "=== Current replication factor of our file ==="
hdfs dfs -stat "%r" "$HDFS_FILE"

echo
echo "=== Cluster-wide default (dfs.replication from hdfs-site.xml) ==="
hdfs getconf -confKey dfs.replication

echo
echo "=== Attempt to raise replication to 2 ==="
echo "(On single-DataNode labs this will show 'under-replicated' —"
echo " that's the expected, teachable outcome: no 2nd DataNode to copy to.)"
hdfs dfs -setrep -w 2 "$HDFS_FILE" || true

echo
echo "=== Full cluster report — see Live datanodes + block distribution ==="
hdfs dfsadmin -report
