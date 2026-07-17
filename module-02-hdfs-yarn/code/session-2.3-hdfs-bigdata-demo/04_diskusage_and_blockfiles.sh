#!/bin/bash
# =============================================================================
# Session 2.3 — Step 4: See the actual block files sitting on local disk
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# This is the "prove it" step. Everything so far has been through HDFS
# commands, which could feel like a black box to students. Here we go
# BELOW HDFS to the DataNode's actual storage directory on the Linux
# filesystem and show that HDFS blocks are just ordinary files (blk_*)
# living on disk — HDFS is a layer of bookkeeping on top of a normal
# filesystem, not magic.
#
# Set DATANODE_DIR to match your hdfs-site.xml dfs.datanode.data.dir
# (see Module2_Complete_Guide.md section 1.4 in the parent module folder).
# =============================================================================

set -euo pipefail

HDFS_DIR="/user/training/module02_bigdata"
DATANODE_DIR="${DATANODE_DIR:-/usr/local/hadoop/data/dataNode}"

echo "=== HDFS-level view: how big does HDFS think this data is? ==="
hdfs dfs -du -h "$HDFS_DIR"

echo
echo "=== Local disk view: the DataNode's real block files ==="
echo "(looking under: $DATANODE_DIR)"
find "$DATANODE_DIR" -name "blk_*" -not -name "*.meta" 2>/dev/null | head -20

echo
echo "Each 'blk_*' file above is one physical chunk of transactions.csv."
echo "The NameNode holds the metadata (which blk_* files, in what order,"
echo "belong to which filename) that stitches these chunks back into 'one file'"
echo "whenever a client reads it."
