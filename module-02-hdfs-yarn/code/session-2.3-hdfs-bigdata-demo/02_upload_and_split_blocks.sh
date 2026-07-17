#!/bin/bash
# =============================================================================
# Session 2.3 — Step 2: Upload big data and force it to split into many blocks
# Big Data Engineering Training — Module 02
# =============================================================================
# WHY THIS STEP EXISTS:
# HDFS's default block size is 128MB. Our demo file (even at a few hundred
# MB) might only become 2-3 blocks — not dramatic enough for students to
# "see" splitting happening. So here we override dfs.blocksize to a small
# 16MB just for this upload, forcing the file into many blocks. This is
# purely a teaching trick — in real clusters you'd keep the 128MB default.
#
# CONCEPT TIE-BACK:
# - hdfs dfs -ls only ever shows "one file" — that's the NameNode's logical
#   view (metadata: name, size, permissions, block list).
# - hdfs fsck reveals what's physically true: many numbered blocks, each
#   with an ID and the DataNode(s) currently holding it.
# =============================================================================

set -euo pipefail

LOCAL_FILE="$(cd "$(dirname "$0")" && pwd)/data/transactions.csv"
HDFS_DIR="/user/training/module02_bigdata/raw_data"

echo "=== 1) Create the target HDFS directory ==="
hdfs dfs -mkdir -p "$HDFS_DIR"

echo
echo "=== 2) Upload with a small (16MB) block size to force multiple blocks ==="
hdfs dfs -D dfs.blocksize=16777216 -put -f "$LOCAL_FILE" "$HDFS_DIR/"

echo
echo "=== 3) NameNode's logical view — looks like just one file ==="
hdfs dfs -ls -h "$HDFS_DIR"

echo
echo "=== 4) Physical reality — the actual blocks and where they live ==="
hdfs fsck "$HDFS_DIR/transactions.csv" -files -blocks -locations

echo
echo "Ask students to count the 'blk_' entries above and match that count"
echo "against file_size / 16MB — that arithmetic IS the block-splitting concept."
