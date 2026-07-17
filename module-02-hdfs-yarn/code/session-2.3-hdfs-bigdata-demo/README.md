# Session 2.3 — HDFS Blocks, Replication & Fault Tolerance with Big Data

## Big Data Engineering with Python, PySpark & Hadoop — Module 02

This session goes one level deeper than [`session-2.1-hdfs/`](../session-2.1-hdfs/).
That session teaches the HDFS *CLI* using a tiny sample CSV. That's fine for
learning commands, but a small file never actually gets split into multiple
blocks — so students never *see* the core idea that gives HDFS its name: a big
file gets broken into blocks and spread across DataNodes, with the NameNode
keeping track of where everything is.

This session fixes that by generating a genuinely large dataset and walking through
six scripts that make block-splitting, replication, and fault tolerance visible and
provable, not just theoretical.

**Prerequisite:** a working single-node (pseudo-distributed) Hadoop cluster — see
[`00-environment-setup`](../../../00-environment-setup/) if it's not installed yet.
Run `jps` first and confirm you see `NameNode`, `DataNode`, `SecondaryNameNode`.

---

## Files in this folder

| File | Purpose |
|------|---------|
| `01_generate_bigdata.sh` | Creates the synthetic transactions dataset (size configurable) |
| `02_upload_and_split_blocks.sh` | Uploads to HDFS, forces block splitting, reveals block layout |
| `03_replication_demo.sh` | Shows replication factor and cluster-wide report |
| `04_diskusage_and_blockfiles.sh` | Drops below HDFS to show the raw block files on local disk |
| `05_wordcount_mapreduce.sh` | Runs a real distributed MapReduce job over the big data |
| `06_failure_simulation.sh` | Kills the DataNode and shows what happens to a running cluster |
| `data/transactions.csv` | Ready-made sample dataset (~100,000 rows, ~6MB) — generated already, safe to re-run step 1 to regenerate/resize |

---

## Step-by-step walkthrough

### Step 1 — Generate the big dataset (`01_generate_bigdata.sh`)

```bash
bash 01_generate_bigdata.sh
```

**What it does:** builds a synthetic e-commerce transactions CSV
(`transaction_id, student_id, product, category, amount, city, transaction_date`)
using `awk`, so generating even millions of rows is fast.

**Why it matters:** HDFS's whole value proposition — splitting files into blocks,
distributing them, replicating them — is invisible on small files. This step gives
you a dial (`ROWS` env var) to make the file as big as you need:

```bash
ROWS=5000000 bash 01_generate_bigdata.sh   # a few hundred MB — do this on the real Ubuntu box
```

A sample file is already generated at `data/transactions.csv` (100,000 rows, ~6MB)
so you can run steps 2-6 immediately without waiting on generation.

### Step 2 — Upload and force block splitting (`02_upload_and_split_blocks.sh`)

```bash
bash 02_upload_and_split_blocks.sh
```

**What it does:** creates `/user/training/module02_bigdata/raw_data` in HDFS,
uploads `transactions.csv` with `dfs.blocksize` overridden to 16MB (instead of the
default 128MB) just for this upload, then runs `hdfs dfs -ls` and `hdfs fsck -files
-blocks -locations` on it.

**Why it matters:** `-ls` shows the NameNode's *logical* view — one file, like any
normal filesystem. `fsck` shows the *physical* reality — many numbered blocks, each
one recorded against a specific DataNode. This is the single clearest way to make
"a file is really made of blocks" click for students: have them count the `blk_`
entries in the `fsck` output and check it roughly matches `file_size ÷ 16MB`.

### Step 3 — Replication (`03_replication_demo.sh`)

```bash
bash 03_replication_demo.sh
```

**What it does:** checks the file's current replication factor, checks the
cluster-wide default (`dfs.replication`), attempts to raise it to 2, and prints
`hdfs dfsadmin -report`.

**Why it matters:** replication is HDFS's answer to "what happens when a disk or
machine dies?" — every block normally has 3 copies on different DataNodes in
production. On this single-node lab there's only one DataNode, so replication is
capped at 1, and asking for 2 will show as "under-replicated." **Tell students this
is expected** — it's the setup's limitation, not HDFS's. If a multi-node cluster is
available, re-running this script there is the payoff: watch the replica count
actually reach 2 or 3.

### Step 4 — Look at the real block files (`04_diskusage_and_blockfiles.sh`)

```bash
bash 04_diskusage_and_blockfiles.sh
```

**What it does:** shows `hdfs dfs -du -h` (the HDFS-level size) next to a `find`
over the DataNode's actual local storage directory (`dfs.datanode.data.dir`),
listing the real `blk_*` files sitting on disk.

**Why it matters:** this is the "prove it's not magic" step. HDFS blocks are just
ordinary files on a normal Linux filesystem — the NameNode is the piece of
bookkeeping that remembers which `blk_*` files, in which order, make up
`transactions.csv`. If your `dfs.datanode.data.dir` differs from the default,
override it: `DATANODE_DIR=/your/path bash 04_diskusage_and_blockfiles.sh`.

### Step 5 — Run a real distributed job (`05_wordcount_mapreduce.sh`)

```bash
bash 05_wordcount_mapreduce.sh
```

**What it does:** runs Hadoop's built-in WordCount MapReduce example over the
uploaded data and prints the top 20 most frequent tokens.

**Why it matters:** steps 2-4 showed *where* data lives; this step shows *why* that
matters. A MapReduce job schedules roughly one map task per input block. On a
single-node lab every task happens to execute on the same machine, but the
framework doesn't special-case that — it's exactly the same mechanism that
parallelizes work across real DataNodes in a multi-node cluster.

### Step 6 — Simulate failure (`06_failure_simulation.sh`)

```bash
bash 06_failure_simulation.sh
```

**What it does:** confirms the cluster is healthy, stops the DataNode daemon,
attempts to read the file, then restarts the DataNode.

**Why it matters:** this is the payoff for the whole session. Because replication
is capped at 1 on this single-node lab, killing the one DataNode makes the file
briefly unreadable — which is exactly the point. It's the strongest possible
argument for why production clusters run replication=3 across separate physical
machines: **use this failure as motivation**, not as a flaw. If you have access to
a multi-node cluster, run this same script there with replication=3 and the read
will succeed even with a DataNode down — a great before/after comparison for
students.

---

## Suggested lecture flow

1. Run steps 1-2 live, pause on the `fsck` output — have students count blocks.
2. Run step 3, discuss what "under-replicated" would mean with a 2nd/3rd DataNode.
3. Run step 4 — this is usually the "oh, it's just files!" moment.
4. Run step 5 and connect block count → map task count.
5. Run step 6 last, as the dramatic close — kill the DataNode, watch the read fail,
   restart it, and pivot into "this is why real clusters have 3+ DataNodes."
