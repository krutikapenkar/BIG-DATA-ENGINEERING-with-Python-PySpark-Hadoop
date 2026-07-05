# Module 2 — HDFS & YARN Deep Dive

Needs the WSL2 Hadoop cluster from [`00-environment-setup`](../00-environment-setup/) up and running (`jps` should show NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager).

---

## Session 2.1 — HDFS Architecture

### Roles

| Component | Role | If it fails |
|---|---|---|
| **NameNode** | Holds the filesystem namespace and block→DataNode mapping in memory. Every read/write asks it "where are the blocks for this file?" | Cluster-wide outage until restarted, or a Standby NameNode (HA) takes over |
| **DataNode** | Stores the actual block data on disk, heartbeats every few seconds | Marked dead after missed heartbeats; blocks re-replicated elsewhere |
| **Secondary NameNode** | Periodically merges the edit log into a checkpoint | **Not a backup** — its absence just means a slower NameNode restart, not data loss. Most misunderstood component in HDFS. |

### Block storage & replication

- Default block size: **128 MB** — a file splits into `ceil(size / 128MB)` blocks.
- Default replication factor: **3** — each block lives on 3 different DataNodes. On this single-node lab cluster it's set to 1 (see `hdfs-site.xml`), since 3 copies on 1 disk buy nothing.

### HDFS CLI

```bash
hdfs dfs -mkdir -p /data/orders
hdfs dfs -put orders.csv /data/orders/
hdfs dfs -ls /data/orders/
hdfs dfs -cat /data/orders/orders.csv
hdfs dfs -du -h /data/orders/
hdfs dfs -get /data/orders/orders.csv ./downloaded.csv
hdfs dfs -mv /data/orders/orders.csv /data/orders/orders_v1.csv
hdfs dfs -chmod 755 /data/orders
hdfs dfs -rm /data/orders/orders_v1.csv
hdfs fsck /data/orders -files -blocks -locations   # see actual block placement
```

Full command set with explanations: [`code/session-2.1-hdfs/hdfs_cli_commands.sh`](code/session-2.1-hdfs/hdfs_cli_commands.sh)

### PySpark ↔ HDFS

```python
df = spark.read.option("header", True).csv("hdfs:///data/orders/orders.csv")
df.write.mode("overwrite").parquet("hdfs:///data/output/orders_parquet")
```

The triple slash (`hdfs:///...`) is what tells Spark to read from HDFS instead of local disk.

**Hands-on:** [`code/session-2.1-hdfs/`](code/session-2.1-hdfs/) — upload the sample dataset, read it with PySpark, transform, write back in CSV/Parquet/JSON, and inspect block placement with `hdfs fsck`.

---

## Session 2.2 — YARN & Cluster Execution

### Components

| Component | Role |
|---|---|
| **ResourceManager** | Cluster-wide scheduler — 1 per cluster. Decides who gets how much CPU/RAM. |
| **NodeManager** | Runs on every worker node — launches and monitors containers, heartbeats back to the RM. |
| **ApplicationMaster** | Per-job process the RM launches. For Spark, this *is* the driver in cluster mode. |

### Client mode vs. cluster mode

| | Client | Cluster |
|---|---|---|
| Driver runs | On your machine | Inside a YARN container |
| Logs | Your terminal | `yarn logs -applicationId <id>` |
| Use case | Development/debugging | Production |

### spark-submit

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 2g \
  --num-executors 2 \
  --executor-cores 2 \
  my_job.py
```

| Flag | Meaning |
|---|---|
| `--master yarn` | Submit to the YARN cluster instead of `local[*]` |
| `--deploy-mode` | `client` or `cluster` (above) |
| `--executor-memory` | JVM heap per executor |
| `--executor-cores` | vCores per executor |
| `--num-executors` | Total executor count |

### Partitions & parallelism

Every HDFS block becomes roughly one Spark partition, and every partition becomes one task. Rule of thumb: partitions ≈ 2–4× total executor cores. Too few and most of the cluster idles; too many and scheduling overhead dominates.

```python
print(df.rdd.getNumPartitions())
df2 = df.repartition(8)   # full shuffle, evens out data
df3 = df.coalesce(2)      # no shuffle, merge only — use before writing small output
```

### Monitoring

- **YARN ResourceManager UI** — http://localhost:8088 — application state, container allocation, logs
- **Spark UI** — http://localhost:4040 (client mode) — Jobs, Stages, Tasks, Executors

```bash
yarn application -list
yarn application -status <application_id>
yarn logs -applicationId <application_id>
```

**Hands-on:** [`code/session-2.2-yarn/`](code/session-2.2-yarn/) — a production-shaped batch job (`pyspark_batch_job.py`), the full `spark-submit` command progression, and a partitions/parallelism demo including a deliberately skewed dataset.
