# Module 4 — Cluster Monitoring with Ambari

Ambari is a web-based management and monitoring layer on top of a Hadoop cluster. There's no code to run in this module — it's entirely about reading a cluster's health from a dashboard instead of piecing it together from CLI commands and log files.

## What Ambari gives you

| Without Ambari | With Ambari |
|---|---|
| SSH into each node to check service status | One dashboard shows every service's state |
| Hand-edit XML config files, restart services manually | Configure services from the UI, restart with one click |
| No history of what changed and when | Full config change history, with rollback |
| Guess which node is overloaded | Heatmap view shows load across every host at a glance |

## Dashboard layout

- **Services panel** — left-hand list of every installed service (HDFS, YARN, MapReduce2, ZooKeeper, etc.) with a health indicator per service.
- **Metric widgets** — the main dashboard area: HDFS disk usage, DataNodes live count, YARN memory/vCores used, CPU load, and more, each as a small chart.
- **Metrics tab** — click into any widget for its full historical graph.
- **Heatmaps tab** — a grid of every host in the cluster, colored by a chosen metric (disk usage, CPU, etc.) — the fastest way to spot one overloaded node among many.
- **Config History tab** — a log of every configuration change ever made to any service, with the ability to roll back to a previous version.

## Key metrics worth knowing

| Metric | What it tells you |
|---|---|
| HDFS disk usage | Total capacity used vs. available — the number that tells you when you need more DataNodes |
| DataNodes live | Should always equal your total DataNode count — a drop means a node is down |
| NameNode heap | JVM memory used by the NameNode — it holds *all* file metadata in RAM, so this is a hard ceiling on how many files the cluster can hold |
| NameNode RPC queue length | Client requests waiting to be processed — a growing queue means the NameNode is a bottleneck |
| YARN memory/vCores used vs. total | Cluster-wide resource utilization — tells you if jobs are being queued because the cluster is full |

## Practical walkthrough (if you have a cluster with Ambari installed)

1. Open the dashboard (typically `http://<cluster-host>:8080`).
2. Check the Services panel — confirm HDFS and YARN both show green/healthy.
3. Open the Heatmaps tab, select "HDFS Bytes Read" or similar, and identify the busiest node.
4. Submit a Spark job from Module 2 (`spark-submit --master yarn ...`) and watch the YARN memory/vCores widgets move in real time.
5. Open Config History and find the last change made to any service — note what changed and when.

## Where this fits

Module 2 taught you what NameNode/DataNode/ResourceManager/NodeManager *are*. Ambari is where you'd actually watch them in a real, multi-node production cluster — the CLI tools (`hdfs dfsadmin -report`, `yarn application -list`) still work underneath, but a dashboard is how you'd realistically monitor a cluster with more than one or two machines.
