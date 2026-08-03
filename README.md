# Big Data Engineering with Python, PySpark & Hadoop

A practical, module-wise course covering the Hadoop ecosystem, HDFS, YARN, and PySpark — from first principles to running real distributed jobs. Each module folder contains notes and runnable code; hands-on exercises are meant to be done, not just read.

## Syllabus at a glance

| Module | Topic | Duration | Environment |
|---|---|---|---|
| [00](00-environment-setup/) | Environment setup | — | Local machine + WSL2 |
| [01](module-01-foundations/) | Foundations of Big Data & Hadoop Ecosystem | 4 hrs | Local (`local[*]`) |
| [02](module-02-hdfs-yarn/) | HDFS & YARN Deep Dive | 4 hrs | Hadoop cluster (WSL2) |
| [03](module-03-rdd-dataframes/) | PySpark Core — RDDs & DataFrames | 6 hrs | Local or cluster |
| [04](module-04-ambari/) | Cluster Monitoring with Ambari | — | Hadoop cluster + Ambari |

## Enterprise projects (trainer-led)

Beyond the modules above, two trainer-executed live-demonstration
projects give students a real-world feel of production Big Data systems.
Students observe, discuss, review the code, and receive the full
codebase.

| Project | Domain | Duration | Technologies |
|---|---|---|---|
| [1](enterprise-projects/project-1-ecommerce-analytics/) | E-Commerce Customer Behaviour & Revenue Analytics | 5 hrs / 2-3 sessions | PySpark, Spark SQL, Delta Lake, Kafka + Structured Streaming, Airflow |

Each project folder has its own `README.md` (architecture, how to run),
`notes.md` (concepts), and `session_plan.md` (the trainer's session-by-session script).

## How to use this repo

1. Do **[00-environment-setup](00-environment-setup/)** first — it gets PySpark running locally for Module 1, and a real single-node Hadoop cluster (HDFS + YARN) running in WSL2 for Module 2 onward.
2. Each module has a `notes.md` (concepts, explained plainly) and a `code/` folder (scripts you actually run, not just read).
3. Module 1 includes the full lecture deck (`.pptx`) for the theory portion — every other module is notes + code only.
4. Run things in order — Module 2's scripts assume the cluster from `00-environment-setup` is up, and later scripts assume earlier HDFS paths already exist.

## Why the environment changes partway through

Module 1 only needs the PySpark DataFrame API, which runs fine as a single local process on any OS — no cluster required. Module 2 is specifically about HDFS and YARN internals (NameNode, DataNode, ResourceManager, NodeManager) — those are real background daemons, which means a real Linux environment. That's why setup starts local and moves to WSL2 partway through.

## Prerequisites

- A laptop/desktop with 8 GB+ RAM (16 GB comfortable)
- Windows 10/11 with WSL2 available, or any Linux/Mac machine
- Basic Python familiarity — no prior Spark or Hadoop knowledge assumed
