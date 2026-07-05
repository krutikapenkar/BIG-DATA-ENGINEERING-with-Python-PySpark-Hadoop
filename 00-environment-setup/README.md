# Environment Setup

Two separate setups, used at different points in the course:

1. **Local PySpark** — needed for Module 1 only. Runs as a single process (`local[*]`), no Hadoop involved.
2. **WSL2 Hadoop cluster** — needed from Module 2 onward. A real single-node HDFS + YARN cluster running inside WSL2 Ubuntu.

---

## 1. Local PySpark (Module 1)

Works on Windows, Mac, or Linux directly — no VM needed.

```bash
# 1. Java (Spark runs on the JVM)
# Windows: install Temurin JDK 11 from adoptium.net, then verify:
java -version

# 2. Python 3.8–3.11
python --version
pip --version

# 3. PySpark
pip install pyspark

# 4. Verify
python -c "import pyspark; print(pyspark.__version__)"
```

**Quick test:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EnvironmentCheck") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)
spark.createDataFrame([("hello", 1)], ["word", "count"]).show()
spark.stop()
```

If that prints a version and a small table, you're ready for Module 1.

**Common issues**
| Symptom | Fix |
|---|---|
| `JAVA_HOME` not set | Set it as a system environment variable, pointing at the JDK install folder (not `bin`) |
| `'pyspark' not recognized` | Add `%SPARK_HOME%\bin` (or your pip install's Scripts folder) to PATH, restart terminal |
| `NullPointerException` on Windows at startup | Missing/mismatched `winutils.exe` — only needed if you installed the Spark binary manually instead of via `pip install pyspark` |

**Alternative — zero local install:** [Databricks Community Edition](https://community.cloud.databricks.com) gives you a real Spark cluster in the browser for free, with a `spark` session already created. Good fallback if local setup is fighting you.

---

## 2. WSL2 Hadoop cluster (Module 2 onward)

A real pseudo-distributed cluster — actual NameNode, DataNode, ResourceManager, and NodeManager processes — running inside WSL2 Ubuntu. This is what Module 2's HDFS/YARN concepts need; `local[*]` cannot provide it.

### 2.1 Install WSL2 + Ubuntu

In an **administrator** PowerShell window:

```powershell
wsl --install -d Ubuntu-24.04
wsl --set-default-version 2
wsl -l -v   # confirm Ubuntu-24.04 shows VERSION 2
```

First launch asks you to create a Linux username/password (separate from your Windows login).

### 2.2 Base packages (inside Ubuntu)

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk openssh-server
java -version
```

### 2.3 Passwordless SSH to localhost

Hadoop's start scripts SSH into worker nodes — even a single-node cluster SSHes into `localhost`.

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost   # should log in without a password prompt
```

### 2.4 Install Hadoop

```bash
wget https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

Add to `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

```bash
source ~/.bashrc
```

### 2.5 Configure Hadoop

`$HADOOP_HOME/etc/hadoop/core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

`hdfs-site.xml`:
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/usr/local/hadoop/data/nameNode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/usr/local/hadoop/data/dataNode</value>
  </property>
</configuration>
```

`mapred-site.xml`:
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

`yarn-site.xml`:
```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
</configuration>
```

### 2.6 Format and start

```bash
hdfs namenode -format     # only once, ever
start-dfs.sh
start-yarn.sh
jps   # should list: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

- NameNode UI: http://localhost:9870
- ResourceManager UI: http://localhost:8088

### 2.7 Install Spark + PySpark

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /usr/local/spark
```

Add to `~/.bashrc`:
```bash
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
```

```bash
source ~/.bashrc
pyspark --version
```

You now have `hdfs`, `yarn`, and `spark-submit` all working against a real local cluster — everything Module 2 onward needs.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `jps` missing NameNode | Check `$HADOOP_HOME/logs/*namenode*.log`; a stale lock from a previous crash is the usual cause |
| `ssh localhost` asks for a password | `~/.ssh/authorized_keys` permissions must be `600` |
| `spark-submit --master yarn` hangs at ACCEPTED | NodeManager not registered — check http://localhost:8088, usually a missing `yarn.nodemanager.aux-services` |
| `Connection refused` on port 9000 | HDFS isn't running — `start-dfs.sh`, then check with `jps` |
