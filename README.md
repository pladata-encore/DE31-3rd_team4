# DE31-3rd_team5
## Objective
The objective of this mini-project is to build fundamental backgrounds for final project during 31st Data Engineering Course of Playdata Coding Bootcamp.

## Overview on Project Architecture
![image](./attachments/overall.png)

|Framework|Purpose on Usage|
|---|---|
|Python|Basic Language used during project|
|HDFS|Data Lake on clustered computers|
|Kafka|For buffering large data to be delivered into HDFS for sure.|
|Spark|Executing EDA on large data under distributed computing environment.|
|MySQL(MariaDB)|For storing analyzed data and gain fast access.|
|Airflow|For automating each data crawl-store-analysis processes.|

## Environment Summary
- Python 3.10.12
- Spark 3.5.1
- Hadoop 3.3.6
- Kafka 2.13-3.2
- mariadb-lts 11.2.4-1

## Installation
### 1. Hadoop
The Hadoop cluster used within project is pre-built cluster; thus I won't explain about ways to instal hadoop and for cluster on this document.

Try following this [official tutorial](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) for quick start!

### 2. Spark
We combined 2 machines with 16GB RAM and 4 core, 1 machine with 12GB RAM and 4 core to form a Spark Cluster; but I recommend using machines with more RAM for better envoironment and computing power.

Installation process was rather simple.
1) Download Spark tar file from [official website](https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz)

You can also do this on terminal using **wget**.

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
```
2) Unzip the downloaded tar file and locate it to somewhere you'll be okay for it to be at.

For example, I renamed unzipped file in simple "spark" and placed it under /opt/ directory.

You can use **tar** command and **mv** command on this process.

```bash
tar xvfz spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark/
sudo chown -R hadoop:hadoop /opt/spark/
```

Remember to change it's ownership to account you current are trying to use spark.

3) Set up environment variables.

To use spark commands and shell codes in terminal, it is needed for bash profile to have these environment variables.

```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/sbin
```

Also, for more easier usage, apply these aliases on bash profile.

```bash
alias spark_start="/opt/spark/sbin/start-all.sh"
alias spark_stop="/opt/spark/sbin/stop-all.sh"
```

With earlier aliases applied, it will be possible to simply type **spark_start** for whole spark cluster to start, and **spark_stop** to stop whole spark cluster.

4) Set up configuration files

Apache spark reads it's configurations from *spark-env.sh* and *spark-defaults.conf*, and *workers* file for designating worker computers.

In this project, only spark-env.sh file was used for configuration.

```bash
cd /opt/spark/conf
cp spark-env.sh.template spark-env.sh
```

By default there are templates for configuration files.

You can also add options for customization according to these templates.

```bash
vim /opt/spark/conf/spark-env.sh
```

Since ownership of the directory /opt/spark has been changed to current working account, it should be possible to open vim without sudo authority.

Apply given options to spark-env.sh

```sh
export SPARK_MASTER_HOST=master
export SPARK_MASETR_PORT=7077
export SPARK_MASTER_WEBUI=8080
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=8g
export SPARK_WORKER_INSTANCES=1
```

Also, you'll need to make sure each nodes that constitutes spark cluster can communicate without extra authentification.

Thus, make sure each nodes have others added to authorized_keys by their own ssh keys.

If you want to designate each nodes with names or urls other than plain IPv4 addresses, please edit /etc/hosts first in order to do so.

The master of a spark cluster will designate it's workers with file named **workers** under conf/ directory.

```bash
vim /opt/spark/conf/workers
```

In this project I used 2 workers named node1, node2.

thus workers file will only have these lines.

```sh
node1
node2
```

5) Run and test spark cluster.

You'll also need to check your firewall configurations to make sure each ports spark cluster use is open and available for connection, and each workers alive and able to connect.

### 3. Airflow

Python environment depends on its packages installed, thus virtual envionment is used within this project

In this project, Python 3.10.12 was used.

I won't explain about installing python in this document, and I'll just assume it is installed.

1) Generate virtual environment.

You can just use conda or miniconda, but I didn't use them, so I'll explain according to virtual environment standards.

```bash
python3 -m venv .venv
```

2) Install Airflow using pip.

Activate virtual environment.

```bash
source .venv/bin/activate
```

Update pip to make sure you get latest airflow.

```bash
python -m pip install -U pip
```

Install Airflow

```bash
pip install apache-airflow
```

3) Set up configurations.

After installation, you'll get *airflow/* directory under your home directory.

By default, there is no *dags/* directory under airflow/ directory.

So make a directory.

```bash
mkdir ~/airflow/dags
```

You'll also need to export additional environment variable to your bash profile.

```bash
vim ~/.bashrc
```

Apply given environment variable to your bash profile.

```bash
export AIRFLOW_HOME=~/airflow
```

4) Run Airflow standalone for test.

The Airflow is also runnable under distributed computation environment, but in this project it is used in standalone mode.

The purpose of using Airflow is to manipulate and run sources on other computer using python subprocess and ssh commands.

If your configuration is applied along with your updated bash profile, it'll be able to activate Airflow standalone mode.

```bash
airflow standalone
```

Example codes used for setting up DAGs is located in [here](./airflow_sample/dag_example.py).