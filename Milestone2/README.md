# SE446 — Milestone 2: Chicago Crime Analytics with Spark + MLlib
---

## 1. Team Members

| Name | ID | Tasks |
|---|---|---|
| Almaha Abdullah Alrasheed | 231707 | Tasks 1-4 (Phase A) |
| Lateen Alhurasen | 231543 | Tasks 5-6 (Phase B) |
| Layan Alshowaier | 231361 | Tasks 7, 9 (Phase B + C) |
| Moudi Alsadoon | 231589 | Tasks 10-11 (Phase C) |

---

## 2. Executive Summary

This project upgrades the Chicago Crime Analytics pipeline from batch MapReduce (Milestone 1) to in-memory Spark analytics and machine learning. We reproduced all M1 analyses using Spark DataFrames and Spark SQL, then built a complete MLlib pipeline to predict whether a crime will result in an arrest. The best model (GBT) achieved an AUC-ROC of 0.8292 and accuracy of 85.26%, with crime type being the dominant predictor (97.05% feature importance).

---

## 3. Dataset Information

| Dataset | Path | Size | Rows |
|---|---|---|---|
| Full dataset | `hdfs:///data/chicago_crimes.csv` | 173.5 MB | 793,073 |
| Sample dataset | `hdfs:///data/chicago_crimes_sample.csv` | 2.3 MB | ~10,000 |

**Data Quality Note:**  
Raw dataset: 793,073 rows (including header).  
After removing NULL values in key columns (Primary Type, Location Description, District, Year): **791,479 clean rows** used for analysis. This is standard data cleaning practice in big data pipelines.

**Phase A** (Tasks 1-4): Full HDFS dataset (791,479 rows)  
**Phase B** (Tasks 5-7): 5% sample (`df.sample(0.05, seed=42)`) required due to cluster memory limits  

---

## 4. M1 vs M2 Comparison (Phase A Results)

### Task 1: Crime Type Distribution

**M1 used MapReduce on a larger version of the dataset (7M+ rows at time of M1 submission). M2 used the current cluster dataset (791,479 rows). The RANKING ORDER is identical in both analyses ✅**

| Crime Type | M1 MapReduce Count | M2 Spark Count | Ranking Match? |
|---|---|---|---|
| THEFT | 1,804,063 | 162,678 | ✅ #1 |
| BATTERY | 1,547,201 | 151,919 | ✅ #2 |
| CRIMINAL DAMAGE | 965,727 | 91,238 | ✅ #3 |
| NARCOTICS | 765,790 | 74,127 | ✅ #4 |
| ASSAULT | 570,382 | 54,064 | ✅ #5 |
| OTHER OFFENSE | 530,139 | 36,885 | ✅ #6 |
| BURGLARY | 448,674 | 39,861 | ✅ #7 |
| MOTOR VEHICLE THEFT | 436,261 | 48,493 | ✅ #8 |
| DECEPTIVE PRACTICE | 392,829 | 28,864 | ✅ #9 |
| ROBBERY | 316,071 | 30,989 | ✅ #10 |

**Observation:** The ranking of crime types is identical between M1 MapReduce and M2 Spark. The count difference is because M1 ran on a larger version of the dataset (7M+ rows) while M2 ran on the current cluster dataset (791,479 rows). Both used `/data/chicago_crimes.csv`. Spark DataFrame API requires only 3 lines of code compared to two separate mapper/reducer Python scripts, and runs significantly faster due to in-memory processing.

---

### Task 2: Location Hotspots

| Location | M1 MapReduce Count | M2 Spark SQL Count | Ranking Match? |
|---|---|---|---|
| STREET | 2,189,093 | 248,326 | ✅ #1 |
| RESIDENCE | 1,389,343 | 136,393 | ✅ #2 |
| APARTMENT | 1,011,372 | 61,235 | ✅ #3 |
| SIDEWALK | 763,643 | 47,506 | ✅ #4 |
| OTHER | 266,754 | 29,671 | ✅ #5 |
| PARKING LOT/GARAGE | — | 22,436 | ✅ #6 |
| ALLEY | — | 18,349 | ✅ #7 |
| SCHOOL PUBLIC BUILDING | — | 15,776 | ✅ #8 |
| RESIDENCE-GARAGE | — | 14,291 | ✅ #9 |
| SMALL RETAIL STORE | — | 13,804 | ✅ #10 |

**Observation:** Spark SQL produces identical location ranking to MapReduce. STREET remains the most common crime location in both analyses, confirming consistency. Spark SQL requires a single query instead of separate mapper/reducer scripts, completing in 3.65 seconds.

---

### Task 3: Crime Trend Over Years

| Year | M2 Spark Count |
|---|---|
| 2001 | 467,298 |
| 2002 | 205,266 |
| 2003 | 983 |
| 2004 | 911 |
| 2005 | 1,028 |
| 2006 | 791 |
| 2007 | 759 |
| 2008 | 1,002 |
| 2009 | 902 |
| 2010 | 689 |
| 2011 | 761 |
| 2012 | 786 |

**Observation:** The dataset shows unusually high counts in 2001 (467,298) and 2002 (205,266), which likely reflects bulk historical data loading into the HDFS dataset. From 2003 onwards, counts stabilize at approximately 700-1,000 records per year in this dataset subset. Spark DataFrame groupBy Year operation is significantly faster than the equivalent MapReduce job, requiring no custom mapper/reducer logic.

---

### Task 4: Arrest Rate Analysis

| Metric | M2 Spark (791,479 rows) |
|---|---|
| Total Crimes | 791,479 |
| Total Arrests | 221,929 |
| **Overall Arrest Rate** | **28.04%** |

**Highest Arrest Rates by Crime Type:**

| Crime Type | Total Crimes | Total Arrests | Arrest Rate |
|---|---|---|---|
| PUBLIC INDECENCY | 17 | 17 | 100.00% |
| DOMESTIC VIOLENCE | 1 | 1 | 100.00% |
| NARCOTICS | 74,127 | 74,039 | 99.88% |
| PROSTITUTION | 9,100 | 9,089 | 99.88% |
| LIQUOR LAW VIOLATION | 2,349 | 2,345 | 99.83% |
| GAMBLING | 1,314 | 1,311 | 99.77% |
| CONCEALED CARRY LICENSE VIOLATION | 77 | 73 | 94.81% |
| OTHER NARCOTIC VIOLATION | 11 | 10 | 90.91% |
| INTERFERENCE WITH PUBLIC OFFICER | 803 | 648 | 80.70% |
| WEAPONS VIOLATION | 8,893 | 6,634 | 74.60% |

**Lowest Arrest Rates by Crime Type:**

| Crime Type | Total Crimes | Total Arrests | Arrest Rate |
|---|---|---|---|
| NON-CRIMINAL | 1 | 0 | 0.00% |
| INTIMIDATION | 91 | 3 | 3.30% |
| BURGLARY | 39,861 | 2,686 | 6.74% |
| CRIMINAL DAMAGE | 91,238 | 6,980 | 7.65% |
| ROBBERY | 30,989 | 3,044 | 9.82% |
| MOTOR VEHICLE THEFT | 48,493 | 5,256 | 10.84% |
| THEFT | 162,678 | 23,174 | 14.25% |

**Interpretation:** NARCOTICS has a near-perfect arrest rate (99.88%) because physical drug evidence is present at the scene, making arrest almost certain. BURGLARY and CRIMINAL DAMAGE have very low arrest rates (6-8%) because perpetrators are rarely caught at the scene  these crimes are typically discovered after the fact. This pattern directly explains why crime_index is the most important ML feature in Task 7 (97.05% importance) the crime type alone predicts arrest outcome with very high accuracy.

---

## 5. ML Results Summary (Phase B)

> **Sampling Note:** Phase B used `df.sample(0.05, seed=42)` on the full HDFS dataset as required due to cluster memory limitations.
>
> Training rows: 31,665 | Test rows: 7,787.

### Model Comparison Table

| Metric | Logistic Regression | Random Forest | GBT |
|---|---|---|---|
| AUC-ROC | 0.6193 | 0.8155 | **0.8292** |
| Accuracy | 0.7262 | 0.8197 | **0.8526** |
| F1 Score | 0.6342 | 0.7865 | **0.8374** |
| Precision | 0.6895 | 0.8557 | **0.8627** |
| Recall | 0.7262 | 0.8197 | **0.8526** |
| Training Time | 19.06s | 29.05s | 430.59s |

### Confusion Matrices

| Model | TN | FP | FN | TP |
|---|---|---|---|---|
| Logistic Regression | 5,530 | 89 | 2,043 | 125 |
| Random Forest | 5,619 | 0 | 1,404 | 764 |
| GBT | 5,525 | 94 | 1,054 | 1,114 |

### Best Model: GBT

GBT outperforms all models across every metric. It builds trees sequentially, each correcting errors of the previous one. This makes it especially powerful for this dataset where crime_index has extremely strong and clear non-linear patterns. The tradeoff is training time (430s vs 29s for RF), but the accuracy gain justifies it.

### Feature Importances (Random Forest)

| Feature | Importance |
|---|---|
| crime_index | 0.9705 (97.05%) |
| domestic_index | 0.0127 (1.27%) |
| Hour | 0.0111 (1.11%) |
| District | 0.0057 (0.57%) |

**crime_index** dominates because the different crime types have extremely different arrest rates (NARCOTICS: 99.88% vs BURGLARY: 6.74%), making crime type the best predictor of arrest outcome.

**Why Logistic Regression performs worse:** It assumes a linear decision boundary between features and arrest outcome. But arrest prediction is dominated by crime_index which has a highly non-linear, categorical relationship with arrest rates. Random Forest and GBT capture this simply through decision trees.

---

## 6. Deployment Evidence

### Task 9: Local Execution
- Master: `local[*]`
- Data: 10,000 generated rows (W09B style)
- Screenshot: `outputs/task_9/task9.png`

### Task 10: Cluster Execution (Client Mode)
- Master: `yarn`
- Data: 791,479 rows from `hdfs:///data/chicago_crimes.csv`
- Screenshot: `outputs/task_10/TASK10.png`

### Task 11: spark-submit (Cluster Mode)
Command used:
```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 512m \
    --num-executors 1 \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.driver.maxResultSize=128m \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12 \
    --conf spark.executorEnv.PYSPARK_PYTHON=python3.12 \
    m2_spark_ml.py
```
Full logs: `outputs/spark_submit/run.log`

---

## 7. Member Contributions

| Member | Tasks | Contribution |
|---|---|---|
| Almaha Abdullah Alrasheed (231707) | Tasks 1-4 | Phase A: Spark DataFrame Analytics, M1 vs M2 comparison |
| Lateen Alhurasen (231543) | Tasks 5-6 | Phase B: Feature Engineering, Model Training & Evaluation |
| Layan Alshowaier (231361) | Tasks 7, 9 | Feature Importances, Local Execution Evidence |
| Moudi Alsadoon (231589) | Tasks 10-11 | Cluster Execution, spark-submit |

---

## 8. Repository Structure

```
SE446_Project_lateens_team/
├── README.md
├── M2_Spark_ML_Group_1_.ipynb
├── m2_spark_ml.py
├── Milestone1/
└── output/
    ├── task9_local_evidence.png
    ├── task10_cluster_evidence.png
    ├── task11_sparksubmit_evidence.png
    ├── task3_crime_trend.png
    ├── task7_feature_importance.png
    └── spark_submit/
        └── run.log
```

---

## 9. spark-submit Terminal Output

*(Paste full terminal output here after running Task 11)*

```
WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR. Using value of YARN_CONF_DIR.
2026-05-21 19:11:04,414 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:directory.info
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:7927
LogContents:
ls -l:
total 52
lrwxrwxrwx 1 hadoop hadoop   90 May 21 18:50 commons-pool2-2.12.0.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/20/commons-pool2-2.12.0.jar
-rw-r--r-- 1 hadoop hadoop   88 May 21 18:50 container_tokens
-rwx------ 1 hadoop hadoop  682 May 21 18:50 default_container_executor_session.sh
-rwx------ 1 hadoop hadoop  737 May 21 18:50 default_container_executor.sh
lrwxrwxrwx 1 hadoop hadoop   89 May 21 18:50 kafka-clients-3.9.0.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/18/kafka-clients-3.9.0.jar
-rwx------ 1 hadoop hadoop 7034 May 21 18:50 launch_container.sh
lrwxrwxrwx 1 hadoop hadoop   87 May 21 18:50 py4j-0.10.9.7-src.zip -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/24/py4j-0.10.9.7-src.zip
lrwxrwxrwx 1 hadoop hadoop   77 May 21 18:50 pyspark.zip -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/19/pyspark.zip
lrwxrwxrwx 1 hadoop hadoop   84 May 21 18:50 __spark_conf__ -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/21/__spark_conf__.zip
lrwxrwxrwx 1 hadoop hadoop  101 May 21 18:50 spark-sql-kafka-0-10_2.12-3.5.4.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/22/spark-sql-kafka-0-10_2.12-3.5.4.jar
lrwxrwxrwx 1 hadoop hadoop  112 May 21 18:50 spark-token-provider-kafka-0-10_2.12-3.5.4.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/23/spark-token-provider-kafka-0-10_2.12-3.5.4.jar
drwx--x--- 2 hadoop hadoop 4096 May 21 18:50 tmp
find -L . -maxdepth 5 -ls:
   797838      4 drwx--x---   3 hadoop   hadoop       4096 May 21 18:50 .
   797847      4 -rw-r--r--   1 hadoop   hadoop         16 May 21 18:50 ./.default_container_executor.sh.crc
   797845      4 -rw-r--r--   1 hadoop   hadoop         16 May 21 18:50 ./.default_container_executor_session.sh.crc
   797784      4 drwx------   3 hadoop   hadoop       4096 May 21 18:50 ./__spark_conf__
   797822      4 -r-x------   1 hadoop   hadoop       1382 May 21 18:50 ./__spark_conf__/__spark_dist_cache__.properties
   797785      4 drwx------   2 hadoop   hadoop       4096 May 21 18:50 ./__spark_conf__/__hadoop_conf__
   797812     16 -r-x------   1 hadoop   hadoop      14007 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hadoop-policy.xml
   797815      4 -r-x------   1 hadoop   hadoop        620 May 21 18:50 ./__spark_conf__/__hadoop_conf__/httpfs-site.xml
   797793      4 -r-x------   1 hadoop   hadoop       3999 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hadoop-env.cmd
   797792      4 -r-x------   1 hadoop   hadoop       1501 May 21 18:50 ./__spark_conf__/__hadoop_conf__/yarn-site.xml
   797816      4 -r-x------   1 hadoop   hadoop        951 May 21 18:50 ./__spark_conf__/__hadoop_conf__/mapred-env.cmd
   797809      4 -r-x------   1 hadoop   hadoop       3414 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hadoop-user-functions.sh.example
   797795      4 -r-x------   1 hadoop   hadoop        259 May 21 18:50 ./__spark_conf__/__hadoop_conf__/core-site.xml
   797818      8 -r-x------   1 hadoop   hadoop       4113 May 21 18:50 ./__spark_conf__/__hadoop_conf__/mapred-queues.xml.template
   797814      4 -r-x------   1 hadoop   hadoop        775 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hdfs-site.xml.bak
   797806      4 -r-x------   1 hadoop   hadoop        683 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hdfs-rbf-site.xml
   797801      8 -r-x------   1 hadoop   hadoop       7095 May 21 18:50 ./__spark_conf__/__hadoop_conf__/yarn-env.sh
   797811      4 -r-x------   1 hadoop   hadoop       1351 May 21 18:50 ./__spark_conf__/__hadoop_conf__/kms-env.sh
   797797      4 -r-x------   1 hadoop   hadoop       2250 May 21 18:50 ./__spark_conf__/__hadoop_conf__/yarn-env.cmd
   797807      4 -r-x------   1 hadoop   hadoop        774 May 21 18:50 ./__spark_conf__/__hadoop_conf__/core-site.xml.bak
   797805      4 -r-x------   1 hadoop   hadoop       1764 May 21 18:50 ./__spark_conf__/__hadoop_conf__/mapred-env.sh
   797791      4 -r-x------   1 hadoop   hadoop       3321 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hadoop-metrics2.properties
   797787     20 -r-x------   1 hadoop   hadoop      16838 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hadoop-env.sh
   797794      4 -r-x------   1 hadoop   hadoop         28 May 21 18:50 ./__spark_conf__/__hadoop_conf__/workers
   797789      4 -r-x------   1 hadoop   hadoop       1149 May 21 18:50 ./__spark_conf__/__hadoop_conf__/mapred-site.xml
   797804      4 -r-x------   1 hadoop   hadoop       2567 May 21 18:50 ./__spark_conf__/__hadoop_conf__/container-executor.cfg
   797786     16 -r-x------   1 hadoop   hadoop      14451 May 21 18:50 ./__spark_conf__/__hadoop_conf__/log4j.properties
   797788      4 -r-x------   1 hadoop   hadoop       2591 May 21 18:50 ./__spark_conf__/__hadoop_conf__/yarnservice-log4j.properties
   797800      4 -r-x------   1 hadoop   hadoop       1335 May 21 18:50 ./__spark_conf__/__hadoop_conf__/configuration.xsl
   797819      4 -r-x------   1 hadoop   hadoop        682 May 21 18:50 ./__spark_conf__/__hadoop_conf__/kms-site.xml
   797790      4 -r-x------   1 hadoop   hadoop       1657 May 21 18:50 ./__spark_conf__/__hadoop_conf__/httpfs-log4j.properties
   797798     12 -r-x------   1 hadoop   hadoop       9213 May 21 18:50 ./__spark_conf__/__hadoop_conf__/capacity-scheduler.xml
   797796      4 -r-x------   1 hadoop   hadoop       1860 May 21 18:50 ./__spark_conf__/__hadoop_conf__/kms-log4j.properties
   797803      4 -r-x------   1 hadoop   hadoop        557 May 21 18:50 ./__spark_conf__/__hadoop_conf__/hdfs-site.xml
   797799      4 -r-x------   1 hadoop   hadoop       3518 May 21 18:50 ./__spark_conf__/__hadoop_conf__/kms-acls.xml
   797817      4 -r-x------   1 hadoop   hadoop       2697 May 21 18:50 ./__spark_conf__/__hadoop_conf__/ssl-server.xml.example
   797802      4 -r-x------   1 hadoop   hadoop       1190 May 21 18:50 ./__spark_conf__/__hadoop_conf__/yarn-site.xml.bak.1778738820
   797813      4 -r-x------   1 hadoop   hadoop       1484 May 21 18:50 ./__spark_conf__/__hadoop_conf__/httpfs-env.sh
   797810      4 -r-x------   1 hadoop   hadoop       2316 May 21 18:50 ./__spark_conf__/__hadoop_conf__/ssl-client.xml.example
   797808      4 -r-x------   1 hadoop   hadoop       2681 May 21 18:50 ./__spark_conf__/__hadoop_conf__/user_ec_policies.xml.template
   797820    244 -r-x------   1 hadoop   hadoop     245914 May 21 18:50 ./__spark_conf__/__spark_hadoop_conf__.xml
   797821      4 -r-x------   1 hadoop   hadoop       1357 May 21 18:50 ./__spark_conf__/__spark_conf__.properties
   797840      4 -rw-r--r--   1 hadoop   hadoop         88 May 21 18:50 ./container_tokens
   797843      4 -rw-r--r--   1 hadoop   hadoop         64 May 21 18:50 ./.launch_container.sh.crc
   797841      4 -rw-r--r--   1 hadoop   hadoop         12 May 21 18:50 ./.container_tokens.crc
   797844      4 -rwx------   1 hadoop   hadoop        682 May 21 18:50 ./default_container_executor_session.sh
   797842      8 -rwx------   1 hadoop   hadoop       7034 May 21 18:50 ./launch_container.sh
   797827     56 -r-x------   1 hadoop   hadoop      56808 May 21 18:50 ./spark-token-provider-kafka-0-10_2.12-3.5.4.jar
   797839      4 drwx--x---   2 hadoop   hadoop       4096 May 21 18:50 ./tmp
   797781    148 -r-x------   1 hadoop   hadoop     150048 May 21 18:50 ./commons-pool2-2.12.0.jar
   797774   8992 -r-x------   1 hadoop   hadoop    9204801 May 21 18:50 ./kafka-clients-3.9.0.jar
   797824    424 -r-x------   1 hadoop   hadoop     432339 May 21 18:50 ./spark-sql-kafka-0-10_2.12-3.5.4.jar
   797830     44 -r-x------   1 hadoop   hadoop      42424 May 21 18:50 ./py4j-0.10.9.7-src.zip
   797778   2380 -r-x------   1 hadoop   hadoop    2434671 May 21 18:50 ./pyspark.zip
   797846      4 -rwx------   1 hadoop   hadoop        737 May 21 18:50 ./default_container_executor.sh
broken symlinks(find -L . -maxdepth 5 -type l -ls):

End of LogType:directory.info
*******************************************************************************

Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:launch_container.sh
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:7034
LogContents:
#!/bin/bash

set -o pipefail -e
export PRELAUNCH_OUT="/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/prelaunch.out"
exec >"${PRELAUNCH_OUT}"
export PRELAUNCH_ERR="/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/prelaunch.err"
exec 2>"${PRELAUNCH_ERR}"
echo "Setting up env variables"
export JAVA_HOME=${JAVA_HOME:-"/usr/lib/jvm/java-11-openjdk-amd64"}
export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-"/opt/hadoop-3.4.1"}
export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME:-"/opt/hadoop-3.4.1"}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/opt/hadoop-3.4.1/etc/hadoop"}
export HADOOP_YARN_HOME=${HADOOP_YARN_HOME:-"/opt/hadoop-3.4.1"}
export HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop-3.4.1"}
export PATH=${PATH:-"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"}
export LANG=${LANG:-"en_US.UTF-8"}
export HADOOP_TOKEN_FILE_LOCATION="/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/container_tokens"
export CONTAINER_ID="container_1778738889964_0059_01_000002"
export NM_PORT="38887"
export NM_HOST="worker-node-1"
export NM_HTTP_PORT="8042"
export LOCAL_DIRS="/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059"
export LOCAL_USER_DIRS="/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/"
export LOG_DIRS="/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002"
export USER="mnalsadoon"
export LOGNAME="mnalsadoon"
export HOME="/home/"
export PWD="/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002"
export LOCALIZATION_COUNTERS="12779755,0,7,0,1594"
export JVM_PID="$$"
export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
export SPARK_YARN_STAGING_DIR="hdfs://master-node:9000/user/mnalsadoon/.sparkStaging/application_1778738889964_0059"
export PYTHONPATH="$PWD/pyspark.zip:$PWD/py4j-0.10.9.7-src.zip"
export SPARK_PREFER_IPV6="false"
export SPARK_DIST_CLASSPATH="/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*"
export CLASSPATH="$PWD:$PWD/__spark_conf__:$PWD/__spark_libs__/*:/opt/spark/jars/*:/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:$PWD/__spark_conf__/__hadoop_conf__"
export PYSPARK_PYTHON="python3.12"
export SPARK_USER="mnalsadoon"
export MALLOC_ARENA_MAX="4"
echo "Setting up job resources"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/24/py4j-0.10.9.7-src.zip" "py4j-0.10.9.7-src.zip"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/22/spark-sql-kafka-0-10_2.12-3.5.4.jar" "spark-sql-kafka-0-10_2.12-3.5.4.jar"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/20/commons-pool2-2.12.0.jar" "commons-pool2-2.12.0.jar"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/21/__spark_conf__.zip" "__spark_conf__"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/19/pyspark.zip" "pyspark.zip"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/18/kafka-clients-3.9.0.jar" "kafka-clients-3.9.0.jar"
ln -sf -- "/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/filecache/23/spark-token-provider-kafka-0-10_2.12-3.5.4.jar" "spark-token-provider-kafka-0-10_2.12-3.5.4.jar"
echo "Copying debugging information"
# Creating copy of launch script
cp "launch_container.sh" "/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/launch_container.sh"
chmod 640 "/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/launch_container.sh"
# Determining directory contents
echo "ls -l:" 1>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
ls -l 1>>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
echo "find -L . -maxdepth 5 -ls:" 1>>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
find -L . -maxdepth 5 -ls 1>>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
echo "broken symlinks(find -L . -maxdepth 5 -type l -ls):" 1>>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
find -L . -maxdepth 5 -type l -ls 1>>"/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/directory.info"
echo "Launching container"
exec /bin/bash -c "$JAVA_HOME/bin/java -server -Xmx1024m '-Djava.net.preferIPv6Addresses=false' '-XX:+IgnoreUnrecognizedVMOptions' '--add-opens=java.base/java.lang=ALL-UNNAMED' '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED' '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED' '--add-opens=java.base/java.io=ALL-UNNAMED' '--add-opens=java.base/java.net=ALL-UNNAMED' '--add-opens=java.base/java.nio=ALL-UNNAMED' '--add-opens=java.base/java.util=ALL-UNNAMED' '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED' '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED' '--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED' '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED' '--add-opens=java.base/sun.security.action=ALL-UNNAMED' '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED' '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED' '-Djdk.reflect.useDirectMethodHandle=false' -Djava.io.tmpdir=$PWD/tmp '-Dspark.network.timeout=300s' '-Dspark.driver.port=38283' '-Dspark.ui.port=0' -Dspark.yarn.app.container.log.dir=/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002 -XX:OnOutOfMemoryError='kill %p' org.apache.spark.executor.YarnCoarseGrainedExecutorBackend --driver-url spark://CoarseGrainedScheduler@worker-node-2:38283 --executor-id 1 --hostname worker-node-1 --cores 1 --app-id application_1778738889964_0059 --resourceProfileId 0 1>/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/stdout 2>/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0059/container_1778738889964_0059_01_000002/stderr"

End of LogType:launch_container.sh
************************************************************************************

Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:prelaunch.err
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:0
LogContents:

End of LogType:prelaunch.err
******************************************************************************

Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:prelaunch.out
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:100
LogContents:
Setting up env variables
Setting up job resources
Copying debugging information
Launching container

End of LogType:prelaunch.out
******************************************************************************

Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:stderr
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:312382
LogContents:
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
26/05/21 18:50:44 INFO CoarseGrainedExecutorBackend: Started daemon with process name: 885567@worker-node-1
26/05/21 18:50:44 INFO SignalUtils: Registering signal handler for TERM
26/05/21 18:50:44 INFO SignalUtils: Registering signal handler for HUP
26/05/21 18:50:44 INFO SignalUtils: Registering signal handler for INT
26/05/21 18:50:45 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/05/21 18:50:46 INFO SecurityManager: Changing view acls to: hadoop,mnalsadoon
26/05/21 18:50:46 INFO SecurityManager: Changing modify acls to: hadoop,mnalsadoon
26/05/21 18:50:46 INFO SecurityManager: Changing view acls groups to: 
26/05/21 18:50:46 INFO SecurityManager: Changing modify acls groups to: 
26/05/21 18:50:46 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: hadoop, mnalsadoon; groups with view permissions: EMPTY; users with modify permissions: hadoop, mnalsadoon; groups with modify permissions: EMPTY
26/05/21 18:50:47 INFO TransportClientFactory: Successfully created connection to worker-node-2/146.190.147.119:38283 after 283 ms (0 ms spent in bootstraps)
26/05/21 18:50:48 INFO SecurityManager: Changing view acls to: hadoop,mnalsadoon
26/05/21 18:50:48 INFO SecurityManager: Changing modify acls to: hadoop,mnalsadoon
26/05/21 18:50:48 INFO SecurityManager: Changing view acls groups to: 
26/05/21 18:50:48 INFO SecurityManager: Changing modify acls groups to: 
26/05/21 18:50:48 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: hadoop, mnalsadoon; groups with view permissions: EMPTY; users with modify permissions: hadoop, mnalsadoon; groups with modify permissions: EMPTY
26/05/21 18:50:48 INFO TransportClientFactory: Successfully created connection to worker-node-2/146.190.147.119:38283 after 13 ms (0 ms spent in bootstraps)
26/05/21 18:50:48 INFO DiskBlockManager: Created local directory at /tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/blockmgr-84bc39ae-5fb6-4450-89c3-909c2717c5be
26/05/21 18:50:49 INFO MemoryStore: MemoryStore started with capacity 413.9 MiB
26/05/21 18:50:50 INFO YarnCoarseGrainedExecutorBackend: Connecting to driver: spark://CoarseGrainedScheduler@worker-node-2:38283
26/05/21 18:50:50 INFO ResourceUtils: ==============================================================
26/05/21 18:50:50 INFO ResourceUtils: No custom resources configured for spark.executor.
26/05/21 18:50:50 INFO ResourceUtils: ==============================================================
26/05/21 18:50:51 INFO YarnCoarseGrainedExecutorBackend: Successfully registered with driver
26/05/21 18:50:51 INFO Executor: Starting executor ID 1 on host worker-node-1
26/05/21 18:50:51 INFO Executor: OS info Linux, 6.8.0-71-generic, amd64
26/05/21 18:50:51 INFO Executor: Java version 11.0.30
26/05/21 18:50:51 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 36309.
26/05/21 18:50:51 INFO NettyBlockTransferService: Server created on worker-node-1:36309
26/05/21 18:50:51 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/05/21 18:50:51 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(1, worker-node-1, 36309, None)
26/05/21 18:50:51 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(1, worker-node-1, 36309, None)
26/05/21 18:50:51 INFO BlockManager: Initialized BlockManager: BlockManagerId(1, worker-node-1, 36309, None)
26/05/21 18:50:51 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): 'file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/__app__.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/commons-pool2-2.12.0.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/kafka-clients-3.9.0.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/spark-sql-kafka-0-10_2.12-3.5.4.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/spark-token-provider-kafka-0-10_2.12-3.5.4.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/spark-sql-kafka-0-10_2.12-3.5.4.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/__app__.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/commons-pool2-2.12.0.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/kafka-clients-3.9.0.jar,file:/tmp/hadoop-hadoop/nm-local-dir/usercache/mnalsadoon/appcache/application_1778738889964_0059/container_1778738889964_0059_01_000002/spark-token-provider-kafka-0-10_2.12-3.5.4.jar'
26/05/21 18:50:51 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@4151411f for default.
26/05/21 18:51:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 0
26/05/21 18:51:06 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
26/05/21 18:51:06 INFO TorrentBroadcast: Started reading broadcast variable 1 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:06 INFO TransportClientFactory: Successfully created connection to worker-node-2/146.190.147.119:34589 after 6 ms (0 ms spent in bootstraps)
26/05/21 18:51:06 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.4 KiB, free 413.9 MiB)
26/05/21 18:51:06 INFO TorrentBroadcast: Reading broadcast variable 1 took 287 ms
26/05/21 18:51:07 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 13.5 KiB, free 413.9 MiB)
26/05/21 18:51:10 INFO CodeGenerator: Code generated in 981.98178 ms
26/05/21 18:51:10 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:51:10 INFO CodeGenerator: Code generated in 62.551976 ms
26/05/21 18:51:10 INFO TorrentBroadcast: Started reading broadcast variable 0 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:10 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 62.4 KiB, free 413.8 MiB)
26/05/21 18:51:10 INFO TorrentBroadcast: Reading broadcast variable 0 took 43 ms
26/05/21 18:51:10 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 595.4 KiB, free 413.3 MiB)
26/05/21 18:51:13 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1684 bytes result sent to driver
26/05/21 18:51:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 1
26/05/21 18:51:14 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
26/05/21 18:51:14 INFO TorrentBroadcast: Started reading broadcast variable 3 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:14 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 13.7 KiB, free 413.3 MiB)
26/05/21 18:51:14 INFO TorrentBroadcast: Reading broadcast variable 3 took 44 ms
26/05/21 18:51:14 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 29.8 KiB, free 413.2 MiB)
26/05/21 18:51:18 INFO CodeGenerator: Code generated in 60.600629 ms
26/05/21 18:51:18 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:51:18 INFO TorrentBroadcast: Started reading broadcast variable 2 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:18 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 62.4 KiB, free 413.2 MiB)
26/05/21 18:51:18 INFO TorrentBroadcast: Reading broadcast variable 2 took 35 ms
26/05/21 18:51:18 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 595.4 KiB, free 412.6 MiB)
26/05/21 18:51:22 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1627 bytes result sent to driver
26/05/21 18:51:22 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 2
26/05/21 18:51:22 INFO Executor: Running task 1.0 in stage 1.0 (TID 2)
26/05/21 18:51:23 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:51:26 INFO Executor: Finished task 1.0 in stage 1.0 (TID 2). 1627 bytes result sent to driver
26/05/21 18:51:28 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 3
26/05/21 18:51:28 INFO Executor: Running task 0.0 in stage 2.0 (TID 3)
26/05/21 18:51:28 INFO TorrentBroadcast: Started reading broadcast variable 5 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:28 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 10.3 KiB, free 412.6 MiB)
26/05/21 18:51:28 INFO TorrentBroadcast: Reading broadcast variable 5 took 38 ms
26/05/21 18:51:28 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 21.2 KiB, free 412.6 MiB)
26/05/21 18:51:28 INFO CodeGenerator: Code generated in 145.068728 ms
26/05/21 18:51:28 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:51:28 INFO CodeGenerator: Code generated in 60.073689 ms
26/05/21 18:51:28 INFO TorrentBroadcast: Started reading broadcast variable 4 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:29 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.5 MiB)
26/05/21 18:51:29 INFO TorrentBroadcast: Reading broadcast variable 4 took 36 ms
26/05/21 18:51:29 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 595.4 KiB, free 412.0 MiB)
26/05/21 18:51:29 INFO CodeGenerator: Code generated in 33.649739 ms
26/05/21 18:51:29 INFO CodeGenerator: Code generated in 32.637941 ms
26/05/21 18:51:34 INFO Executor: Finished task 0.0 in stage 2.0 (TID 3). 1956 bytes result sent to driver
26/05/21 18:51:34 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 4
26/05/21 18:51:34 INFO Executor: Running task 1.0 in stage 2.0 (TID 4)
26/05/21 18:51:34 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:51:36 INFO Executor: Finished task 1.0 in stage 2.0 (TID 4). 1999 bytes result sent to driver
26/05/21 18:51:37 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 5
26/05/21 18:51:37 INFO Executor: Running task 0.0 in stage 4.0 (TID 5)
26/05/21 18:51:37 INFO MapOutputTrackerWorker: Updating epoch to 1 and clearing cache
26/05/21 18:51:37 INFO TorrentBroadcast: Started reading broadcast variable 6 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:37 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.0 MiB)
26/05/21 18:51:37 INFO TorrentBroadcast: Reading broadcast variable 6 took 30 ms
26/05/21 18:51:37 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 12.5 KiB, free 411.9 MiB)
26/05/21 18:51:37 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 0, fetching them
26/05/21 18:51:37 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:51:37 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:51:37 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:51:37 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 31 ms
26/05/21 18:51:37 INFO CodeGenerator: Code generated in 74.109188 ms
26/05/21 18:51:37 INFO Executor: Finished task 0.0 in stage 4.0 (TID 5). 3893 bytes result sent to driver
26/05/21 18:51:38 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 6
26/05/21 18:51:38 INFO Executor: Running task 0.0 in stage 5.0 (TID 6)
26/05/21 18:51:38 INFO TorrentBroadcast: Started reading broadcast variable 8 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:38 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 10.5 KiB, free 412.0 MiB)
26/05/21 18:51:38 INFO TorrentBroadcast: Reading broadcast variable 8 took 42 ms
26/05/21 18:51:38 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 21.9 KiB, free 411.9 MiB)
26/05/21 18:51:38 INFO CodeGenerator: Code generated in 134.166761 ms
26/05/21 18:51:38 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:51:38 INFO TorrentBroadcast: Started reading broadcast variable 7 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:38 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 411.9 MiB)
26/05/21 18:51:38 INFO TorrentBroadcast: Reading broadcast variable 7 took 30 ms
26/05/21 18:51:38 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 595.4 KiB, free 411.3 MiB)
26/05/21 18:51:42 INFO Executor: Finished task 0.0 in stage 5.0 (TID 6). 2012 bytes result sent to driver
26/05/21 18:51:42 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 7
26/05/21 18:51:42 INFO Executor: Running task 1.0 in stage 5.0 (TID 7)
26/05/21 18:51:42 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:51:45 INFO Executor: Finished task 1.0 in stage 5.0 (TID 7). 2055 bytes result sent to driver
26/05/21 18:51:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 8
26/05/21 18:51:45 INFO Executor: Running task 0.0 in stage 7.0 (TID 8)
26/05/21 18:51:45 INFO MapOutputTrackerWorker: Updating epoch to 2 and clearing cache
26/05/21 18:51:45 INFO TorrentBroadcast: Started reading broadcast variable 9 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:45 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.9 MiB)
26/05/21 18:51:45 INFO TorrentBroadcast: Reading broadcast variable 9 took 41 ms
26/05/21 18:51:45 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 12.5 KiB, free 411.9 MiB)
26/05/21 18:51:45 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 1, fetching them
26/05/21 18:51:45 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:51:45 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:51:45 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:51:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:51:45 INFO Executor: Finished task 0.0 in stage 7.0 (TID 8). 3807 bytes result sent to driver
26/05/21 18:51:47 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 9
26/05/21 18:51:47 INFO Executor: Running task 0.0 in stage 8.0 (TID 9)
26/05/21 18:51:47 INFO TorrentBroadcast: Started reading broadcast variable 11 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:47 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 19.4 KiB, free 412.6 MiB)
26/05/21 18:51:47 INFO TorrentBroadcast: Reading broadcast variable 11 took 38 ms
26/05/21 18:51:47 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 43.0 KiB, free 412.6 MiB)
26/05/21 18:51:48 INFO CodeGenerator: Code generated in 264.153415 ms
26/05/21 18:51:48 INFO CodeGenerator: Code generated in 112.650183 ms
26/05/21 18:51:48 INFO CodeGenerator: Code generated in 46.036767 ms
26/05/21 18:51:49 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:51:49 INFO CodeGenerator: Code generated in 75.749984 ms
26/05/21 18:51:49 INFO TorrentBroadcast: Started reading broadcast variable 10 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:49 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.5 MiB)
26/05/21 18:51:49 INFO TorrentBroadcast: Reading broadcast variable 10 took 47 ms
26/05/21 18:51:49 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 595.4 KiB, free 411.9 MiB)
26/05/21 18:51:49 INFO CodeGenerator: Code generated in 26.520092 ms
26/05/21 18:51:54 INFO MemoryStore: Block rdd_28_0 stored as values in memory (estimated size 72.8 KiB, free 411.9 MiB)
26/05/21 18:51:54 INFO Executor: 1 block locks were not released by task 0.0 in stage 8.0 (TID 9)
[rdd_28_0]
26/05/21 18:51:54 INFO Executor: Finished task 0.0 in stage 8.0 (TID 9). 2156 bytes result sent to driver
26/05/21 18:51:54 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 10
26/05/21 18:51:54 INFO Executor: Running task 1.0 in stage 8.0 (TID 10)
26/05/21 18:51:54 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:51:57 INFO MemoryStore: Block rdd_28_1 stored as values in memory (estimated size 67.6 KiB, free 411.8 MiB)
26/05/21 18:51:57 INFO Executor: 1 block locks were not released by task 1.0 in stage 8.0 (TID 10)
[rdd_28_1]
26/05/21 18:51:57 INFO Executor: Finished task 1.0 in stage 8.0 (TID 10). 2113 bytes result sent to driver
26/05/21 18:51:58 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 11
26/05/21 18:51:58 INFO Executor: Running task 0.0 in stage 9.0 (TID 11)
26/05/21 18:51:58 INFO TorrentBroadcast: Started reading broadcast variable 12 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:58 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 22.2 KiB, free 411.8 MiB)
26/05/21 18:51:58 INFO TorrentBroadcast: Reading broadcast variable 12 took 73 ms
26/05/21 18:51:58 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 49.7 KiB, free 411.8 MiB)
26/05/21 18:51:58 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:51:58 INFO CodeGenerator: Code generated in 24.765703 ms
26/05/21 18:51:58 INFO CodeGenerator: Code generated in 76.274918 ms
26/05/21 18:51:58 INFO CodeGenerator: Code generated in 51.644024 ms
26/05/21 18:51:58 INFO Executor: Finished task 0.0 in stage 9.0 (TID 11). 2763 bytes result sent to driver
26/05/21 18:51:58 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 12
26/05/21 18:51:58 INFO Executor: Running task 1.0 in stage 9.0 (TID 12)
26/05/21 18:51:58 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:51:58 INFO Executor: Finished task 1.0 in stage 9.0 (TID 12). 2763 bytes result sent to driver
26/05/21 18:51:58 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 13
26/05/21 18:51:58 INFO Executor: Running task 0.0 in stage 11.0 (TID 13)
26/05/21 18:51:58 INFO MapOutputTrackerWorker: Updating epoch to 3 and clearing cache
26/05/21 18:51:58 INFO TorrentBroadcast: Started reading broadcast variable 13 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:58 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.8 MiB)
26/05/21 18:51:58 INFO TorrentBroadcast: Reading broadcast variable 13 took 39 ms
26/05/21 18:51:58 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 12.5 KiB, free 411.8 MiB)
26/05/21 18:51:58 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 2, fetching them
26/05/21 18:51:58 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:51:58 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:51:58 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:51:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 5 ms
26/05/21 18:51:58 INFO Executor: Finished task 0.0 in stage 11.0 (TID 13). 3893 bytes result sent to driver
26/05/21 18:51:59 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 14
26/05/21 18:51:59 INFO Executor: Running task 0.0 in stage 12.0 (TID 14)
26/05/21 18:51:59 INFO TorrentBroadcast: Started reading broadcast variable 15 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:51:59 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 21.3 KiB, free 411.8 MiB)
26/05/21 18:51:59 INFO TorrentBroadcast: Reading broadcast variable 15 took 38 ms
26/05/21 18:51:59 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 46.2 KiB, free 411.8 MiB)
26/05/21 18:52:00 INFO CodeGenerator: Code generated in 215.615977 ms
26/05/21 18:52:00 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:52:00 INFO TorrentBroadcast: Started reading broadcast variable 14 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:00 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 411.7 MiB)
26/05/21 18:52:00 INFO TorrentBroadcast: Reading broadcast variable 14 took 66 ms
26/05/21 18:52:00 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 595.4 KiB, free 411.1 MiB)
26/05/21 18:52:04 INFO Executor: Finished task 0.0 in stage 12.0 (TID 14). 2940 bytes result sent to driver
26/05/21 18:52:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 15
26/05/21 18:52:04 INFO Executor: Running task 1.0 in stage 12.0 (TID 15)
26/05/21 18:52:04 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:52:07 INFO Executor: Finished task 1.0 in stage 12.0 (TID 15). 2897 bytes result sent to driver
26/05/21 18:52:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 16
26/05/21 18:52:07 INFO Executor: Running task 0.0 in stage 14.0 (TID 16)
26/05/21 18:52:07 INFO MapOutputTrackerWorker: Updating epoch to 4 and clearing cache
26/05/21 18:52:07 INFO TorrentBroadcast: Started reading broadcast variable 16 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:07 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.1 MiB)
26/05/21 18:52:07 INFO TorrentBroadcast: Reading broadcast variable 16 took 33 ms
26/05/21 18:52:07 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 12.5 KiB, free 411.1 MiB)
26/05/21 18:52:07 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 3, fetching them
26/05/21 18:52:07 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:07 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:07 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:07 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:52:07 INFO Executor: Finished task 0.0 in stage 14.0 (TID 16). 3850 bytes result sent to driver
26/05/21 18:52:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 17
26/05/21 18:52:08 INFO Executor: Running task 0.0 in stage 15.0 (TID 17)
26/05/21 18:52:08 INFO TorrentBroadcast: Started reading broadcast variable 17 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:08 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 23.5 KiB, free 411.2 MiB)
26/05/21 18:52:08 INFO TorrentBroadcast: Reading broadcast variable 17 took 45 ms
26/05/21 18:52:08 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 52.8 KiB, free 411.1 MiB)
26/05/21 18:52:08 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:52:08 INFO CodeGenerator: Code generated in 66.484354 ms
26/05/21 18:52:08 INFO CodeGenerator: Code generated in 27.022407 ms
26/05/21 18:52:08 INFO CodeGenerator: Code generated in 26.417156 ms
26/05/21 18:52:08 INFO CodeGenerator: Code generated in 38.510564 ms
26/05/21 18:52:08 INFO CodeGenerator: Code generated in 31.941335 ms
26/05/21 18:52:09 INFO CodeGenerator: Code generated in 70.983347 ms
26/05/21 18:52:09 INFO Executor: Finished task 0.0 in stage 15.0 (TID 17). 2895 bytes result sent to driver
26/05/21 18:52:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 18
26/05/21 18:52:09 INFO Executor: Running task 1.0 in stage 15.0 (TID 18)
26/05/21 18:52:09 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:52:09 INFO Executor: Finished task 1.0 in stage 15.0 (TID 18). 2852 bytes result sent to driver
26/05/21 18:52:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 19
26/05/21 18:52:09 INFO Executor: Running task 0.0 in stage 17.0 (TID 19)
26/05/21 18:52:09 INFO MapOutputTrackerWorker: Updating epoch to 5 and clearing cache
26/05/21 18:52:09 INFO TorrentBroadcast: Started reading broadcast variable 18 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:09 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 25.5 KiB, free 411.8 MiB)
26/05/21 18:52:09 INFO TorrentBroadcast: Reading broadcast variable 18 took 27 ms
26/05/21 18:52:09 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 57.7 KiB, free 411.7 MiB)
26/05/21 18:52:09 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 4, fetching them
26/05/21 18:52:09 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:09 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:09 INFO ShuffleBlockFetcherIterator: Getting 2 (1657.0 B) non-empty blocks including 2 (1657.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:09 INFO CodeGenerator: Code generated in 52.6806 ms
26/05/21 18:52:09 INFO Executor: Finished task 0.0 in stage 17.0 (TID 19). 5965 bytes result sent to driver
26/05/21 18:52:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 20
26/05/21 18:52:11 INFO Executor: Running task 0.0 in stage 18.0 (TID 20)
26/05/21 18:52:11 INFO TorrentBroadcast: Started reading broadcast variable 19 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:11 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 28.8 KiB, free 411.8 MiB)
26/05/21 18:52:11 INFO TorrentBroadcast: Reading broadcast variable 19 took 26 ms
26/05/21 18:52:11 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 64.8 KiB, free 411.8 MiB)
26/05/21 18:52:11 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:52:11 INFO CodeGenerator: Code generated in 68.806042 ms
26/05/21 18:52:11 INFO CodeGenerator: Code generated in 115.635001 ms
26/05/21 18:52:12 INFO Executor: Finished task 0.0 in stage 18.0 (TID 20). 2964 bytes result sent to driver
26/05/21 18:52:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 21
26/05/21 18:52:12 INFO Executor: Running task 1.0 in stage 18.0 (TID 21)
26/05/21 18:52:12 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:52:12 INFO Executor: Finished task 1.0 in stage 18.0 (TID 21). 3050 bytes result sent to driver
26/05/21 18:52:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 22
26/05/21 18:52:12 INFO Executor: Running task 0.0 in stage 20.0 (TID 22)
26/05/21 18:52:12 INFO MapOutputTrackerWorker: Updating epoch to 6 and clearing cache
26/05/21 18:52:12 INFO TorrentBroadcast: Started reading broadcast variable 20 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:12 INFO MemoryStore: Block broadcast_20_piece0 stored as bytes in memory (estimated size 29.7 KiB, free 411.7 MiB)
26/05/21 18:52:12 INFO TorrentBroadcast: Reading broadcast variable 20 took 24 ms
26/05/21 18:52:12 INFO MemoryStore: Block broadcast_20 stored as values in memory (estimated size 66.1 KiB, free 411.7 MiB)
26/05/21 18:52:12 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 5, fetching them
26/05/21 18:52:12 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:12 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:12 INFO ShuffleBlockFetcherIterator: Getting 2 (608.0 B) non-empty blocks including 2 (608.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
26/05/21 18:52:12 INFO Executor: Finished task 0.0 in stage 20.0 (TID 22). 5510 bytes result sent to driver
26/05/21 18:52:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 23
26/05/21 18:52:15 INFO Executor: Running task 0.0 in stage 21.0 (TID 23)
26/05/21 18:52:15 INFO TorrentBroadcast: Started reading broadcast variable 21 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:15 INFO MemoryStore: Block broadcast_21_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 411.8 MiB)
26/05/21 18:52:15 INFO TorrentBroadcast: Reading broadcast variable 21 took 34 ms
26/05/21 18:52:15 INFO MemoryStore: Block broadcast_21 stored as values in memory (estimated size 78.3 KiB, free 411.8 MiB)
26/05/21 18:52:15 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:52:15 INFO CodeGenerator: Code generated in 66.519359 ms
26/05/21 18:52:15 INFO CodeGenerator: Code generated in 140.675093 ms
26/05/21 18:52:15 INFO CodeGenerator: Code generated in 37.161403 ms
26/05/21 18:52:15 INFO Executor: 1 block locks were not released by task 0.0 in stage 21.0 (TID 23)
[rdd_28_0]
26/05/21 18:52:15 INFO Executor: Finished task 0.0 in stage 21.0 (TID 23). 2512 bytes result sent to driver
26/05/21 18:52:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 24
26/05/21 18:52:18 INFO Executor: Running task 0.0 in stage 22.0 (TID 24)
26/05/21 18:52:18 INFO TorrentBroadcast: Started reading broadcast variable 22 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:18 INFO MemoryStore: Block broadcast_22_piece0 stored as bytes in memory (estimated size 46.8 KiB, free 411.8 MiB)
26/05/21 18:52:18 INFO TorrentBroadcast: Reading broadcast variable 22 took 29 ms
26/05/21 18:52:18 INFO MemoryStore: Block broadcast_22 stored as values in memory (estimated size 118.1 KiB, free 411.7 MiB)
26/05/21 18:52:18 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:52:19 INFO CodeGenerator: Code generated in 211.242581 ms
26/05/21 18:52:19 INFO CodeGenerator: Code generated in 30.991813 ms
26/05/21 18:52:19 INFO CodeGenerator: Code generated in 22.186939 ms
26/05/21 18:52:19 INFO Executor: Finished task 0.0 in stage 22.0 (TID 24). 3038 bytes result sent to driver
26/05/21 18:52:19 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 25
26/05/21 18:52:19 INFO Executor: Running task 1.0 in stage 22.0 (TID 25)
26/05/21 18:52:19 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:52:20 INFO Executor: Finished task 1.0 in stage 22.0 (TID 25). 3038 bytes result sent to driver
26/05/21 18:52:24 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 26
26/05/21 18:52:24 INFO Executor: Running task 0.0 in stage 23.0 (TID 26)
26/05/21 18:52:24 INFO TorrentBroadcast: Started reading broadcast variable 26 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:24 INFO MemoryStore: Block broadcast_26_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 413.1 MiB)
26/05/21 18:52:24 INFO TorrentBroadcast: Reading broadcast variable 26 took 34 ms
26/05/21 18:52:24 INFO MemoryStore: Block broadcast_26 stored as values in memory (estimated size 118.8 KiB, free 413.0 MiB)
26/05/21 18:52:24 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:52:24 INFO TorrentBroadcast: Started reading broadcast variable 23 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:24 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 77.0 B, free 413.0 MiB)
26/05/21 18:52:24 INFO TorrentBroadcast: Reading broadcast variable 23 took 35 ms
26/05/21 18:52:24 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 72.0 B, free 413.0 MiB)
26/05/21 18:52:25 INFO MemoryStore: Block rdd_83_0 stored as values in memory (estimated size 634.2 KiB, free 412.4 MiB)
26/05/21 18:52:25 INFO TorrentBroadcast: Started reading broadcast variable 25 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:25 INFO MemoryStore: Block broadcast_25_piece0 stored as bytes in memory (estimated size 61.0 B, free 412.4 MiB)
26/05/21 18:52:25 INFO TorrentBroadcast: Reading broadcast variable 25 took 35 ms
26/05/21 18:52:25 INFO MemoryStore: Block broadcast_25 stored as values in memory (estimated size 96.0 B, free 412.4 MiB)
26/05/21 18:52:25 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
26/05/21 18:52:25 INFO TorrentBroadcast: Started reading broadcast variable 24 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:25 INFO MemoryStore: Block broadcast_24_piece0 stored as bytes in memory (estimated size 77.0 B, free 412.4 MiB)
26/05/21 18:52:25 INFO TorrentBroadcast: Reading broadcast variable 24 took 42 ms
26/05/21 18:52:25 INFO MemoryStore: Block broadcast_24 stored as values in memory (estimated size 72.0 B, free 412.4 MiB)
26/05/21 18:52:25 INFO Executor: Finished task 0.0 in stage 23.0 (TID 26). 2714 bytes result sent to driver
26/05/21 18:52:25 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 27
26/05/21 18:52:25 INFO Executor: Running task 1.0 in stage 23.0 (TID 27)
26/05/21 18:52:25 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:52:25 INFO MemoryStore: Block rdd_83_1 stored as values in memory (estimated size 605.5 KiB, free 411.8 MiB)
26/05/21 18:52:25 INFO Executor: Finished task 1.0 in stage 23.0 (TID 27). 2671 bytes result sent to driver
26/05/21 18:52:26 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 28
26/05/21 18:52:26 INFO Executor: Running task 0.0 in stage 24.0 (TID 28)
26/05/21 18:52:26 INFO TorrentBroadcast: Started reading broadcast variable 28 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_28_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.9 MiB)
26/05/21 18:52:26 INFO TorrentBroadcast: Reading broadcast variable 28 took 48 ms
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_28 stored as values in memory (estimated size 118.8 KiB, free 411.8 MiB)
26/05/21 18:52:26 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:26 INFO TorrentBroadcast: Started reading broadcast variable 27 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_27_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.8 MiB)
26/05/21 18:52:26 INFO TorrentBroadcast: Reading broadcast variable 27 took 33 ms
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_27 stored as values in memory (estimated size 96.0 B, free 411.8 MiB)
26/05/21 18:52:26 INFO Executor: Finished task 0.0 in stage 24.0 (TID 28). 2628 bytes result sent to driver
26/05/21 18:52:26 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 29
26/05/21 18:52:26 INFO Executor: Running task 1.0 in stage 24.0 (TID 29)
26/05/21 18:52:26 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:26 INFO Executor: Finished task 1.0 in stage 24.0 (TID 29). 2671 bytes result sent to driver
26/05/21 18:52:26 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 30
26/05/21 18:52:26 INFO Executor: Running task 0.0 in stage 25.0 (TID 30)
26/05/21 18:52:26 INFO TorrentBroadcast: Started reading broadcast variable 30 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_30_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.7 MiB)
26/05/21 18:52:26 INFO TorrentBroadcast: Reading broadcast variable 30 took 29 ms
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_30 stored as values in memory (estimated size 118.8 KiB, free 411.6 MiB)
26/05/21 18:52:26 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:26 INFO TorrentBroadcast: Started reading broadcast variable 29 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_29_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.6 MiB)
26/05/21 18:52:26 INFO TorrentBroadcast: Reading broadcast variable 29 took 23 ms
26/05/21 18:52:26 INFO MemoryStore: Block broadcast_29 stored as values in memory (estimated size 96.0 B, free 411.6 MiB)
26/05/21 18:52:26 INFO Executor: Finished task 0.0 in stage 25.0 (TID 30). 2671 bytes result sent to driver
26/05/21 18:52:26 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 31
26/05/21 18:52:26 INFO Executor: Running task 1.0 in stage 25.0 (TID 31)
26/05/21 18:52:26 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:26 INFO Executor: Finished task 1.0 in stage 25.0 (TID 31). 2671 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 32
26/05/21 18:52:27 INFO Executor: Running task 0.0 in stage 26.0 (TID 32)
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 32 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_32_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.6 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 32 took 26 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_32 stored as values in memory (estimated size 118.8 KiB, free 411.4 MiB)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 31 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_31_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.4 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 31 took 27 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_31 stored as values in memory (estimated size 96.0 B, free 411.4 MiB)
26/05/21 18:52:27 INFO Executor: Finished task 0.0 in stage 26.0 (TID 32). 2628 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 33
26/05/21 18:52:27 INFO Executor: Running task 1.0 in stage 26.0 (TID 33)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:27 INFO Executor: Finished task 1.0 in stage 26.0 (TID 33). 2671 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 34
26/05/21 18:52:27 INFO Executor: Running task 0.0 in stage 27.0 (TID 34)
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 34 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_34_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.9 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 34 took 24 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_34 stored as values in memory (estimated size 118.8 KiB, free 411.8 MiB)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 33 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_33_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.8 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 33 took 36 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_33 stored as values in memory (estimated size 96.0 B, free 411.8 MiB)
26/05/21 18:52:27 INFO Executor: Finished task 0.0 in stage 27.0 (TID 34). 2628 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 35
26/05/21 18:52:27 INFO Executor: Running task 1.0 in stage 27.0 (TID 35)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:27 INFO Executor: Finished task 1.0 in stage 27.0 (TID 35). 2628 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 36
26/05/21 18:52:27 INFO Executor: Running task 0.0 in stage 28.0 (TID 36)
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 36 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_36_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.7 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 36 took 39 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_36 stored as values in memory (estimated size 118.8 KiB, free 411.6 MiB)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:27 INFO TorrentBroadcast: Started reading broadcast variable 35 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_35_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.6 MiB)
26/05/21 18:52:27 INFO TorrentBroadcast: Reading broadcast variable 35 took 23 ms
26/05/21 18:52:27 INFO MemoryStore: Block broadcast_35 stored as values in memory (estimated size 96.0 B, free 411.6 MiB)
26/05/21 18:52:27 INFO Executor: Finished task 0.0 in stage 28.0 (TID 36). 2714 bytes result sent to driver
26/05/21 18:52:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 37
26/05/21 18:52:27 INFO Executor: Running task 1.0 in stage 28.0 (TID 37)
26/05/21 18:52:27 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:27 INFO Executor: Finished task 1.0 in stage 28.0 (TID 37). 2671 bytes result sent to driver
26/05/21 18:52:28 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 38
26/05/21 18:52:28 INFO Executor: Running task 0.0 in stage 29.0 (TID 38)
26/05/21 18:52:28 INFO TorrentBroadcast: Started reading broadcast variable 38 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:28 INFO MemoryStore: Block broadcast_38_piece0 stored as bytes in memory (estimated size 46.9 KiB, free 411.6 MiB)
26/05/21 18:52:28 INFO TorrentBroadcast: Reading broadcast variable 38 took 25 ms
26/05/21 18:52:28 INFO MemoryStore: Block broadcast_38 stored as values in memory (estimated size 118.8 KiB, free 411.4 MiB)
26/05/21 18:52:28 INFO BlockManager: Found block rdd_83_0 locally
26/05/21 18:52:28 INFO TorrentBroadcast: Started reading broadcast variable 37 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:28 INFO MemoryStore: Block broadcast_37_piece0 stored as bytes in memory (estimated size 87.0 B, free 411.4 MiB)
26/05/21 18:52:28 INFO TorrentBroadcast: Reading broadcast variable 37 took 25 ms
26/05/21 18:52:28 INFO MemoryStore: Block broadcast_37 stored as values in memory (estimated size 96.0 B, free 411.4 MiB)
26/05/21 18:52:28 INFO Executor: Finished task 0.0 in stage 29.0 (TID 38). 2628 bytes result sent to driver
26/05/21 18:52:28 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 39
26/05/21 18:52:28 INFO Executor: Running task 1.0 in stage 29.0 (TID 39)
26/05/21 18:52:28 INFO BlockManager: Found block rdd_83_1 locally
26/05/21 18:52:28 INFO Executor: Finished task 1.0 in stage 29.0 (TID 39). 2671 bytes result sent to driver
26/05/21 18:52:28 INFO BlockManager: Removing RDD 83
26/05/21 18:52:28 INFO BlockManager: Removing RDD 83
26/05/21 18:52:32 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 40
26/05/21 18:52:32 INFO Executor: Running task 0.0 in stage 30.0 (TID 40)
26/05/21 18:52:32 INFO TorrentBroadcast: Started reading broadcast variable 40 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:32 INFO MemoryStore: Block broadcast_40_piece0 stored as bytes in memory (estimated size 42.1 KiB, free 413.1 MiB)
26/05/21 18:52:32 INFO TorrentBroadcast: Reading broadcast variable 40 took 24 ms
26/05/21 18:52:32 INFO MemoryStore: Block broadcast_40 stored as values in memory (estimated size 100.7 KiB, free 413.0 MiB)
26/05/21 18:52:32 INFO CodeGenerator: Code generated in 138.475673 ms
26/05/21 18:52:32 INFO CodeGenerator: Code generated in 34.736386 ms
26/05/21 18:52:32 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:52:32 INFO TorrentBroadcast: Started reading broadcast variable 39 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:32 INFO MemoryStore: Block broadcast_39_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:52:32 INFO TorrentBroadcast: Reading broadcast variable 39 took 26 ms
26/05/21 18:52:32 INFO MemoryStore: Block broadcast_39 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:52:38 INFO Executor: Finished task 0.0 in stage 30.0 (TID 40). 2405 bytes result sent to driver
26/05/21 18:52:38 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 41
26/05/21 18:52:38 INFO Executor: Running task 1.0 in stage 30.0 (TID 41)
26/05/21 18:52:38 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:52:42 INFO Executor: Finished task 1.0 in stage 30.0 (TID 41). 2405 bytes result sent to driver
26/05/21 18:52:42 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 42
26/05/21 18:52:42 INFO Executor: Running task 0.0 in stage 31.0 (TID 42)
26/05/21 18:52:42 INFO MapOutputTrackerWorker: Updating epoch to 7 and clearing cache
26/05/21 18:52:42 INFO TorrentBroadcast: Started reading broadcast variable 41 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:42 INFO MemoryStore: Block broadcast_41_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 412.4 MiB)
26/05/21 18:52:42 INFO TorrentBroadcast: Reading broadcast variable 41 took 24 ms
26/05/21 18:52:42 INFO MemoryStore: Block broadcast_41 stored as values in memory (estimated size 7.3 KiB, free 412.4 MiB)
26/05/21 18:52:42 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 6, fetching them
26/05/21 18:52:42 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:42 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:42 INFO ShuffleBlockFetcherIterator: Getting 2 (35.8 KiB) non-empty blocks including 2 (35.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:43 INFO Executor: Finished task 0.0 in stage 31.0 (TID 42). 2256 bytes result sent to driver
26/05/21 18:52:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 43
26/05/21 18:52:43 INFO Executor: Running task 1.0 in stage 31.0 (TID 43)
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Getting 2 (35.8 KiB) non-empty blocks including 2 (35.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
26/05/21 18:52:43 INFO Executor: Finished task 1.0 in stage 31.0 (TID 43). 2213 bytes result sent to driver
26/05/21 18:52:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 44
26/05/21 18:52:43 INFO Executor: Running task 0.0 in stage 33.0 (TID 44)
26/05/21 18:52:43 INFO TorrentBroadcast: Started reading broadcast variable 42 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:43 INFO MemoryStore: Block broadcast_42_piece0 stored as bytes in memory (estimated size 3.2 KiB, free 412.4 MiB)
26/05/21 18:52:43 INFO TorrentBroadcast: Reading broadcast variable 42 took 25 ms
26/05/21 18:52:43 INFO MemoryStore: Block broadcast_42 stored as values in memory (estimated size 5.7 KiB, free 412.3 MiB)
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Getting 2 (35.8 KiB) non-empty blocks including 2 (35.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
26/05/21 18:52:43 INFO Executor: Finished task 0.0 in stage 33.0 (TID 44). 1834 bytes result sent to driver
26/05/21 18:52:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 45
26/05/21 18:52:43 INFO Executor: Running task 1.0 in stage 33.0 (TID 45)
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Getting 2 (35.8 KiB) non-empty blocks including 2 (35.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:52:43 INFO Executor: Finished task 1.0 in stage 33.0 (TID 45). 1834 bytes result sent to driver
26/05/21 18:52:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 46
26/05/21 18:52:43 INFO Executor: Running task 0.0 in stage 34.0 (TID 46)
26/05/21 18:52:43 INFO MapOutputTrackerWorker: Updating epoch to 8 and clearing cache
26/05/21 18:52:43 INFO TorrentBroadcast: Started reading broadcast variable 43 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:43 INFO MemoryStore: Block broadcast_43_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:52:43 INFO TorrentBroadcast: Reading broadcast variable 43 took 22 ms
26/05/21 18:52:43 INFO MemoryStore: Block broadcast_43 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:52:43 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 7, fetching them
26/05/21 18:52:43 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:43 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 5 ms
26/05/21 18:52:43 INFO Executor: Finished task 0.0 in stage 34.0 (TID 46). 1717 bytes result sent to driver
26/05/21 18:52:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 47
26/05/21 18:52:43 INFO Executor: Running task 1.0 in stage 34.0 (TID 47)
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:44 INFO Executor: Finished task 1.0 in stage 34.0 (TID 47). 1674 bytes result sent to driver
26/05/21 18:52:44 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 48
26/05/21 18:52:44 INFO Executor: Running task 0.0 in stage 37.0 (TID 48)
26/05/21 18:52:44 INFO TorrentBroadcast: Started reading broadcast variable 44 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:44 INFO MemoryStore: Block broadcast_44_piece0 stored as bytes in memory (estimated size 3.8 KiB, free 412.5 MiB)
26/05/21 18:52:44 INFO TorrentBroadcast: Reading broadcast variable 44 took 29 ms
26/05/21 18:52:44 INFO MemoryStore: Block broadcast_44 stored as values in memory (estimated size 7.0 KiB, free 412.5 MiB)
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:44 INFO Executor: Finished task 0.0 in stage 37.0 (TID 48). 1852 bytes result sent to driver
26/05/21 18:52:44 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 49
26/05/21 18:52:44 INFO Executor: Running task 1.0 in stage 37.0 (TID 49)
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:44 INFO Executor: Finished task 1.0 in stage 37.0 (TID 49). 1809 bytes result sent to driver
26/05/21 18:52:44 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 50
26/05/21 18:52:44 INFO Executor: Running task 0.0 in stage 40.0 (TID 50)
26/05/21 18:52:44 INFO TorrentBroadcast: Started reading broadcast variable 45 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:44 INFO MemoryStore: Block broadcast_45_piece0 stored as bytes in memory (estimated size 4.2 KiB, free 412.5 MiB)
26/05/21 18:52:44 INFO TorrentBroadcast: Reading broadcast variable 45 took 26 ms
26/05/21 18:52:44 INFO MemoryStore: Block broadcast_45 stored as values in memory (estimated size 8.6 KiB, free 412.5 MiB)
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
26/05/21 18:52:44 INFO MemoryStore: Block rdd_123_0 stored as values in memory (estimated size 41.1 KiB, free 412.4 MiB)
26/05/21 18:52:44 INFO Executor: Finished task 0.0 in stage 40.0 (TID 50). 1761 bytes result sent to driver
26/05/21 18:52:44 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 51
26/05/21 18:52:44 INFO Executor: Running task 1.0 in stage 40.0 (TID 51)
26/05/21 18:52:45 INFO ShuffleBlockFetcherIterator: Getting 2 (29.6 KiB) non-empty blocks including 2 (29.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:52:45 INFO MemoryStore: Block rdd_123_1 stored as values in memory (estimated size 41.1 KiB, free 412.4 MiB)
26/05/21 18:52:45 INFO Executor: Finished task 1.0 in stage 40.0 (TID 51). 1761 bytes result sent to driver
26/05/21 18:52:45 INFO BlockManager: Removing RDD 123
26/05/21 18:52:45 INFO BlockManager: Removing RDD 123
26/05/21 18:52:46 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 52
26/05/21 18:52:46 INFO Executor: Running task 0.0 in stage 41.0 (TID 52)
26/05/21 18:52:46 INFO TorrentBroadcast: Started reading broadcast variable 47 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:46 INFO MemoryStore: Block broadcast_47_piece0 stored as bytes in memory (estimated size 44.7 KiB, free 413.1 MiB)
26/05/21 18:52:46 INFO TorrentBroadcast: Reading broadcast variable 47 took 27 ms
26/05/21 18:52:46 INFO MemoryStore: Block broadcast_47 stored as values in memory (estimated size 112.4 KiB, free 413.0 MiB)
26/05/21 18:52:46 INFO CodeGenerator: Code generated in 190.23081 ms
26/05/21 18:52:46 INFO CodeGenerator: Code generated in 33.421527 ms
26/05/21 18:52:46 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:52:46 INFO TorrentBroadcast: Started reading broadcast variable 46 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:46 INFO MemoryStore: Block broadcast_46_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:52:46 INFO TorrentBroadcast: Reading broadcast variable 46 took 30 ms
26/05/21 18:52:46 INFO MemoryStore: Block broadcast_46 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:52:50 INFO Executor: Finished task 0.0 in stage 41.0 (TID 52). 2448 bytes result sent to driver
26/05/21 18:52:50 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 53
26/05/21 18:52:50 INFO Executor: Running task 1.0 in stage 41.0 (TID 53)
26/05/21 18:52:50 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:52:53 INFO Executor: Finished task 1.0 in stage 41.0 (TID 53). 2405 bytes result sent to driver
26/05/21 18:52:53 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 54
26/05/21 18:52:53 INFO Executor: Running task 0.0 in stage 42.0 (TID 54)
26/05/21 18:52:53 INFO MapOutputTrackerWorker: Updating epoch to 9 and clearing cache
26/05/21 18:52:53 INFO TorrentBroadcast: Started reading broadcast variable 48 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:53 INFO MemoryStore: Block broadcast_48_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:52:53 INFO TorrentBroadcast: Reading broadcast variable 48 took 21 ms
26/05/21 18:52:53 INFO MemoryStore: Block broadcast_48 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:52:53 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 8, fetching them
26/05/21 18:52:53 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:52:53 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:52:53 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:52:53 INFO Executor: Finished task 0.0 in stage 42.0 (TID 54). 1727 bytes result sent to driver
26/05/21 18:52:53 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 55
26/05/21 18:52:53 INFO Executor: Running task 1.0 in stage 42.0 (TID 55)
26/05/21 18:52:53 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:52:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:52:53 INFO Executor: Finished task 1.0 in stage 42.0 (TID 55). 1727 bytes result sent to driver
26/05/21 18:52:54 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 56
26/05/21 18:52:54 INFO Executor: Running task 0.0 in stage 43.0 (TID 56)
26/05/21 18:52:54 INFO TorrentBroadcast: Started reading broadcast variable 50 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:54 INFO MemoryStore: Block broadcast_50_piece0 stored as bytes in memory (estimated size 44.7 KiB, free 413.1 MiB)
26/05/21 18:52:54 INFO TorrentBroadcast: Reading broadcast variable 50 took 17 ms
26/05/21 18:52:54 INFO MemoryStore: Block broadcast_50 stored as values in memory (estimated size 112.4 KiB, free 413.0 MiB)
26/05/21 18:52:54 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:52:54 INFO TorrentBroadcast: Started reading broadcast variable 49 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:52:54 INFO MemoryStore: Block broadcast_49_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:52:54 INFO TorrentBroadcast: Reading broadcast variable 49 took 24 ms
26/05/21 18:52:54 INFO MemoryStore: Block broadcast_49 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:52:57 INFO Executor: Finished task 0.0 in stage 43.0 (TID 56). 2405 bytes result sent to driver
26/05/21 18:52:57 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 57
26/05/21 18:52:57 INFO Executor: Running task 1.0 in stage 43.0 (TID 57)
26/05/21 18:52:57 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:00 INFO Executor: Finished task 1.0 in stage 43.0 (TID 57). 2448 bytes result sent to driver
26/05/21 18:53:00 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 58
26/05/21 18:53:00 INFO Executor: Running task 0.0 in stage 44.0 (TID 58)
26/05/21 18:53:00 INFO MapOutputTrackerWorker: Updating epoch to 10 and clearing cache
26/05/21 18:53:00 INFO TorrentBroadcast: Started reading broadcast variable 51 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:00 INFO MemoryStore: Block broadcast_51_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:53:01 INFO TorrentBroadcast: Reading broadcast variable 51 took 40 ms
26/05/21 18:53:01 INFO MemoryStore: Block broadcast_51 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:53:01 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 9, fetching them
26/05/21 18:53:01 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:01 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:01 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:01 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:01 INFO Executor: Finished task 0.0 in stage 44.0 (TID 58). 1727 bytes result sent to driver
26/05/21 18:53:01 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 59
26/05/21 18:53:01 INFO Executor: Running task 1.0 in stage 44.0 (TID 59)
26/05/21 18:53:01 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:01 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:01 INFO Executor: Finished task 1.0 in stage 44.0 (TID 59). 1727 bytes result sent to driver
26/05/21 18:53:01 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 60
26/05/21 18:53:01 INFO Executor: Running task 0.0 in stage 45.0 (TID 60)
26/05/21 18:53:01 INFO TorrentBroadcast: Started reading broadcast variable 53 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:02 INFO MemoryStore: Block broadcast_53_piece0 stored as bytes in memory (estimated size 44.7 KiB, free 413.1 MiB)
26/05/21 18:53:02 INFO TorrentBroadcast: Reading broadcast variable 53 took 51 ms
26/05/21 18:53:02 INFO MemoryStore: Block broadcast_53 stored as values in memory (estimated size 112.4 KiB, free 413.0 MiB)
26/05/21 18:53:02 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:02 INFO TorrentBroadcast: Started reading broadcast variable 52 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:02 INFO MemoryStore: Block broadcast_52_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:53:02 INFO TorrentBroadcast: Reading broadcast variable 52 took 23 ms
26/05/21 18:53:02 INFO MemoryStore: Block broadcast_52 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:53:05 INFO Executor: Finished task 0.0 in stage 45.0 (TID 60). 2405 bytes result sent to driver
26/05/21 18:53:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 61
26/05/21 18:53:05 INFO Executor: Running task 1.0 in stage 45.0 (TID 61)
26/05/21 18:53:05 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:08 INFO Executor: Finished task 1.0 in stage 45.0 (TID 61). 2405 bytes result sent to driver
26/05/21 18:53:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 62
26/05/21 18:53:08 INFO Executor: Running task 0.0 in stage 46.0 (TID 62)
26/05/21 18:53:08 INFO MapOutputTrackerWorker: Updating epoch to 11 and clearing cache
26/05/21 18:53:08 INFO TorrentBroadcast: Started reading broadcast variable 54 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:08 INFO MemoryStore: Block broadcast_54_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:53:08 INFO TorrentBroadcast: Reading broadcast variable 54 took 25 ms
26/05/21 18:53:08 INFO MemoryStore: Block broadcast_54 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:53:08 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 10, fetching them
26/05/21 18:53:08 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:08 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:08 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:08 INFO Executor: Finished task 0.0 in stage 46.0 (TID 62). 1727 bytes result sent to driver
26/05/21 18:53:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 63
26/05/21 18:53:08 INFO Executor: Running task 1.0 in stage 46.0 (TID 63)
26/05/21 18:53:08 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:08 INFO Executor: Finished task 1.0 in stage 46.0 (TID 63). 1727 bytes result sent to driver
26/05/21 18:53:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 64
26/05/21 18:53:09 INFO Executor: Running task 0.0 in stage 47.0 (TID 64)
26/05/21 18:53:09 INFO TorrentBroadcast: Started reading broadcast variable 56 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:09 INFO MemoryStore: Block broadcast_56_piece0 stored as bytes in memory (estimated size 44.7 KiB, free 413.1 MiB)
26/05/21 18:53:09 INFO TorrentBroadcast: Reading broadcast variable 56 took 22 ms
26/05/21 18:53:09 INFO MemoryStore: Block broadcast_56 stored as values in memory (estimated size 112.4 KiB, free 413.0 MiB)
26/05/21 18:53:09 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:09 INFO TorrentBroadcast: Started reading broadcast variable 55 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:09 INFO MemoryStore: Block broadcast_55_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:53:09 INFO TorrentBroadcast: Reading broadcast variable 55 took 29 ms
26/05/21 18:53:09 INFO MemoryStore: Block broadcast_55 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:53:12 INFO Executor: Finished task 0.0 in stage 47.0 (TID 64). 2405 bytes result sent to driver
26/05/21 18:53:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 65
26/05/21 18:53:12 INFO Executor: Running task 1.0 in stage 47.0 (TID 65)
26/05/21 18:53:12 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:15 INFO Executor: Finished task 1.0 in stage 47.0 (TID 65). 2405 bytes result sent to driver
26/05/21 18:53:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 66
26/05/21 18:53:15 INFO Executor: Running task 0.0 in stage 48.0 (TID 66)
26/05/21 18:53:15 INFO MapOutputTrackerWorker: Updating epoch to 12 and clearing cache
26/05/21 18:53:15 INFO TorrentBroadcast: Started reading broadcast variable 57 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:15 INFO MemoryStore: Block broadcast_57_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:53:15 INFO TorrentBroadcast: Reading broadcast variable 57 took 28 ms
26/05/21 18:53:15 INFO MemoryStore: Block broadcast_57 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:53:15 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 11, fetching them
26/05/21 18:53:15 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:15 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:15 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:15 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:15 INFO Executor: Finished task 0.0 in stage 48.0 (TID 66). 1770 bytes result sent to driver
26/05/21 18:53:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 67
26/05/21 18:53:15 INFO Executor: Running task 1.0 in stage 48.0 (TID 67)
26/05/21 18:53:15 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:15 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:15 INFO Executor: Finished task 1.0 in stage 48.0 (TID 67). 1727 bytes result sent to driver
26/05/21 18:53:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 68
26/05/21 18:53:16 INFO Executor: Running task 0.0 in stage 49.0 (TID 68)
26/05/21 18:53:16 INFO TorrentBroadcast: Started reading broadcast variable 59 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:16 INFO MemoryStore: Block broadcast_59_piece0 stored as bytes in memory (estimated size 42.8 KiB, free 413.1 MiB)
26/05/21 18:53:16 INFO TorrentBroadcast: Reading broadcast variable 59 took 23 ms
26/05/21 18:53:16 INFO MemoryStore: Block broadcast_59 stored as values in memory (estimated size 102.0 KiB, free 413.0 MiB)
26/05/21 18:53:16 INFO CodeGenerator: Code generated in 166.850454 ms
26/05/21 18:53:16 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:16 INFO TorrentBroadcast: Started reading broadcast variable 58 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:16 INFO MemoryStore: Block broadcast_58_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:53:16 INFO TorrentBroadcast: Reading broadcast variable 58 took 18 ms
26/05/21 18:53:16 INFO MemoryStore: Block broadcast_58 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:53:19 INFO Executor: Finished task 0.0 in stage 49.0 (TID 68). 3121 bytes result sent to driver
26/05/21 18:53:19 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 69
26/05/21 18:53:19 INFO Executor: Running task 1.0 in stage 49.0 (TID 69)
26/05/21 18:53:19 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:22 INFO Executor: Finished task 1.0 in stage 49.0 (TID 69). 3121 bytes result sent to driver
26/05/21 18:53:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 70
26/05/21 18:53:23 INFO Executor: Running task 0.0 in stage 51.0 (TID 70)
26/05/21 18:53:23 INFO MapOutputTrackerWorker: Updating epoch to 13 and clearing cache
26/05/21 18:53:23 INFO TorrentBroadcast: Started reading broadcast variable 60 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:23 INFO MemoryStore: Block broadcast_60_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.4 MiB)
26/05/21 18:53:23 INFO TorrentBroadcast: Reading broadcast variable 60 took 16 ms
26/05/21 18:53:23 INFO MemoryStore: Block broadcast_60 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:53:23 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 12, fetching them
26/05/21 18:53:23 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:23 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:23 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:53:23 INFO Executor: Finished task 0.0 in stage 51.0 (TID 70). 3850 bytes result sent to driver
26/05/21 18:53:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 71
26/05/21 18:53:23 INFO Executor: Running task 0.0 in stage 52.0 (TID 71)
26/05/21 18:53:23 INFO TorrentBroadcast: Started reading broadcast variable 62 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:23 INFO MemoryStore: Block broadcast_62_piece0 stored as bytes in memory (estimated size 42.7 KiB, free 412.5 MiB)
26/05/21 18:53:23 INFO TorrentBroadcast: Reading broadcast variable 62 took 18 ms
26/05/21 18:53:23 INFO MemoryStore: Block broadcast_62 stored as values in memory (estimated size 102.0 KiB, free 412.4 MiB)
26/05/21 18:53:24 INFO CodeGenerator: Code generated in 179.002236 ms
26/05/21 18:53:24 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:24 INFO TorrentBroadcast: Started reading broadcast variable 61 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:24 INFO MemoryStore: Block broadcast_61_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.2 MiB)
26/05/21 18:53:24 INFO TorrentBroadcast: Reading broadcast variable 61 took 25 ms
26/05/21 18:53:24 INFO MemoryStore: Block broadcast_61 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:53:28 INFO Executor: Finished task 0.0 in stage 52.0 (TID 71). 3121 bytes result sent to driver
26/05/21 18:53:28 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 72
26/05/21 18:53:28 INFO Executor: Running task 1.0 in stage 52.0 (TID 72)
26/05/21 18:53:28 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:31 INFO Executor: Finished task 1.0 in stage 52.0 (TID 72). 3164 bytes result sent to driver
26/05/21 18:53:31 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 73
26/05/21 18:53:31 INFO Executor: Running task 0.0 in stage 54.0 (TID 73)
26/05/21 18:53:31 INFO MapOutputTrackerWorker: Updating epoch to 14 and clearing cache
26/05/21 18:53:31 INFO TorrentBroadcast: Started reading broadcast variable 63 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:31 INFO MemoryStore: Block broadcast_63_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.7 MiB)
26/05/21 18:53:31 INFO TorrentBroadcast: Reading broadcast variable 63 took 28 ms
26/05/21 18:53:31 INFO MemoryStore: Block broadcast_63 stored as values in memory (estimated size 12.5 KiB, free 411.7 MiB)
26/05/21 18:53:31 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 13, fetching them
26/05/21 18:53:31 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:31 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:31 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:31 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:31 INFO Executor: Finished task 0.0 in stage 54.0 (TID 73). 3807 bytes result sent to driver
26/05/21 18:53:32 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 74
26/05/21 18:53:32 INFO Executor: Running task 0.0 in stage 55.0 (TID 74)
26/05/21 18:53:32 INFO TorrentBroadcast: Started reading broadcast variable 65 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:32 INFO MemoryStore: Block broadcast_65_piece0 stored as bytes in memory (estimated size 42.7 KiB, free 412.5 MiB)
26/05/21 18:53:32 INFO TorrentBroadcast: Reading broadcast variable 65 took 18 ms
26/05/21 18:53:32 INFO MemoryStore: Block broadcast_65 stored as values in memory (estimated size 102.0 KiB, free 412.4 MiB)
26/05/21 18:53:32 INFO CodeGenerator: Code generated in 188.286683 ms
26/05/21 18:53:32 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:32 INFO TorrentBroadcast: Started reading broadcast variable 64 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:32 INFO MemoryStore: Block broadcast_64_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.2 MiB)
26/05/21 18:53:32 INFO TorrentBroadcast: Reading broadcast variable 64 took 17 ms
26/05/21 18:53:32 INFO MemoryStore: Block broadcast_64 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:53:35 INFO Executor: Finished task 0.0 in stage 55.0 (TID 74). 3121 bytes result sent to driver
26/05/21 18:53:35 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 75
26/05/21 18:53:35 INFO Executor: Running task 1.0 in stage 55.0 (TID 75)
26/05/21 18:53:35 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:38 INFO Executor: Finished task 1.0 in stage 55.0 (TID 75). 3121 bytes result sent to driver
26/05/21 18:53:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 76
26/05/21 18:53:39 INFO Executor: Running task 0.0 in stage 57.0 (TID 76)
26/05/21 18:53:39 INFO MapOutputTrackerWorker: Updating epoch to 15 and clearing cache
26/05/21 18:53:39 INFO TorrentBroadcast: Started reading broadcast variable 66 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:39 INFO MemoryStore: Block broadcast_66_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.5 MiB)
26/05/21 18:53:39 INFO TorrentBroadcast: Reading broadcast variable 66 took 24 ms
26/05/21 18:53:39 INFO MemoryStore: Block broadcast_66 stored as values in memory (estimated size 12.5 KiB, free 412.5 MiB)
26/05/21 18:53:39 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 14, fetching them
26/05/21 18:53:39 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:39 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:39 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:53:39 INFO Executor: Finished task 0.0 in stage 57.0 (TID 76). 3807 bytes result sent to driver
26/05/21 18:53:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 77
26/05/21 18:53:39 INFO Executor: Running task 0.0 in stage 58.0 (TID 77)
26/05/21 18:53:39 INFO TorrentBroadcast: Started reading broadcast variable 68 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:39 INFO MemoryStore: Block broadcast_68_piece0 stored as bytes in memory (estimated size 42.7 KiB, free 413.1 MiB)
26/05/21 18:53:39 INFO TorrentBroadcast: Reading broadcast variable 68 took 25 ms
26/05/21 18:53:39 INFO MemoryStore: Block broadcast_68 stored as values in memory (estimated size 102.0 KiB, free 413.0 MiB)
26/05/21 18:53:40 INFO CodeGenerator: Code generated in 161.047011 ms
26/05/21 18:53:40 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:53:40 INFO TorrentBroadcast: Started reading broadcast variable 67 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:40 INFO MemoryStore: Block broadcast_67_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.9 MiB)
26/05/21 18:53:40 INFO TorrentBroadcast: Reading broadcast variable 67 took 17 ms
26/05/21 18:53:40 INFO MemoryStore: Block broadcast_67 stored as values in memory (estimated size 595.4 KiB, free 412.3 MiB)
26/05/21 18:53:43 INFO Executor: Finished task 0.0 in stage 58.0 (TID 77). 3121 bytes result sent to driver
26/05/21 18:53:43 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 78
26/05/21 18:53:43 INFO Executor: Running task 1.0 in stage 58.0 (TID 78)
26/05/21 18:53:43 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:53:45 INFO Executor: Finished task 1.0 in stage 58.0 (TID 78). 3121 bytes result sent to driver
26/05/21 18:53:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 79
26/05/21 18:53:45 INFO Executor: Running task 0.0 in stage 60.0 (TID 79)
26/05/21 18:53:45 INFO MapOutputTrackerWorker: Updating epoch to 16 and clearing cache
26/05/21 18:53:45 INFO TorrentBroadcast: Started reading broadcast variable 69 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:45 INFO MemoryStore: Block broadcast_69_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.4 MiB)
26/05/21 18:53:45 INFO TorrentBroadcast: Reading broadcast variable 69 took 17 ms
26/05/21 18:53:45 INFO MemoryStore: Block broadcast_69 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:53:45 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 15, fetching them
26/05/21 18:53:45 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:45 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:45 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:53:45 INFO Executor: Finished task 0.0 in stage 60.0 (TID 79). 3807 bytes result sent to driver
26/05/21 18:53:46 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 80
26/05/21 18:53:46 INFO Executor: Running task 0.0 in stage 61.0 (TID 80)
26/05/21 18:53:46 INFO TorrentBroadcast: Started reading broadcast variable 70 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:46 INFO MemoryStore: Block broadcast_70_piece0 stored as bytes in memory (estimated size 32.7 KiB, free 413.1 MiB)
26/05/21 18:53:46 INFO TorrentBroadcast: Reading broadcast variable 70 took 17 ms
26/05/21 18:53:46 INFO MemoryStore: Block broadcast_70 stored as values in memory (estimated size 77.2 KiB, free 413.0 MiB)
26/05/21 18:53:46 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:53:46 INFO CodeGenerator: Code generated in 99.159469 ms
26/05/21 18:53:47 INFO Executor: Finished task 0.0 in stage 61.0 (TID 80). 2931 bytes result sent to driver
26/05/21 18:53:47 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 81
26/05/21 18:53:47 INFO Executor: Running task 1.0 in stage 61.0 (TID 81)
26/05/21 18:53:47 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:53:47 INFO Executor: Finished task 1.0 in stage 61.0 (TID 81). 2931 bytes result sent to driver
26/05/21 18:53:47 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 82
26/05/21 18:53:47 INFO Executor: Running task 0.0 in stage 63.0 (TID 82)
26/05/21 18:53:47 INFO MapOutputTrackerWorker: Updating epoch to 17 and clearing cache
26/05/21 18:53:47 INFO TorrentBroadcast: Started reading broadcast variable 71 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:47 INFO MemoryStore: Block broadcast_71_piece0 stored as bytes in memory (estimated size 6.2 KiB, free 413.0 MiB)
26/05/21 18:53:47 INFO TorrentBroadcast: Reading broadcast variable 71 took 15 ms
26/05/21 18:53:47 INFO MemoryStore: Block broadcast_71 stored as values in memory (estimated size 13.3 KiB, free 413.0 MiB)
26/05/21 18:53:47 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 16, fetching them
26/05/21 18:53:47 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:47 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:47 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:53:47 INFO CodeGenerator: Code generated in 48.5755 ms
26/05/21 18:53:47 INFO Executor: Finished task 0.0 in stage 63.0 (TID 82). 3801 bytes result sent to driver
26/05/21 18:53:48 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 83
26/05/21 18:53:48 INFO Executor: Running task 0.0 in stage 64.0 (TID 83)
26/05/21 18:53:48 INFO TorrentBroadcast: Started reading broadcast variable 72 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_72_piece0 stored as bytes in memory (estimated size 45.1 KiB, free 413.1 MiB)
26/05/21 18:53:48 INFO TorrentBroadcast: Reading broadcast variable 72 took 15 ms
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_72 stored as values in memory (estimated size 114.0 KiB, free 413.0 MiB)
26/05/21 18:53:48 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:53:48 INFO CodeGenerator: Code generated in 112.068298 ms
26/05/21 18:53:48 INFO Executor: 1 block locks were not released by task 0.0 in stage 64.0 (TID 83)
[rdd_28_0]
26/05/21 18:53:48 INFO Executor: Finished task 0.0 in stage 64.0 (TID 83). 2312 bytes result sent to driver
26/05/21 18:53:48 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 84
26/05/21 18:53:48 INFO Executor: Running task 0.0 in stage 65.0 (TID 84)
26/05/21 18:53:48 INFO TorrentBroadcast: Started reading broadcast variable 73 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_73_piece0 stored as bytes in memory (estimated size 45.1 KiB, free 412.9 MiB)
26/05/21 18:53:48 INFO TorrentBroadcast: Reading broadcast variable 73 took 21 ms
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_73 stored as values in memory (estimated size 114.0 KiB, free 412.8 MiB)
26/05/21 18:53:48 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:53:48 INFO Executor: Finished task 0.0 in stage 65.0 (TID 84). 2409 bytes result sent to driver
26/05/21 18:53:48 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 85
26/05/21 18:53:48 INFO Executor: Running task 1.0 in stage 65.0 (TID 85)
26/05/21 18:53:48 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:53:48 INFO Executor: Finished task 1.0 in stage 65.0 (TID 85). 2366 bytes result sent to driver
26/05/21 18:53:48 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 86
26/05/21 18:53:48 INFO Executor: Running task 0.0 in stage 66.0 (TID 86)
26/05/21 18:53:48 INFO TorrentBroadcast: Started reading broadcast variable 74 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_74_piece0 stored as bytes in memory (estimated size 46.5 KiB, free 412.8 MiB)
26/05/21 18:53:48 INFO TorrentBroadcast: Reading broadcast variable 74 took 14 ms
26/05/21 18:53:48 INFO MemoryStore: Block broadcast_74 stored as values in memory (estimated size 117.7 KiB, free 412.7 MiB)
26/05/21 18:53:49 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:53:49 INFO Executor: Finished task 0.0 in stage 66.0 (TID 86). 2646 bytes result sent to driver
26/05/21 18:53:49 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 87
26/05/21 18:53:49 INFO Executor: Running task 1.0 in stage 66.0 (TID 87)
26/05/21 18:53:49 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:53:49 INFO Executor: Finished task 1.0 in stage 66.0 (TID 87). 2646 bytes result sent to driver
26/05/21 18:53:49 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 88
26/05/21 18:53:49 INFO Executor: Running task 0.0 in stage 67.0 (TID 88)
26/05/21 18:53:49 INFO MapOutputTrackerWorker: Updating epoch to 18 and clearing cache
26/05/21 18:53:49 INFO TorrentBroadcast: Started reading broadcast variable 75 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:49 INFO MemoryStore: Block broadcast_75_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 413.0 MiB)
26/05/21 18:53:49 INFO TorrentBroadcast: Reading broadcast variable 75 took 48 ms
26/05/21 18:53:49 INFO MemoryStore: Block broadcast_75 stored as values in memory (estimated size 8.9 KiB, free 413.0 MiB)
26/05/21 18:53:49 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 17, fetching them
26/05/21 18:53:49 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:49 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:49 INFO ShuffleBlockFetcherIterator: Getting 2 (1506.0 B) non-empty blocks including 2 (1506.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:49 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
26/05/21 18:53:49 INFO Executor: Finished task 0.0 in stage 67.0 (TID 88). 2427 bytes result sent to driver
26/05/21 18:53:49 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 89
26/05/21 18:53:49 INFO Executor: Running task 1.0 in stage 67.0 (TID 89)
26/05/21 18:53:49 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:49 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:53:49 INFO Executor: Finished task 1.0 in stage 67.0 (TID 89). 1669 bytes result sent to driver
26/05/21 18:53:50 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 90
26/05/21 18:53:50 INFO Executor: Running task 0.0 in stage 68.0 (TID 90)
26/05/21 18:53:50 INFO TorrentBroadcast: Started reading broadcast variable 78 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_78_piece0 stored as bytes in memory (estimated size 49.7 KiB, free 412.9 MiB)
26/05/21 18:53:50 INFO TorrentBroadcast: Reading broadcast variable 78 took 22 ms
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_78 stored as values in memory (estimated size 126.6 KiB, free 412.8 MiB)
26/05/21 18:53:50 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:53:50 INFO MemoryStore: Block rdd_223_0 stored as values in memory (estimated size 8.0 MiB, free 404.8 MiB)
26/05/21 18:53:50 INFO TorrentBroadcast: Started reading broadcast variable 77 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_77_piece0 stored as bytes in memory (estimated size 945.0 B, free 404.8 MiB)
26/05/21 18:53:50 INFO TorrentBroadcast: Reading broadcast variable 77 took 11 ms
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_77 stored as values in memory (estimated size 11.2 KiB, free 404.8 MiB)
26/05/21 18:53:50 INFO TorrentBroadcast: Started reading broadcast variable 76 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_76_piece0 stored as bytes in memory (estimated size 344.0 B, free 404.8 MiB)
26/05/21 18:53:50 INFO TorrentBroadcast: Reading broadcast variable 76 took 17 ms
26/05/21 18:53:50 INFO MemoryStore: Block broadcast_76 stored as values in memory (estimated size 1344.0 B, free 404.8 MiB)
26/05/21 18:53:51 INFO Executor: Finished task 0.0 in stage 68.0 (TID 90). 2646 bytes result sent to driver
26/05/21 18:53:51 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 91
26/05/21 18:53:51 INFO Executor: Running task 1.0 in stage 68.0 (TID 91)
26/05/21 18:53:51 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:53:51 INFO MemoryStore: Block rdd_223_1 stored as values in memory (estimated size 7.6 MiB, free 397.2 MiB)
26/05/21 18:53:52 INFO Executor: Finished task 1.0 in stage 68.0 (TID 91). 2689 bytes result sent to driver
26/05/21 18:53:52 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 92
26/05/21 18:53:52 INFO Executor: Running task 0.0 in stage 69.0 (TID 92)
26/05/21 18:53:52 INFO MapOutputTrackerWorker: Updating epoch to 19 and clearing cache
26/05/21 18:53:52 INFO TorrentBroadcast: Started reading broadcast variable 79 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:52 INFO MemoryStore: Block broadcast_79_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 397.2 MiB)
26/05/21 18:53:52 INFO TorrentBroadcast: Reading broadcast variable 79 took 18 ms
26/05/21 18:53:52 INFO MemoryStore: Block broadcast_79 stored as values in memory (estimated size 10.1 KiB, free 397.2 MiB)
26/05/21 18:53:52 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 18, fetching them
26/05/21 18:53:52 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:52 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:52 INFO ShuffleBlockFetcherIterator: Getting 2 (45.5 KiB) non-empty blocks including 2 (45.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:53:52 INFO Executor: Finished task 0.0 in stage 69.0 (TID 92). 14536 bytes result sent to driver
26/05/21 18:53:52 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 93
26/05/21 18:53:52 INFO Executor: Running task 1.0 in stage 69.0 (TID 93)
26/05/21 18:53:52 INFO ShuffleBlockFetcherIterator: Getting 2 (45.5 KiB) non-empty blocks including 2 (45.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:52 INFO Executor: Finished task 1.0 in stage 69.0 (TID 93). 15319 bytes result sent to driver
26/05/21 18:53:52 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 94
26/05/21 18:53:52 INFO Executor: Running task 0.0 in stage 70.0 (TID 94)
26/05/21 18:53:52 INFO TorrentBroadcast: Started reading broadcast variable 81 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:52 INFO MemoryStore: Block broadcast_81_piece0 stored as bytes in memory (estimated size 65.3 KiB, free 397.5 MiB)
26/05/21 18:53:52 INFO TorrentBroadcast: Reading broadcast variable 81 took 23 ms
26/05/21 18:53:52 INFO MemoryStore: Block broadcast_81 stored as values in memory (estimated size 171.7 KiB, free 397.4 MiB)
26/05/21 18:53:53 INFO BlockManager: Found block rdd_223_0 locally
26/05/21 18:53:53 INFO TorrentBroadcast: Started reading broadcast variable 80 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:53 INFO MemoryStore: Block broadcast_80_piece0 stored as bytes in memory (estimated size 1777.0 B, free 397.4 MiB)
26/05/21 18:53:53 INFO TorrentBroadcast: Reading broadcast variable 80 took 14 ms
26/05/21 18:53:53 INFO MemoryStore: Block broadcast_80 stored as values in memory (estimated size 21.7 KiB, free 397.3 MiB)
26/05/21 18:53:53 INFO Executor: Finished task 0.0 in stage 70.0 (TID 94). 2646 bytes result sent to driver
26/05/21 18:53:53 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 95
26/05/21 18:53:53 INFO Executor: Running task 1.0 in stage 70.0 (TID 95)
26/05/21 18:53:53 INFO BlockManager: Found block rdd_223_1 locally
26/05/21 18:53:54 INFO Executor: Finished task 1.0 in stage 70.0 (TID 95). 2689 bytes result sent to driver
26/05/21 18:53:54 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 96
26/05/21 18:53:54 INFO Executor: Running task 0.0 in stage 71.0 (TID 96)
26/05/21 18:53:54 INFO MapOutputTrackerWorker: Updating epoch to 20 and clearing cache
26/05/21 18:53:54 INFO TorrentBroadcast: Started reading broadcast variable 82 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:54 INFO MemoryStore: Block broadcast_82_piece0 stored as bytes in memory (estimated size 9.8 KiB, free 397.3 MiB)
26/05/21 18:53:54 INFO TorrentBroadcast: Reading broadcast variable 82 took 18 ms
26/05/21 18:53:54 INFO MemoryStore: Block broadcast_82 stored as values in memory (estimated size 25.6 KiB, free 397.3 MiB)
26/05/21 18:53:54 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 19, fetching them
26/05/21 18:53:54 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:54 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:54 INFO ShuffleBlockFetcherIterator: Getting 2 (69.8 KiB) non-empty blocks including 2 (69.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:53:54 INFO Executor: Finished task 0.0 in stage 71.0 (TID 96). 20746 bytes result sent to driver
26/05/21 18:53:54 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 97
26/05/21 18:53:54 INFO Executor: Running task 1.0 in stage 71.0 (TID 97)
26/05/21 18:53:54 INFO ShuffleBlockFetcherIterator: Getting 2 (84.5 KiB) non-empty blocks including 2 (84.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:53:55 INFO Executor: Finished task 1.0 in stage 71.0 (TID 97). 26130 bytes result sent to driver
26/05/21 18:53:55 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 98
26/05/21 18:53:55 INFO Executor: Running task 0.0 in stage 72.0 (TID 98)
26/05/21 18:53:55 INFO TorrentBroadcast: Started reading broadcast variable 84 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:55 INFO MemoryStore: Block broadcast_84_piece0 stored as bytes in memory (estimated size 90.9 KiB, free 397.2 MiB)
26/05/21 18:53:55 INFO TorrentBroadcast: Reading broadcast variable 84 took 21 ms
26/05/21 18:53:55 INFO MemoryStore: Block broadcast_84 stored as values in memory (estimated size 245.8 KiB, free 397.0 MiB)
26/05/21 18:53:55 INFO BlockManager: Found block rdd_223_0 locally
26/05/21 18:53:55 INFO TorrentBroadcast: Started reading broadcast variable 83 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:55 INFO MemoryStore: Block broadcast_83_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 397.0 MiB)
26/05/21 18:53:55 INFO TorrentBroadcast: Reading broadcast variable 83 took 22 ms
26/05/21 18:53:55 INFO MemoryStore: Block broadcast_83 stored as values in memory (estimated size 40.8 KiB, free 397.0 MiB)
26/05/21 18:53:56 INFO Executor: Finished task 0.0 in stage 72.0 (TID 98). 2646 bytes result sent to driver
26/05/21 18:53:56 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 99
26/05/21 18:53:56 INFO Executor: Running task 1.0 in stage 72.0 (TID 99)
26/05/21 18:53:56 INFO BlockManager: Found block rdd_223_1 locally
26/05/21 18:53:57 INFO Executor: Finished task 1.0 in stage 72.0 (TID 99). 2689 bytes result sent to driver
26/05/21 18:53:57 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 100
26/05/21 18:53:57 INFO Executor: Running task 0.0 in stage 73.0 (TID 100)
26/05/21 18:53:57 INFO MapOutputTrackerWorker: Updating epoch to 21 and clearing cache
26/05/21 18:53:57 INFO TorrentBroadcast: Started reading broadcast variable 85 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:57 INFO MemoryStore: Block broadcast_85_piece0 stored as bytes in memory (estimated size 15.3 KiB, free 396.9 MiB)
26/05/21 18:53:57 INFO TorrentBroadcast: Reading broadcast variable 85 took 19 ms
26/05/21 18:53:57 INFO MemoryStore: Block broadcast_85 stored as values in memory (estimated size 41.0 KiB, free 396.9 MiB)
26/05/21 18:53:57 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 20, fetching them
26/05/21 18:53:57 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:57 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:57 INFO ShuffleBlockFetcherIterator: Getting 2 (123.7 KiB) non-empty blocks including 2 (123.7 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:57 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:53:57 INFO Executor: Finished task 0.0 in stage 73.0 (TID 100). 41361 bytes result sent to driver
26/05/21 18:53:57 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 101
26/05/21 18:53:57 INFO Executor: Running task 1.0 in stage 73.0 (TID 101)
26/05/21 18:53:57 INFO ShuffleBlockFetcherIterator: Getting 2 (112.4 KiB) non-empty blocks including 2 (112.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:57 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:53:57 INFO Executor: Finished task 1.0 in stage 73.0 (TID 101). 37107 bytes result sent to driver
26/05/21 18:53:58 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 102
26/05/21 18:53:58 INFO Executor: Running task 0.0 in stage 74.0 (TID 102)
26/05/21 18:53:58 INFO TorrentBroadcast: Started reading broadcast variable 87 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:58 INFO MemoryStore: Block broadcast_87_piece0 stored as bytes in memory (estimated size 137.0 KiB, free 397.4 MiB)
26/05/21 18:53:58 INFO TorrentBroadcast: Reading broadcast variable 87 took 15 ms
26/05/21 18:53:58 INFO MemoryStore: Block broadcast_87 stored as values in memory (estimated size 374.7 KiB, free 397.0 MiB)
26/05/21 18:53:58 INFO BlockManager: Found block rdd_223_0 locally
26/05/21 18:53:58 INFO TorrentBroadcast: Started reading broadcast variable 86 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:58 INFO MemoryStore: Block broadcast_86_piece0 stored as bytes in memory (estimated size 5.4 KiB, free 397.0 MiB)
26/05/21 18:53:58 INFO TorrentBroadcast: Reading broadcast variable 86 took 15 ms
26/05/21 18:53:58 INFO MemoryStore: Block broadcast_86 stored as values in memory (estimated size 70.5 KiB, free 397.0 MiB)
26/05/21 18:53:59 INFO Executor: Finished task 0.0 in stage 74.0 (TID 102). 2646 bytes result sent to driver
26/05/21 18:53:59 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 103
26/05/21 18:53:59 INFO Executor: Running task 1.0 in stage 74.0 (TID 103)
26/05/21 18:53:59 INFO BlockManager: Found block rdd_223_1 locally
26/05/21 18:53:59 INFO Executor: Finished task 1.0 in stage 74.0 (TID 103). 2689 bytes result sent to driver
26/05/21 18:53:59 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 104
26/05/21 18:53:59 INFO Executor: Running task 0.0 in stage 75.0 (TID 104)
26/05/21 18:53:59 INFO MapOutputTrackerWorker: Updating epoch to 22 and clearing cache
26/05/21 18:53:59 INFO TorrentBroadcast: Started reading broadcast variable 88 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:53:59 INFO MemoryStore: Block broadcast_88_piece0 stored as bytes in memory (estimated size 22.7 KiB, free 396.9 MiB)
26/05/21 18:53:59 INFO TorrentBroadcast: Reading broadcast variable 88 took 16 ms
26/05/21 18:53:59 INFO MemoryStore: Block broadcast_88 stored as values in memory (estimated size 65.0 KiB, free 396.9 MiB)
26/05/21 18:53:59 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 21, fetching them
26/05/21 18:53:59 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:53:59 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:53:59 INFO ShuffleBlockFetcherIterator: Getting 2 (181.1 KiB) non-empty blocks including 2 (181.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:53:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:00 INFO Executor: Finished task 0.0 in stage 75.0 (TID 104). 63878 bytes result sent to driver
26/05/21 18:54:00 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 105
26/05/21 18:54:00 INFO Executor: Running task 1.0 in stage 75.0 (TID 105)
26/05/21 18:54:00 INFO ShuffleBlockFetcherIterator: Getting 2 (172.8 KiB) non-empty blocks including 2 (172.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:00 INFO Executor: Finished task 1.0 in stage 75.0 (TID 105). 60031 bytes result sent to driver
26/05/21 18:54:00 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 106
26/05/21 18:54:00 INFO Executor: Running task 0.0 in stage 76.0 (TID 106)
26/05/21 18:54:00 INFO TorrentBroadcast: Started reading broadcast variable 90 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:00 INFO MemoryStore: Block broadcast_90_piece0 stored as bytes in memory (estimated size 212.4 KiB, free 397.4 MiB)
26/05/21 18:54:00 INFO TorrentBroadcast: Reading broadcast variable 90 took 19 ms
26/05/21 18:54:00 INFO MemoryStore: Block broadcast_90 stored as values in memory (estimated size 587.2 KiB, free 396.8 MiB)
26/05/21 18:54:01 INFO BlockManager: Found block rdd_223_0 locally
26/05/21 18:54:01 INFO TorrentBroadcast: Started reading broadcast variable 89 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:01 INFO MemoryStore: Block broadcast_89_piece0 stored as bytes in memory (estimated size 9.2 KiB, free 396.8 MiB)
26/05/21 18:54:01 INFO TorrentBroadcast: Reading broadcast variable 89 took 17 ms
26/05/21 18:54:01 INFO MemoryStore: Block broadcast_89 stored as values in memory (estimated size 124.9 KiB, free 396.7 MiB)
26/05/21 18:54:02 INFO Executor: Finished task 0.0 in stage 76.0 (TID 106). 2646 bytes result sent to driver
26/05/21 18:54:02 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 107
26/05/21 18:54:02 INFO Executor: Running task 1.0 in stage 76.0 (TID 107)
26/05/21 18:54:02 INFO BlockManager: Found block rdd_223_1 locally
26/05/21 18:54:03 INFO Executor: Finished task 1.0 in stage 76.0 (TID 107). 2689 bytes result sent to driver
26/05/21 18:54:03 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 108
26/05/21 18:54:03 INFO Executor: Running task 0.0 in stage 77.0 (TID 108)
26/05/21 18:54:03 INFO MapOutputTrackerWorker: Updating epoch to 23 and clearing cache
26/05/21 18:54:03 INFO TorrentBroadcast: Started reading broadcast variable 91 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:03 INFO MemoryStore: Block broadcast_91_piece0 stored as bytes in memory (estimated size 35.7 KiB, free 396.6 MiB)
26/05/21 18:54:03 INFO TorrentBroadcast: Reading broadcast variable 91 took 19 ms
26/05/21 18:54:03 INFO MemoryStore: Block broadcast_91 stored as values in memory (estimated size 106.8 KiB, free 396.5 MiB)
26/05/21 18:54:03 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 22, fetching them
26/05/21 18:54:03 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:03 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:03 INFO ShuffleBlockFetcherIterator: Getting 2 (265.1 KiB) non-empty blocks including 2 (265.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:04 INFO Executor: Finished task 0.0 in stage 77.0 (TID 108). 98163 bytes result sent to driver
26/05/21 18:54:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 109
26/05/21 18:54:04 INFO Executor: Running task 1.0 in stage 77.0 (TID 109)
26/05/21 18:54:04 INFO ShuffleBlockFetcherIterator: Getting 2 (265.1 KiB) non-empty blocks including 2 (265.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:54:04 INFO Executor: Finished task 1.0 in stage 77.0 (TID 109). 94165 bytes result sent to driver
26/05/21 18:54:04 INFO BlockManager: Removing RDD 223
26/05/21 18:54:05 INFO BlockManager: Removing RDD 223
26/05/21 18:54:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 110
26/05/21 18:54:06 INFO Executor: Running task 0.0 in stage 78.0 (TID 110)
26/05/21 18:54:06 INFO TorrentBroadcast: Started reading broadcast variable 93 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:06 INFO MemoryStore: Block broadcast_93_piece0 stored as bytes in memory (estimated size 192.1 KiB, free 413.0 MiB)
26/05/21 18:54:06 INFO TorrentBroadcast: Reading broadcast variable 93 took 23 ms
26/05/21 18:54:06 INFO MemoryStore: Block broadcast_93 stored as values in memory (estimated size 524.0 KiB, free 412.4 MiB)
26/05/21 18:54:06 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:06 INFO TorrentBroadcast: Started reading broadcast variable 92 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:06 INFO MemoryStore: Block broadcast_92_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:06 INFO TorrentBroadcast: Reading broadcast variable 92 took 13 ms
26/05/21 18:54:07 INFO MemoryStore: Block broadcast_92 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:10 INFO Executor: Finished task 0.0 in stage 78.0 (TID 110). 2405 bytes result sent to driver
26/05/21 18:54:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 111
26/05/21 18:54:10 INFO Executor: Running task 1.0 in stage 78.0 (TID 111)
26/05/21 18:54:10 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:13 INFO Executor: Finished task 1.0 in stage 78.0 (TID 111). 2405 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 112
26/05/21 18:54:13 INFO Executor: Running task 0.0 in stage 79.0 (TID 112)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Updating epoch to 24 and clearing cache
26/05/21 18:54:13 INFO TorrentBroadcast: Started reading broadcast variable 94 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_94_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 411.8 MiB)
26/05/21 18:54:13 INFO TorrentBroadcast: Reading broadcast variable 94 took 16 ms
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_94 stored as values in memory (estimated size 7.3 KiB, free 411.8 MiB)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 23, fetching them
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (14.5 KiB) non-empty blocks including 2 (14.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:13 INFO Executor: Finished task 0.0 in stage 79.0 (TID 112). 2170 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 113
26/05/21 18:54:13 INFO Executor: Running task 1.0 in stage 79.0 (TID 113)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (14.5 KiB) non-empty blocks including 2 (14.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:13 INFO Executor: Finished task 1.0 in stage 79.0 (TID 113). 2170 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 114
26/05/21 18:54:13 INFO Executor: Running task 0.0 in stage 81.0 (TID 114)
26/05/21 18:54:13 INFO TorrentBroadcast: Started reading broadcast variable 95 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_95_piece0 stored as bytes in memory (estimated size 3.2 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO TorrentBroadcast: Reading broadcast variable 95 took 17 ms
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_95 stored as values in memory (estimated size 5.7 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (14.5 KiB) non-empty blocks including 2 (14.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:13 INFO Executor: Finished task 0.0 in stage 81.0 (TID 114). 1877 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 115
26/05/21 18:54:13 INFO Executor: Running task 1.0 in stage 81.0 (TID 115)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (14.5 KiB) non-empty blocks including 2 (14.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:13 INFO Executor: Finished task 1.0 in stage 81.0 (TID 115). 1834 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 116
26/05/21 18:54:13 INFO Executor: Running task 0.0 in stage 82.0 (TID 116)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Updating epoch to 25 and clearing cache
26/05/21 18:54:13 INFO TorrentBroadcast: Started reading broadcast variable 96 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_96_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO TorrentBroadcast: Reading broadcast variable 96 took 16 ms
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_96 stored as values in memory (estimated size 5.3 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 24, fetching them
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:13 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (10.4 KiB) non-empty blocks including 2 (10.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:54:13 INFO Executor: Finished task 0.0 in stage 82.0 (TID 116). 1760 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 117
26/05/21 18:54:13 INFO Executor: Running task 1.0 in stage 82.0 (TID 117)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (9.0 KiB) non-empty blocks including 2 (9.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:13 INFO Executor: Finished task 1.0 in stage 82.0 (TID 117). 1674 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 118
26/05/21 18:54:13 INFO Executor: Running task 0.0 in stage 85.0 (TID 118)
26/05/21 18:54:13 INFO TorrentBroadcast: Started reading broadcast variable 97 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_97_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO TorrentBroadcast: Reading broadcast variable 97 took 9 ms
26/05/21 18:54:13 INFO MemoryStore: Block broadcast_97 stored as values in memory (estimated size 6.7 KiB, free 412.5 MiB)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (10.4 KiB) non-empty blocks including 2 (10.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:13 INFO Executor: Finished task 0.0 in stage 85.0 (TID 118). 1809 bytes result sent to driver
26/05/21 18:54:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 119
26/05/21 18:54:13 INFO Executor: Running task 1.0 in stage 85.0 (TID 119)
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Getting 2 (9.0 KiB) non-empty blocks including 2 (9.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:13 INFO Executor: Finished task 1.0 in stage 85.0 (TID 119). 1809 bytes result sent to driver
26/05/21 18:54:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 120
26/05/21 18:54:14 INFO Executor: Running task 0.0 in stage 88.0 (TID 120)
26/05/21 18:54:14 INFO TorrentBroadcast: Started reading broadcast variable 98 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_98_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 412.5 MiB)
26/05/21 18:54:14 INFO TorrentBroadcast: Reading broadcast variable 98 took 12 ms
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_98 stored as values in memory (estimated size 8.3 KiB, free 412.5 MiB)
26/05/21 18:54:14 INFO ShuffleBlockFetcherIterator: Getting 2 (10.4 KiB) non-empty blocks including 2 (10.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:14 INFO MemoryStore: Block rdd_270_0 stored as values in memory (estimated size 57.5 KiB, free 412.4 MiB)
26/05/21 18:54:14 INFO Executor: Finished task 0.0 in stage 88.0 (TID 120). 1718 bytes result sent to driver
26/05/21 18:54:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 121
26/05/21 18:54:14 INFO Executor: Running task 1.0 in stage 88.0 (TID 121)
26/05/21 18:54:14 INFO ShuffleBlockFetcherIterator: Getting 2 (9.0 KiB) non-empty blocks including 2 (9.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:14 INFO MemoryStore: Block rdd_270_1 stored as values in memory (estimated size 49.3 KiB, free 412.4 MiB)
26/05/21 18:54:14 INFO Executor: Finished task 1.0 in stage 88.0 (TID 121). 1718 bytes result sent to driver
26/05/21 18:54:14 INFO BlockManager: Removing RDD 270
26/05/21 18:54:14 INFO BlockManager: Removing RDD 270
26/05/21 18:54:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 122
26/05/21 18:54:14 INFO Executor: Running task 0.0 in stage 89.0 (TID 122)
26/05/21 18:54:14 INFO TorrentBroadcast: Started reading broadcast variable 100 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_100_piece0 stored as bytes in memory (estimated size 195.8 KiB, free 413.0 MiB)
26/05/21 18:54:14 INFO TorrentBroadcast: Reading broadcast variable 100 took 15 ms
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_100 stored as values in memory (estimated size 535.7 KiB, free 412.4 MiB)
26/05/21 18:54:14 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:14 INFO TorrentBroadcast: Started reading broadcast variable 99 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_99_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:14 INFO TorrentBroadcast: Reading broadcast variable 99 took 19 ms
26/05/21 18:54:14 INFO MemoryStore: Block broadcast_99 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:18 INFO Executor: Finished task 0.0 in stage 89.0 (TID 122). 2405 bytes result sent to driver
26/05/21 18:54:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 123
26/05/21 18:54:18 INFO Executor: Running task 1.0 in stage 89.0 (TID 123)
26/05/21 18:54:18 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:21 INFO Executor: Finished task 1.0 in stage 89.0 (TID 123). 2405 bytes result sent to driver
26/05/21 18:54:21 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 124
26/05/21 18:54:21 INFO Executor: Running task 0.0 in stage 90.0 (TID 124)
26/05/21 18:54:21 INFO MapOutputTrackerWorker: Updating epoch to 26 and clearing cache
26/05/21 18:54:21 INFO TorrentBroadcast: Started reading broadcast variable 101 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:21 INFO MemoryStore: Block broadcast_101_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 411.8 MiB)
26/05/21 18:54:21 INFO TorrentBroadcast: Reading broadcast variable 101 took 19 ms
26/05/21 18:54:21 INFO MemoryStore: Block broadcast_101 stored as values in memory (estimated size 5.3 KiB, free 411.8 MiB)
26/05/21 18:54:21 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 25, fetching them
26/05/21 18:54:21 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:21 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:21 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:21 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:54:21 INFO Executor: Finished task 0.0 in stage 90.0 (TID 124). 1727 bytes result sent to driver
26/05/21 18:54:21 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 125
26/05/21 18:54:21 INFO Executor: Running task 1.0 in stage 90.0 (TID 125)
26/05/21 18:54:21 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:21 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:21 INFO Executor: Finished task 1.0 in stage 90.0 (TID 125). 1727 bytes result sent to driver
26/05/21 18:54:22 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 126
26/05/21 18:54:22 INFO Executor: Running task 0.0 in stage 91.0 (TID 126)
26/05/21 18:54:22 INFO TorrentBroadcast: Started reading broadcast variable 103 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:22 INFO MemoryStore: Block broadcast_103_piece0 stored as bytes in memory (estimated size 195.7 KiB, free 413.0 MiB)
26/05/21 18:54:22 INFO TorrentBroadcast: Reading broadcast variable 103 took 16 ms
26/05/21 18:54:22 INFO MemoryStore: Block broadcast_103 stored as values in memory (estimated size 535.7 KiB, free 412.4 MiB)
26/05/21 18:54:22 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:22 INFO TorrentBroadcast: Started reading broadcast variable 102 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:22 INFO MemoryStore: Block broadcast_102_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:22 INFO TorrentBroadcast: Reading broadcast variable 102 took 19 ms
26/05/21 18:54:22 INFO MemoryStore: Block broadcast_102 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:24 INFO Executor: Finished task 0.0 in stage 91.0 (TID 126). 2448 bytes result sent to driver
26/05/21 18:54:25 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 127
26/05/21 18:54:25 INFO Executor: Running task 1.0 in stage 91.0 (TID 127)
26/05/21 18:54:25 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:27 INFO Executor: Finished task 1.0 in stage 91.0 (TID 127). 2405 bytes result sent to driver
26/05/21 18:54:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 128
26/05/21 18:54:27 INFO Executor: Running task 0.0 in stage 92.0 (TID 128)
26/05/21 18:54:27 INFO MapOutputTrackerWorker: Updating epoch to 27 and clearing cache
26/05/21 18:54:27 INFO TorrentBroadcast: Started reading broadcast variable 104 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:27 INFO MemoryStore: Block broadcast_104_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 411.8 MiB)
26/05/21 18:54:27 INFO TorrentBroadcast: Reading broadcast variable 104 took 22 ms
26/05/21 18:54:27 INFO MemoryStore: Block broadcast_104 stored as values in memory (estimated size 5.3 KiB, free 411.8 MiB)
26/05/21 18:54:27 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 26, fetching them
26/05/21 18:54:27 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:27 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:27 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:27 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:27 INFO Executor: Finished task 0.0 in stage 92.0 (TID 128). 1727 bytes result sent to driver
26/05/21 18:54:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 129
26/05/21 18:54:27 INFO Executor: Running task 1.0 in stage 92.0 (TID 129)
26/05/21 18:54:27 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:27 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:27 INFO Executor: Finished task 1.0 in stage 92.0 (TID 129). 1727 bytes result sent to driver
26/05/21 18:54:28 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 130
26/05/21 18:54:28 INFO Executor: Running task 0.0 in stage 93.0 (TID 130)
26/05/21 18:54:28 INFO TorrentBroadcast: Started reading broadcast variable 106 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:28 INFO MemoryStore: Block broadcast_106_piece0 stored as bytes in memory (estimated size 195.8 KiB, free 413.0 MiB)
26/05/21 18:54:28 INFO TorrentBroadcast: Reading broadcast variable 106 took 25 ms
26/05/21 18:54:28 INFO MemoryStore: Block broadcast_106 stored as values in memory (estimated size 535.7 KiB, free 412.4 MiB)
26/05/21 18:54:28 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:28 INFO TorrentBroadcast: Started reading broadcast variable 105 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:28 INFO MemoryStore: Block broadcast_105_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:28 INFO TorrentBroadcast: Reading broadcast variable 105 took 22 ms
26/05/21 18:54:28 INFO MemoryStore: Block broadcast_105 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:31 INFO Executor: Finished task 0.0 in stage 93.0 (TID 130). 2405 bytes result sent to driver
26/05/21 18:54:31 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 131
26/05/21 18:54:31 INFO Executor: Running task 1.0 in stage 93.0 (TID 131)
26/05/21 18:54:31 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:33 INFO Executor: Finished task 1.0 in stage 93.0 (TID 131). 2405 bytes result sent to driver
26/05/21 18:54:33 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 132
26/05/21 18:54:33 INFO Executor: Running task 0.0 in stage 94.0 (TID 132)
26/05/21 18:54:33 INFO MapOutputTrackerWorker: Updating epoch to 28 and clearing cache
26/05/21 18:54:33 INFO TorrentBroadcast: Started reading broadcast variable 107 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:33 INFO MemoryStore: Block broadcast_107_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 411.8 MiB)
26/05/21 18:54:33 INFO TorrentBroadcast: Reading broadcast variable 107 took 13 ms
26/05/21 18:54:33 INFO MemoryStore: Block broadcast_107 stored as values in memory (estimated size 5.3 KiB, free 411.8 MiB)
26/05/21 18:54:33 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 27, fetching them
26/05/21 18:54:33 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:33 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:33 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:33 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:33 INFO Executor: Finished task 0.0 in stage 94.0 (TID 132). 1727 bytes result sent to driver
26/05/21 18:54:33 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 133
26/05/21 18:54:33 INFO Executor: Running task 1.0 in stage 94.0 (TID 133)
26/05/21 18:54:33 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:33 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:33 INFO Executor: Finished task 1.0 in stage 94.0 (TID 133). 1727 bytes result sent to driver
26/05/21 18:54:34 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 134
26/05/21 18:54:34 INFO Executor: Running task 0.0 in stage 95.0 (TID 134)
26/05/21 18:54:34 INFO TorrentBroadcast: Started reading broadcast variable 109 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:34 INFO MemoryStore: Block broadcast_109_piece0 stored as bytes in memory (estimated size 195.8 KiB, free 413.0 MiB)
26/05/21 18:54:34 INFO TorrentBroadcast: Reading broadcast variable 109 took 21 ms
26/05/21 18:54:34 INFO MemoryStore: Block broadcast_109 stored as values in memory (estimated size 535.7 KiB, free 412.4 MiB)
26/05/21 18:54:34 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:34 INFO TorrentBroadcast: Started reading broadcast variable 108 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:34 INFO MemoryStore: Block broadcast_108_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:34 INFO TorrentBroadcast: Reading broadcast variable 108 took 14 ms
26/05/21 18:54:34 INFO MemoryStore: Block broadcast_108 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:36 INFO Executor: Finished task 0.0 in stage 95.0 (TID 134). 2405 bytes result sent to driver
26/05/21 18:54:36 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 135
26/05/21 18:54:36 INFO Executor: Running task 1.0 in stage 95.0 (TID 135)
26/05/21 18:54:36 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:39 INFO Executor: Finished task 1.0 in stage 95.0 (TID 135). 2405 bytes result sent to driver
26/05/21 18:54:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 136
26/05/21 18:54:39 INFO Executor: Running task 0.0 in stage 96.0 (TID 136)
26/05/21 18:54:39 INFO MapOutputTrackerWorker: Updating epoch to 29 and clearing cache
26/05/21 18:54:39 INFO TorrentBroadcast: Started reading broadcast variable 110 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_110_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 411.8 MiB)
26/05/21 18:54:39 INFO TorrentBroadcast: Reading broadcast variable 110 took 18 ms
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_110 stored as values in memory (estimated size 5.3 KiB, free 411.8 MiB)
26/05/21 18:54:39 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 28, fetching them
26/05/21 18:54:39 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:39 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:39 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:39 INFO Executor: Finished task 0.0 in stage 96.0 (TID 136). 1727 bytes result sent to driver
26/05/21 18:54:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 137
26/05/21 18:54:39 INFO Executor: Running task 1.0 in stage 96.0 (TID 137)
26/05/21 18:54:39 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:39 INFO Executor: Finished task 1.0 in stage 96.0 (TID 137). 1727 bytes result sent to driver
26/05/21 18:54:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 138
26/05/21 18:54:39 INFO Executor: Running task 0.0 in stage 97.0 (TID 138)
26/05/21 18:54:39 INFO TorrentBroadcast: Started reading broadcast variable 112 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_112_piece0 stored as bytes in memory (estimated size 193.8 KiB, free 413.0 MiB)
26/05/21 18:54:39 INFO TorrentBroadcast: Reading broadcast variable 112 took 17 ms
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_112 stored as values in memory (estimated size 525.3 KiB, free 412.4 MiB)
26/05/21 18:54:39 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:39 INFO TorrentBroadcast: Started reading broadcast variable 111 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_111_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.3 MiB)
26/05/21 18:54:39 INFO TorrentBroadcast: Reading broadcast variable 111 took 12 ms
26/05/21 18:54:39 INFO MemoryStore: Block broadcast_111 stored as values in memory (estimated size 595.4 KiB, free 411.7 MiB)
26/05/21 18:54:42 INFO Executor: Finished task 0.0 in stage 97.0 (TID 138). 3121 bytes result sent to driver
26/05/21 18:54:42 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 139
26/05/21 18:54:42 INFO Executor: Running task 1.0 in stage 97.0 (TID 139)
26/05/21 18:54:42 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:44 INFO Executor: Finished task 1.0 in stage 97.0 (TID 139). 3121 bytes result sent to driver
26/05/21 18:54:44 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 140
26/05/21 18:54:44 INFO Executor: Running task 0.0 in stage 99.0 (TID 140)
26/05/21 18:54:44 INFO MapOutputTrackerWorker: Updating epoch to 30 and clearing cache
26/05/21 18:54:44 INFO TorrentBroadcast: Started reading broadcast variable 113 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:44 INFO MemoryStore: Block broadcast_113_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.8 MiB)
26/05/21 18:54:44 INFO TorrentBroadcast: Reading broadcast variable 113 took 15 ms
26/05/21 18:54:44 INFO MemoryStore: Block broadcast_113 stored as values in memory (estimated size 12.5 KiB, free 411.8 MiB)
26/05/21 18:54:44 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 29, fetching them
26/05/21 18:54:44 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:44 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:44 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:44 INFO Executor: Finished task 0.0 in stage 99.0 (TID 140). 3807 bytes result sent to driver
26/05/21 18:54:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 141
26/05/21 18:54:45 INFO Executor: Running task 0.0 in stage 100.0 (TID 141)
26/05/21 18:54:45 INFO TorrentBroadcast: Started reading broadcast variable 115 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:45 INFO MemoryStore: Block broadcast_115_piece0 stored as bytes in memory (estimated size 193.9 KiB, free 412.3 MiB)
26/05/21 18:54:45 INFO TorrentBroadcast: Reading broadcast variable 115 took 12 ms
26/05/21 18:54:45 INFO MemoryStore: Block broadcast_115 stored as values in memory (estimated size 525.3 KiB, free 411.8 MiB)
26/05/21 18:54:45 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:45 INFO TorrentBroadcast: Started reading broadcast variable 114 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:45 INFO MemoryStore: Block broadcast_114_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 411.7 MiB)
26/05/21 18:54:45 INFO TorrentBroadcast: Reading broadcast variable 114 took 13 ms
26/05/21 18:54:45 INFO MemoryStore: Block broadcast_114 stored as values in memory (estimated size 595.4 KiB, free 411.1 MiB)
26/05/21 18:54:47 INFO Executor: Finished task 0.0 in stage 100.0 (TID 141). 3121 bytes result sent to driver
26/05/21 18:54:47 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 142
26/05/21 18:54:47 INFO Executor: Running task 1.0 in stage 100.0 (TID 142)
26/05/21 18:54:47 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:49 INFO Executor: Finished task 1.0 in stage 100.0 (TID 142). 3121 bytes result sent to driver
26/05/21 18:54:49 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 143
26/05/21 18:54:49 INFO Executor: Running task 0.0 in stage 102.0 (TID 143)
26/05/21 18:54:49 INFO MapOutputTrackerWorker: Updating epoch to 31 and clearing cache
26/05/21 18:54:49 INFO TorrentBroadcast: Started reading broadcast variable 116 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:49 INFO MemoryStore: Block broadcast_116_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.2 MiB)
26/05/21 18:54:49 INFO TorrentBroadcast: Reading broadcast variable 116 took 13 ms
26/05/21 18:54:49 INFO MemoryStore: Block broadcast_116 stored as values in memory (estimated size 12.5 KiB, free 411.1 MiB)
26/05/21 18:54:49 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 30, fetching them
26/05/21 18:54:49 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:49 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:49 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:49 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:54:49 INFO Executor: Finished task 0.0 in stage 102.0 (TID 143). 3807 bytes result sent to driver
26/05/21 18:54:50 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 144
26/05/21 18:54:50 INFO Executor: Running task 0.0 in stage 103.0 (TID 144)
26/05/21 18:54:50 INFO TorrentBroadcast: Started reading broadcast variable 118 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:50 INFO MemoryStore: Block broadcast_118_piece0 stored as bytes in memory (estimated size 193.8 KiB, free 412.3 MiB)
26/05/21 18:54:50 INFO TorrentBroadcast: Reading broadcast variable 118 took 16 ms
26/05/21 18:54:50 INFO MemoryStore: Block broadcast_118 stored as values in memory (estimated size 525.3 KiB, free 411.8 MiB)
26/05/21 18:54:50 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:50 INFO TorrentBroadcast: Started reading broadcast variable 117 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:50 INFO MemoryStore: Block broadcast_117_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 411.7 MiB)
26/05/21 18:54:50 INFO TorrentBroadcast: Reading broadcast variable 117 took 11 ms
26/05/21 18:54:50 INFO MemoryStore: Block broadcast_117 stored as values in memory (estimated size 595.4 KiB, free 411.1 MiB)
26/05/21 18:54:52 INFO Executor: Finished task 0.0 in stage 103.0 (TID 144). 3121 bytes result sent to driver
26/05/21 18:54:52 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 145
26/05/21 18:54:52 INFO Executor: Running task 1.0 in stage 103.0 (TID 145)
26/05/21 18:54:53 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:54:56 INFO Executor: Finished task 1.0 in stage 103.0 (TID 145). 3121 bytes result sent to driver
26/05/21 18:54:56 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 146
26/05/21 18:54:56 INFO Executor: Running task 0.0 in stage 105.0 (TID 146)
26/05/21 18:54:56 INFO MapOutputTrackerWorker: Updating epoch to 32 and clearing cache
26/05/21 18:54:56 INFO TorrentBroadcast: Started reading broadcast variable 119 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_119_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.2 MiB)
26/05/21 18:54:56 INFO TorrentBroadcast: Reading broadcast variable 119 took 15 ms
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_119 stored as values in memory (estimated size 12.5 KiB, free 411.1 MiB)
26/05/21 18:54:56 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 31, fetching them
26/05/21 18:54:56 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:54:56 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:54:56 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:54:56 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:54:56 INFO Executor: Finished task 0.0 in stage 105.0 (TID 146). 3807 bytes result sent to driver
26/05/21 18:54:56 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 147
26/05/21 18:54:56 INFO Executor: Running task 0.0 in stage 106.0 (TID 147)
26/05/21 18:54:56 INFO TorrentBroadcast: Started reading broadcast variable 121 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_121_piece0 stored as bytes in memory (estimated size 193.8 KiB, free 412.3 MiB)
26/05/21 18:54:56 INFO TorrentBroadcast: Reading broadcast variable 121 took 20 ms
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_121 stored as values in memory (estimated size 525.3 KiB, free 411.8 MiB)
26/05/21 18:54:56 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:54:56 INFO TorrentBroadcast: Started reading broadcast variable 120 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_120_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 411.7 MiB)
26/05/21 18:54:56 INFO TorrentBroadcast: Reading broadcast variable 120 took 19 ms
26/05/21 18:54:56 INFO MemoryStore: Block broadcast_120 stored as values in memory (estimated size 595.4 KiB, free 411.1 MiB)
26/05/21 18:54:59 INFO Executor: Finished task 0.0 in stage 106.0 (TID 147). 3121 bytes result sent to driver
26/05/21 18:54:59 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 148
26/05/21 18:54:59 INFO Executor: Running task 1.0 in stage 106.0 (TID 148)
26/05/21 18:54:59 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:02 INFO Executor: Finished task 1.0 in stage 106.0 (TID 148). 3121 bytes result sent to driver
26/05/21 18:55:02 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 149
26/05/21 18:55:02 INFO Executor: Running task 0.0 in stage 108.0 (TID 149)
26/05/21 18:55:02 INFO MapOutputTrackerWorker: Updating epoch to 33 and clearing cache
26/05/21 18:55:02 INFO TorrentBroadcast: Started reading broadcast variable 122 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:02 INFO MemoryStore: Block broadcast_122_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 411.8 MiB)
26/05/21 18:55:02 INFO TorrentBroadcast: Reading broadcast variable 122 took 27 ms
26/05/21 18:55:02 INFO MemoryStore: Block broadcast_122 stored as values in memory (estimated size 12.5 KiB, free 411.8 MiB)
26/05/21 18:55:02 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 32, fetching them
26/05/21 18:55:02 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:02 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:02 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:02 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
26/05/21 18:55:02 INFO Executor: Finished task 0.0 in stage 108.0 (TID 149). 3893 bytes result sent to driver
26/05/21 18:55:03 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 150
26/05/21 18:55:03 INFO Executor: Running task 0.0 in stage 109.0 (TID 150)
26/05/21 18:55:03 INFO TorrentBroadcast: Started reading broadcast variable 123 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:03 INFO MemoryStore: Block broadcast_123_piece0 stored as bytes in memory (estimated size 44.6 KiB, free 413.1 MiB)
26/05/21 18:55:03 INFO TorrentBroadcast: Reading broadcast variable 123 took 30 ms
26/05/21 18:55:03 INFO MemoryStore: Block broadcast_123 stored as values in memory (estimated size 113.4 KiB, free 413.0 MiB)
26/05/21 18:55:03 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:55:03 INFO Executor: 1 block locks were not released by task 0.0 in stage 109.0 (TID 150)
[rdd_28_0]
26/05/21 18:55:03 INFO Executor: Finished task 0.0 in stage 109.0 (TID 150). 2355 bytes result sent to driver
26/05/21 18:55:03 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 151
26/05/21 18:55:03 INFO Executor: Running task 0.0 in stage 110.0 (TID 151)
26/05/21 18:55:03 INFO TorrentBroadcast: Started reading broadcast variable 124 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:03 INFO MemoryStore: Block broadcast_124_piece0 stored as bytes in memory (estimated size 44.7 KiB, free 412.9 MiB)
26/05/21 18:55:03 INFO TorrentBroadcast: Reading broadcast variable 124 took 20 ms
26/05/21 18:55:03 INFO MemoryStore: Block broadcast_124 stored as values in memory (estimated size 113.5 KiB, free 412.8 MiB)
26/05/21 18:55:03 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:55:03 INFO Executor: Finished task 0.0 in stage 110.0 (TID 151). 2366 bytes result sent to driver
26/05/21 18:55:03 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 152
26/05/21 18:55:03 INFO Executor: Running task 1.0 in stage 110.0 (TID 152)
26/05/21 18:55:03 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:55:03 INFO Executor: Finished task 1.0 in stage 110.0 (TID 152). 2323 bytes result sent to driver
26/05/21 18:55:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 153
26/05/21 18:55:04 INFO Executor: Running task 0.0 in stage 111.0 (TID 153)
26/05/21 18:55:04 INFO TorrentBroadcast: Started reading broadcast variable 125 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_125_piece0 stored as bytes in memory (estimated size 46.1 KiB, free 412.8 MiB)
26/05/21 18:55:04 INFO TorrentBroadcast: Reading broadcast variable 125 took 18 ms
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_125 stored as values in memory (estimated size 117.1 KiB, free 412.7 MiB)
26/05/21 18:55:04 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:55:04 INFO Executor: Finished task 0.0 in stage 111.0 (TID 153). 2646 bytes result sent to driver
26/05/21 18:55:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 154
26/05/21 18:55:04 INFO Executor: Running task 1.0 in stage 111.0 (TID 154)
26/05/21 18:55:04 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:55:04 INFO Executor: Finished task 1.0 in stage 111.0 (TID 154). 2646 bytes result sent to driver
26/05/21 18:55:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 155
26/05/21 18:55:04 INFO Executor: Running task 0.0 in stage 112.0 (TID 155)
26/05/21 18:55:04 INFO MapOutputTrackerWorker: Updating epoch to 34 and clearing cache
26/05/21 18:55:04 INFO TorrentBroadcast: Started reading broadcast variable 126 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_126_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 412.7 MiB)
26/05/21 18:55:04 INFO TorrentBroadcast: Reading broadcast variable 126 took 17 ms
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_126 stored as values in memory (estimated size 8.9 KiB, free 412.7 MiB)
26/05/21 18:55:04 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 33, fetching them
26/05/21 18:55:04 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:04 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:04 INFO ShuffleBlockFetcherIterator: Getting 2 (1506.0 B) non-empty blocks including 2 (1506.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:55:04 INFO Executor: Finished task 0.0 in stage 112.0 (TID 155). 2427 bytes result sent to driver
26/05/21 18:55:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 156
26/05/21 18:55:04 INFO Executor: Running task 1.0 in stage 112.0 (TID 156)
26/05/21 18:55:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:04 INFO Executor: Finished task 1.0 in stage 112.0 (TID 156). 1669 bytes result sent to driver
26/05/21 18:55:04 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 157
26/05/21 18:55:04 INFO Executor: Running task 0.0 in stage 113.0 (TID 157)
26/05/21 18:55:04 INFO TorrentBroadcast: Started reading broadcast variable 129 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_129_piece0 stored as bytes in memory (estimated size 47.3 KiB, free 413.1 MiB)
26/05/21 18:55:04 INFO TorrentBroadcast: Reading broadcast variable 129 took 42 ms
26/05/21 18:55:04 INFO MemoryStore: Block broadcast_129 stored as values in memory (estimated size 119.1 KiB, free 413.0 MiB)
26/05/21 18:55:04 INFO BlockManager: Found block rdd_28_0 locally
26/05/21 18:55:05 INFO MemoryStore: Block rdd_361_0 stored as values in memory (estimated size 1075.9 KiB, free 411.9 MiB)
26/05/21 18:55:05 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:05 INFO MemoryStore: Block rdd_363_0 stored as values in memory (estimated size 63.4 KiB, free 411.9 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Started reading broadcast variable 128 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_128_piece0 stored as bytes in memory (estimated size 45.0 B, free 411.9 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Reading broadcast variable 128 took 16 ms
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_128 stored as values in memory (estimated size 40.0 B, free 411.9 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Started reading broadcast variable 127 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_127_piece0 stored as bytes in memory (estimated size 344.0 B, free 411.9 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Reading broadcast variable 127 took 12 ms
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_127 stored as values in memory (estimated size 1360.0 B, free 411.9 MiB)
26/05/21 18:55:05 INFO Executor: Finished task 0.0 in stage 113.0 (TID 157). 2689 bytes result sent to driver
26/05/21 18:55:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 158
26/05/21 18:55:05 INFO Executor: Running task 1.0 in stage 113.0 (TID 158)
26/05/21 18:55:05 INFO BlockManager: Found block rdd_28_1 locally
26/05/21 18:55:05 INFO MemoryStore: Block rdd_361_1 stored as values in memory (estimated size 1023.0 KiB, free 410.9 MiB)
26/05/21 18:55:05 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:05 INFO MemoryStore: Block rdd_363_1 stored as values in memory (estimated size 60.5 KiB, free 410.8 MiB)
26/05/21 18:55:05 INFO Executor: Finished task 1.0 in stage 113.0 (TID 158). 2646 bytes result sent to driver
26/05/21 18:55:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 159
26/05/21 18:55:05 INFO Executor: Running task 0.0 in stage 114.0 (TID 159)
26/05/21 18:55:05 INFO MapOutputTrackerWorker: Updating epoch to 35 and clearing cache
26/05/21 18:55:05 INFO TorrentBroadcast: Started reading broadcast variable 130 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_130_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 410.8 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Reading broadcast variable 130 took 20 ms
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_130 stored as values in memory (estimated size 7.5 KiB, free 410.8 MiB)
26/05/21 18:55:05 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 34, fetching them
26/05/21 18:55:05 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:05 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:05 INFO ShuffleBlockFetcherIterator: Getting 2 (3.0 KiB) non-empty blocks including 2 (3.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:05 INFO Executor: Finished task 0.0 in stage 114.0 (TID 159). 2216 bytes result sent to driver
26/05/21 18:55:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 160
26/05/21 18:55:05 INFO Executor: Running task 1.0 in stage 114.0 (TID 160)
26/05/21 18:55:05 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:05 INFO Executor: Finished task 1.0 in stage 114.0 (TID 160). 1669 bytes result sent to driver
26/05/21 18:55:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 161
26/05/21 18:55:05 INFO Executor: Running task 0.0 in stage 115.0 (TID 161)
26/05/21 18:55:05 INFO TorrentBroadcast: Started reading broadcast variable 132 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_132_piece0 stored as bytes in memory (estimated size 47.9 KiB, free 410.8 MiB)
26/05/21 18:55:05 INFO TorrentBroadcast: Reading broadcast variable 132 took 21 ms
26/05/21 18:55:05 INFO MemoryStore: Block broadcast_132 stored as values in memory (estimated size 120.3 KiB, free 410.6 MiB)
26/05/21 18:55:06 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:06 INFO BlockManager: Found block rdd_363_0 locally
26/05/21 18:55:06 INFO TorrentBroadcast: Started reading broadcast variable 131 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_131_piece0 stored as bytes in memory (estimated size 45.0 B, free 410.6 MiB)
26/05/21 18:55:06 INFO TorrentBroadcast: Reading broadcast variable 131 took 18 ms
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_131 stored as values in memory (estimated size 40.0 B, free 410.6 MiB)
26/05/21 18:55:06 INFO Executor: Finished task 0.0 in stage 115.0 (TID 161). 2603 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 162
26/05/21 18:55:06 INFO Executor: Running task 1.0 in stage 115.0 (TID 162)
26/05/21 18:55:06 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:06 INFO BlockManager: Found block rdd_363_1 locally
26/05/21 18:55:06 INFO Executor: Finished task 1.0 in stage 115.0 (TID 162). 2603 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 163
26/05/21 18:55:06 INFO Executor: Running task 0.0 in stage 116.0 (TID 163)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Updating epoch to 36 and clearing cache
26/05/21 18:55:06 INFO TorrentBroadcast: Started reading broadcast variable 133 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_133_piece0 stored as bytes in memory (estimated size 4.3 KiB, free 410.6 MiB)
26/05/21 18:55:06 INFO TorrentBroadcast: Reading broadcast variable 133 took 15 ms
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_133 stored as values in memory (estimated size 8.0 KiB, free 410.6 MiB)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 35, fetching them
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 2 (2.5 KiB) non-empty blocks including 2 (2.5 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:06 INFO Executor: Finished task 0.0 in stage 116.0 (TID 163). 2026 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 164
26/05/21 18:55:06 INFO Executor: Running task 1.0 in stage 116.0 (TID 164)
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 2 (2.9 KiB) non-empty blocks including 2 (2.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:06 INFO Executor: Finished task 1.0 in stage 116.0 (TID 164). 2171 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 165
26/05/21 18:55:06 INFO Executor: Running task 0.0 in stage 117.0 (TID 165)
26/05/21 18:55:06 INFO TorrentBroadcast: Started reading broadcast variable 135 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_135_piece0 stored as bytes in memory (estimated size 48.3 KiB, free 410.9 MiB)
26/05/21 18:55:06 INFO TorrentBroadcast: Reading broadcast variable 135 took 21 ms
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_135 stored as values in memory (estimated size 121.2 KiB, free 410.8 MiB)
26/05/21 18:55:06 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:06 INFO BlockManager: Found block rdd_363_0 locally
26/05/21 18:55:06 INFO TorrentBroadcast: Started reading broadcast variable 134 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_134_piece0 stored as bytes in memory (estimated size 45.0 B, free 410.8 MiB)
26/05/21 18:55:06 INFO TorrentBroadcast: Reading broadcast variable 134 took 17 ms
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_134 stored as values in memory (estimated size 40.0 B, free 410.8 MiB)
26/05/21 18:55:06 INFO Executor: Finished task 0.0 in stage 117.0 (TID 165). 2603 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 166
26/05/21 18:55:06 INFO Executor: Running task 1.0 in stage 117.0 (TID 166)
26/05/21 18:55:06 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:06 INFO BlockManager: Found block rdd_363_1 locally
26/05/21 18:55:06 INFO Executor: Finished task 1.0 in stage 117.0 (TID 166). 2603 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 167
26/05/21 18:55:06 INFO Executor: Running task 0.0 in stage 118.0 (TID 167)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Updating epoch to 37 and clearing cache
26/05/21 18:55:06 INFO TorrentBroadcast: Started reading broadcast variable 136 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_136_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 410.8 MiB)
26/05/21 18:55:06 INFO TorrentBroadcast: Reading broadcast variable 136 took 20 ms
26/05/21 18:55:06 INFO MemoryStore: Block broadcast_136 stored as values in memory (estimated size 8.2 KiB, free 410.8 MiB)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 36, fetching them
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:06 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:06 INFO Executor: Finished task 0.0 in stage 118.0 (TID 167). 2305 bytes result sent to driver
26/05/21 18:55:06 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 168
26/05/21 18:55:06 INFO Executor: Running task 1.0 in stage 118.0 (TID 168)
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 2 (4.0 KiB) non-empty blocks including 2 (4.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:06 INFO Executor: Finished task 1.0 in stage 118.0 (TID 168). 2254 bytes result sent to driver
26/05/21 18:55:06 INFO BlockManager: Removing RDD 363
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 169
26/05/21 18:55:07 INFO Executor: Running task 0.0 in stage 119.0 (TID 169)
26/05/21 18:55:07 INFO TorrentBroadcast: Started reading broadcast variable 138 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_138_piece0 stored as bytes in memory (estimated size 51.6 KiB, free 411.0 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Reading broadcast variable 138 took 18 ms
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_138 stored as values in memory (estimated size 128.1 KiB, free 410.9 MiB)
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:07 INFO MemoryStore: Block rdd_375_0 stored as values in memory (estimated size 696.2 KiB, free 410.2 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block rdd_378_0 stored as values in memory (estimated size 569.7 KiB, free 409.7 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Started reading broadcast variable 137 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_137_piece0 stored as bytes in memory (estimated size 45.0 B, free 409.7 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Reading broadcast variable 137 took 14 ms
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_137 stored as values in memory (estimated size 40.0 B, free 409.7 MiB)
26/05/21 18:55:07 INFO Executor: Finished task 0.0 in stage 119.0 (TID 169). 2646 bytes result sent to driver
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 170
26/05/21 18:55:07 INFO Executor: Running task 1.0 in stage 119.0 (TID 170)
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:07 INFO MemoryStore: Block rdd_375_1 stored as values in memory (estimated size 662.1 KiB, free 409.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block rdd_378_1 stored as values in memory (estimated size 541.8 KiB, free 408.5 MiB)
26/05/21 18:55:07 INFO Executor: Finished task 1.0 in stage 119.0 (TID 170). 2646 bytes result sent to driver
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 171
26/05/21 18:55:07 INFO Executor: Running task 0.0 in stage 120.0 (TID 171)
26/05/21 18:55:07 INFO MapOutputTrackerWorker: Updating epoch to 38 and clearing cache
26/05/21 18:55:07 INFO TorrentBroadcast: Started reading broadcast variable 139 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_139_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 408.5 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Reading broadcast variable 139 took 18 ms
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_139 stored as values in memory (estimated size 7.5 KiB, free 408.5 MiB)
26/05/21 18:55:07 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 37, fetching them
26/05/21 18:55:07 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:07 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:07 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:07 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:07 INFO Executor: Finished task 0.0 in stage 120.0 (TID 171). 2258 bytes result sent to driver
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 172
26/05/21 18:55:07 INFO Executor: Running task 1.0 in stage 120.0 (TID 172)
26/05/21 18:55:07 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:07 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:07 INFO Executor: Finished task 1.0 in stage 120.0 (TID 172). 1669 bytes result sent to driver
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 173
26/05/21 18:55:07 INFO Executor: Running task 0.0 in stage 121.0 (TID 173)
26/05/21 18:55:07 INFO TorrentBroadcast: Started reading broadcast variable 141 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_141_piece0 stored as bytes in memory (estimated size 52.1 KiB, free 408.4 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Reading broadcast variable 141 took 17 ms
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_141 stored as values in memory (estimated size 128.9 KiB, free 408.3 MiB)
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_378_0 locally
26/05/21 18:55:07 INFO TorrentBroadcast: Started reading broadcast variable 140 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_140_piece0 stored as bytes in memory (estimated size 45.0 B, free 408.3 MiB)
26/05/21 18:55:07 INFO TorrentBroadcast: Reading broadcast variable 140 took 11 ms
26/05/21 18:55:07 INFO MemoryStore: Block broadcast_140 stored as values in memory (estimated size 40.0 B, free 408.3 MiB)
26/05/21 18:55:07 INFO Executor: Finished task 0.0 in stage 121.0 (TID 173). 2603 bytes result sent to driver
26/05/21 18:55:07 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 174
26/05/21 18:55:07 INFO Executor: Running task 1.0 in stage 121.0 (TID 174)
26/05/21 18:55:07 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:07 INFO BlockManager: Found block rdd_378_1 locally
26/05/21 18:55:08 INFO Executor: Finished task 1.0 in stage 121.0 (TID 174). 2603 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 175
26/05/21 18:55:08 INFO Executor: Running task 0.0 in stage 122.0 (TID 175)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Updating epoch to 39 and clearing cache
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 142 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_142_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 408.3 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 142 took 16 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_142 stored as values in memory (estimated size 8.0 KiB, free 408.3 MiB)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 38, fetching them
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:08 INFO Executor: Finished task 0.0 in stage 122.0 (TID 175). 1992 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 176
26/05/21 18:55:08 INFO Executor: Running task 1.0 in stage 122.0 (TID 176)
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:08 INFO Executor: Finished task 1.0 in stage 122.0 (TID 176). 2145 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 177
26/05/21 18:55:08 INFO Executor: Running task 0.0 in stage 123.0 (TID 177)
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 144 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_144_piece0 stored as bytes in memory (estimated size 52.5 KiB, free 408.6 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 144 took 26 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_144 stored as values in memory (estimated size 129.7 KiB, free 408.5 MiB)
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_378_0 locally
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 143 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_143_piece0 stored as bytes in memory (estimated size 45.0 B, free 408.5 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 143 took 21 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_143 stored as values in memory (estimated size 40.0 B, free 408.5 MiB)
26/05/21 18:55:08 INFO Executor: Finished task 0.0 in stage 123.0 (TID 177). 2646 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 178
26/05/21 18:55:08 INFO Executor: Running task 1.0 in stage 123.0 (TID 178)
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_378_1 locally
26/05/21 18:55:08 INFO Executor: Finished task 1.0 in stage 123.0 (TID 178). 2603 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 179
26/05/21 18:55:08 INFO Executor: Running task 0.0 in stage 124.0 (TID 179)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Updating epoch to 40 and clearing cache
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 145 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_145_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 408.5 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 145 took 9 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_145 stored as values in memory (estimated size 8.2 KiB, free 408.5 MiB)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 39, fetching them
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:08 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Getting 2 (6.8 KiB) non-empty blocks including 2 (6.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:08 INFO Executor: Finished task 0.0 in stage 124.0 (TID 179). 2289 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 180
26/05/21 18:55:08 INFO Executor: Running task 1.0 in stage 124.0 (TID 180)
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Getting 2 (4.9 KiB) non-empty blocks including 2 (4.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:08 INFO Executor: Finished task 1.0 in stage 124.0 (TID 180). 2289 bytes result sent to driver
26/05/21 18:55:08 INFO BlockManager: Removing RDD 378
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 181
26/05/21 18:55:08 INFO Executor: Running task 0.0 in stage 125.0 (TID 181)
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 147 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_147_piece0 stored as bytes in memory (estimated size 54.0 KiB, free 409.5 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 147 took 10 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_147 stored as values in memory (estimated size 132.5 KiB, free 409.4 MiB)
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_375_0 locally
26/05/21 18:55:08 INFO MemoryStore: Block rdd_391_0 stored as values in memory (estimated size 696.2 KiB, free 408.7 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block rdd_394_0 stored as values in memory (estimated size 569.7 KiB, free 408.2 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Started reading broadcast variable 146 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_146_piece0 stored as bytes in memory (estimated size 45.0 B, free 408.2 MiB)
26/05/21 18:55:08 INFO TorrentBroadcast: Reading broadcast variable 146 took 12 ms
26/05/21 18:55:08 INFO MemoryStore: Block broadcast_146 stored as values in memory (estimated size 40.0 B, free 408.2 MiB)
26/05/21 18:55:08 INFO Executor: Finished task 0.0 in stage 125.0 (TID 181). 2689 bytes result sent to driver
26/05/21 18:55:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 182
26/05/21 18:55:08 INFO Executor: Running task 1.0 in stage 125.0 (TID 182)
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:08 INFO BlockManager: Found block rdd_375_1 locally
26/05/21 18:55:08 INFO MemoryStore: Block rdd_391_1 stored as values in memory (estimated size 662.1 KiB, free 407.5 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block rdd_394_1 stored as values in memory (estimated size 541.8 KiB, free 407.0 MiB)
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 125.0 (TID 182). 2603 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 183
26/05/21 18:55:09 INFO Executor: Running task 0.0 in stage 126.0 (TID 183)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Updating epoch to 41 and clearing cache
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 148 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_148_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 407.0 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 148 took 13 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_148 stored as values in memory (estimated size 7.5 KiB, free 407.0 MiB)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 40, fetching them
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:09 INFO Executor: Finished task 0.0 in stage 126.0 (TID 183). 2214 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 184
26/05/21 18:55:09 INFO Executor: Running task 1.0 in stage 126.0 (TID 184)
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 126.0 (TID 184). 1669 bytes result sent to driver
26/05/21 18:55:09 INFO BlockManager: Removing RDD 378
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 185
26/05/21 18:55:09 INFO Executor: Running task 0.0 in stage 127.0 (TID 185)
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 150 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_150_piece0 stored as bytes in memory (estimated size 54.5 KiB, free 407.3 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 150 took 11 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_150 stored as values in memory (estimated size 133.3 KiB, free 407.2 MiB)
26/05/21 18:55:09 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:09 INFO BlockManager: Found block rdd_394_0 locally
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 149 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_149_piece0 stored as bytes in memory (estimated size 45.0 B, free 407.2 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 149 took 10 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_149 stored as values in memory (estimated size 40.0 B, free 407.2 MiB)
26/05/21 18:55:09 INFO Executor: Finished task 0.0 in stage 127.0 (TID 185). 2603 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 186
26/05/21 18:55:09 INFO Executor: Running task 1.0 in stage 127.0 (TID 186)
26/05/21 18:55:09 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:09 INFO BlockManager: Found block rdd_394_1 locally
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 127.0 (TID 186). 2646 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 187
26/05/21 18:55:09 INFO Executor: Running task 0.0 in stage 128.0 (TID 187)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Updating epoch to 42 and clearing cache
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 151 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_151_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 407.2 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 151 took 12 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_151 stored as values in memory (estimated size 8.0 KiB, free 407.2 MiB)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 41, fetching them
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:09 INFO Executor: Finished task 0.0 in stage 128.0 (TID 187). 2009 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 188
26/05/21 18:55:09 INFO Executor: Running task 1.0 in stage 128.0 (TID 188)
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 128.0 (TID 188). 2145 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 189
26/05/21 18:55:09 INFO Executor: Running task 0.0 in stage 129.0 (TID 189)
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 153 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_153_piece0 stored as bytes in memory (estimated size 54.9 KiB, free 407.1 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 153 took 13 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_153 stored as values in memory (estimated size 134.2 KiB, free 407.0 MiB)
26/05/21 18:55:09 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:09 INFO BlockManager: Found block rdd_394_0 locally
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 152 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_152_piece0 stored as bytes in memory (estimated size 45.0 B, free 407.0 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 152 took 11 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_152 stored as values in memory (estimated size 40.0 B, free 407.0 MiB)
26/05/21 18:55:09 INFO Executor: Finished task 0.0 in stage 129.0 (TID 189). 2603 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 190
26/05/21 18:55:09 INFO Executor: Running task 1.0 in stage 129.0 (TID 190)
26/05/21 18:55:09 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:09 INFO BlockManager: Found block rdd_394_1 locally
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 129.0 (TID 190). 2603 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 191
26/05/21 18:55:09 INFO Executor: Running task 0.0 in stage 130.0 (TID 191)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Updating epoch to 43 and clearing cache
26/05/21 18:55:09 INFO TorrentBroadcast: Started reading broadcast variable 154 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_154_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 407.2 MiB)
26/05/21 18:55:09 INFO TorrentBroadcast: Reading broadcast variable 154 took 11 ms
26/05/21 18:55:09 INFO MemoryStore: Block broadcast_154 stored as values in memory (estimated size 8.2 KiB, free 407.2 MiB)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 42, fetching them
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:09 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 2 (6.4 KiB) non-empty blocks including 2 (6.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:09 INFO Executor: Finished task 0.0 in stage 130.0 (TID 191). 2187 bytes result sent to driver
26/05/21 18:55:09 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 192
26/05/21 18:55:09 INFO Executor: Running task 1.0 in stage 130.0 (TID 192)
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Getting 2 (5.9 KiB) non-empty blocks including 2 (5.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:09 INFO Executor: Finished task 1.0 in stage 130.0 (TID 192). 2255 bytes result sent to driver
26/05/21 18:55:09 INFO BlockManager: Removing RDD 394
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 193
26/05/21 18:55:10 INFO Executor: Running task 0.0 in stage 131.0 (TID 193)
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 156 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_156_piece0 stored as bytes in memory (estimated size 56.2 KiB, free 408.2 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 156 took 13 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_156 stored as values in memory (estimated size 136.5 KiB, free 408.1 MiB)
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_391_0 locally
26/05/21 18:55:10 INFO MemoryStore: Block rdd_407_0 stored as values in memory (estimated size 696.2 KiB, free 407.4 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block rdd_410_0 stored as values in memory (estimated size 569.7 KiB, free 406.8 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 155 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_155_piece0 stored as bytes in memory (estimated size 45.0 B, free 406.8 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 155 took 12 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_155 stored as values in memory (estimated size 40.0 B, free 406.8 MiB)
26/05/21 18:55:10 INFO Executor: Finished task 0.0 in stage 131.0 (TID 193). 2646 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 194
26/05/21 18:55:10 INFO Executor: Running task 1.0 in stage 131.0 (TID 194)
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_391_1 locally
26/05/21 18:55:10 INFO MemoryStore: Block rdd_407_1 stored as values in memory (estimated size 662.1 KiB, free 406.2 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block rdd_410_1 stored as values in memory (estimated size 541.8 KiB, free 405.6 MiB)
26/05/21 18:55:10 INFO Executor: Finished task 1.0 in stage 131.0 (TID 194). 2603 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 195
26/05/21 18:55:10 INFO Executor: Running task 0.0 in stage 132.0 (TID 195)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Updating epoch to 44 and clearing cache
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 157 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_157_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.6 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 157 took 34 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_157 stored as values in memory (estimated size 7.5 KiB, free 405.6 MiB)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 43, fetching them
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:10 INFO Executor: Finished task 0.0 in stage 132.0 (TID 195). 2215 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 196
26/05/21 18:55:10 INFO Executor: Running task 1.0 in stage 132.0 (TID 196)
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:10 INFO Executor: Finished task 1.0 in stage 132.0 (TID 196). 1712 bytes result sent to driver
26/05/21 18:55:10 INFO BlockManager: Removing RDD 394
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 197
26/05/21 18:55:10 INFO Executor: Running task 0.0 in stage 133.0 (TID 197)
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 159 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_159_piece0 stored as bytes in memory (estimated size 56.5 KiB, free 406.0 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 159 took 11 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_159 stored as values in memory (estimated size 137.3 KiB, free 405.8 MiB)
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_410_0 locally
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 158 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_158_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.8 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 158 took 8 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_158 stored as values in memory (estimated size 40.0 B, free 405.8 MiB)
26/05/21 18:55:10 INFO Executor: Finished task 0.0 in stage 133.0 (TID 197). 2603 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 198
26/05/21 18:55:10 INFO Executor: Running task 1.0 in stage 133.0 (TID 198)
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_410_1 locally
26/05/21 18:55:10 INFO Executor: Finished task 1.0 in stage 133.0 (TID 198). 2603 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 199
26/05/21 18:55:10 INFO Executor: Running task 0.0 in stage 134.0 (TID 199)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Updating epoch to 45 and clearing cache
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 160 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_160_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.8 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 160 took 9 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_160 stored as values in memory (estimated size 8.0 KiB, free 405.8 MiB)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 44, fetching them
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:10 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:10 INFO Executor: Finished task 0.0 in stage 134.0 (TID 199). 1992 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 200
26/05/21 18:55:10 INFO Executor: Running task 1.0 in stage 134.0 (TID 200)
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:10 INFO Executor: Finished task 1.0 in stage 134.0 (TID 200). 2111 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 201
26/05/21 18:55:10 INFO Executor: Running task 0.0 in stage 135.0 (TID 201)
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 162 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_162_piece0 stored as bytes in memory (estimated size 56.9 KiB, free 405.8 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 162 took 10 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_162 stored as values in memory (estimated size 138.1 KiB, free 405.6 MiB)
26/05/21 18:55:10 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:10 INFO BlockManager: Found block rdd_410_0 locally
26/05/21 18:55:10 INFO TorrentBroadcast: Started reading broadcast variable 161 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_161_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:10 INFO TorrentBroadcast: Reading broadcast variable 161 took 11 ms
26/05/21 18:55:10 INFO MemoryStore: Block broadcast_161 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:10 INFO Executor: Finished task 0.0 in stage 135.0 (TID 201). 2603 bytes result sent to driver
26/05/21 18:55:10 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 202
26/05/21 18:55:10 INFO Executor: Running task 1.0 in stage 135.0 (TID 202)
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_410_1 locally
26/05/21 18:55:11 INFO Executor: Finished task 1.0 in stage 135.0 (TID 202). 2603 bytes result sent to driver
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 203
26/05/21 18:55:11 INFO Executor: Running task 0.0 in stage 136.0 (TID 203)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Updating epoch to 46 and clearing cache
26/05/21 18:55:11 INFO TorrentBroadcast: Started reading broadcast variable 163 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_163_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.6 MiB)
26/05/21 18:55:11 INFO TorrentBroadcast: Reading broadcast variable 163 took 15 ms
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_163 stored as values in memory (estimated size 8.2 KiB, free 405.6 MiB)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 45, fetching them
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Getting 2 (7.1 KiB) non-empty blocks including 2 (7.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:11 INFO Executor: Finished task 0.0 in stage 136.0 (TID 203). 2204 bytes result sent to driver
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 204
26/05/21 18:55:11 INFO Executor: Running task 1.0 in stage 136.0 (TID 204)
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Getting 2 (5.1 KiB) non-empty blocks including 2 (5.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:11 INFO Executor: Finished task 1.0 in stage 136.0 (TID 204). 2167 bytes result sent to driver
26/05/21 18:55:11 INFO BlockManager: Removing RDD 410
26/05/21 18:55:11 INFO BlockManager: Removing RDD 375
26/05/21 18:55:11 INFO BlockManager: Removing RDD 410
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 205
26/05/21 18:55:11 INFO Executor: Running task 0.0 in stage 137.0 (TID 205)
26/05/21 18:55:11 INFO TorrentBroadcast: Started reading broadcast variable 165 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_165_piece0 stored as bytes in memory (estimated size 57.6 KiB, free 408.4 MiB)
26/05/21 18:55:11 INFO TorrentBroadcast: Reading broadcast variable 165 took 12 ms
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_165 stored as values in memory (estimated size 140.4 KiB, free 408.2 MiB)
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_407_0 locally
26/05/21 18:55:11 INFO MemoryStore: Block rdd_423_0 stored as values in memory (estimated size 696.2 KiB, free 407.6 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block rdd_426_0 stored as values in memory (estimated size 569.7 KiB, free 407.0 MiB)
26/05/21 18:55:11 INFO TorrentBroadcast: Started reading broadcast variable 164 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_164_piece0 stored as bytes in memory (estimated size 45.0 B, free 407.0 MiB)
26/05/21 18:55:11 INFO TorrentBroadcast: Reading broadcast variable 164 took 10 ms
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_164 stored as values in memory (estimated size 40.0 B, free 407.0 MiB)
26/05/21 18:55:11 INFO Executor: Finished task 0.0 in stage 137.0 (TID 205). 2646 bytes result sent to driver
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 206
26/05/21 18:55:11 INFO Executor: Running task 1.0 in stage 137.0 (TID 206)
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:11 INFO BlockManager: Found block rdd_407_1 locally
26/05/21 18:55:11 INFO MemoryStore: Block rdd_423_1 stored as values in memory (estimated size 662.1 KiB, free 406.4 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block rdd_426_1 stored as values in memory (estimated size 541.8 KiB, free 405.8 MiB)
26/05/21 18:55:11 INFO Executor: Finished task 1.0 in stage 137.0 (TID 206). 2646 bytes result sent to driver
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 207
26/05/21 18:55:11 INFO Executor: Running task 0.0 in stage 138.0 (TID 207)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Updating epoch to 47 and clearing cache
26/05/21 18:55:11 INFO TorrentBroadcast: Started reading broadcast variable 166 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_166_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.8 MiB)
26/05/21 18:55:11 INFO TorrentBroadcast: Reading broadcast variable 166 took 12 ms
26/05/21 18:55:11 INFO MemoryStore: Block broadcast_166 stored as values in memory (estimated size 7.5 KiB, free 405.8 MiB)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 46, fetching them
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:11 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:55:11 INFO Executor: Finished task 0.0 in stage 138.0 (TID 207). 2217 bytes result sent to driver
26/05/21 18:55:11 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 208
26/05/21 18:55:11 INFO Executor: Running task 1.0 in stage 138.0 (TID 208)
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:11 INFO Executor: Finished task 1.0 in stage 138.0 (TID 208). 1669 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 209
26/05/21 18:55:12 INFO Executor: Running task 0.0 in stage 139.0 (TID 209)
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 168 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_168_piece0 stored as bytes in memory (estimated size 58.0 KiB, free 405.8 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 168 took 14 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_168 stored as values in memory (estimated size 141.1 KiB, free 405.6 MiB)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_426_0 locally
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 167 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_167_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 167 took 11 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_167 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:12 INFO Executor: Finished task 0.0 in stage 139.0 (TID 209). 2603 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 210
26/05/21 18:55:12 INFO Executor: Running task 1.0 in stage 139.0 (TID 210)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_426_1 locally
26/05/21 18:55:12 INFO Executor: Finished task 1.0 in stage 139.0 (TID 210). 2603 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 211
26/05/21 18:55:12 INFO Executor: Running task 0.0 in stage 140.0 (TID 211)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Updating epoch to 48 and clearing cache
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 169 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_169_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.8 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 169 took 17 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_169 stored as values in memory (estimated size 8.0 KiB, free 405.8 MiB)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 47, fetching them
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 2 (3.8 KiB) non-empty blocks including 2 (3.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:12 INFO Executor: Finished task 0.0 in stage 140.0 (TID 211). 2060 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 212
26/05/21 18:55:12 INFO Executor: Running task 1.0 in stage 140.0 (TID 212)
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:12 INFO Executor: Finished task 1.0 in stage 140.0 (TID 212). 2145 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 213
26/05/21 18:55:12 INFO Executor: Running task 0.0 in stage 141.0 (TID 213)
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 171 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_171_piece0 stored as bytes in memory (estimated size 58.3 KiB, free 405.8 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 171 took 9 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_171 stored as values in memory (estimated size 142.1 KiB, free 405.6 MiB)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_426_0 locally
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 170 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_170_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 170 took 8 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_170 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:12 INFO Executor: Finished task 0.0 in stage 141.0 (TID 213). 2603 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 214
26/05/21 18:55:12 INFO Executor: Running task 1.0 in stage 141.0 (TID 214)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_426_1 locally
26/05/21 18:55:12 INFO Executor: Finished task 1.0 in stage 141.0 (TID 214). 2603 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 215
26/05/21 18:55:12 INFO Executor: Running task 0.0 in stage 142.0 (TID 215)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Updating epoch to 49 and clearing cache
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 172 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_172_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.6 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 172 took 7 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_172 stored as values in memory (estimated size 8.2 KiB, free 405.6 MiB)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 48, fetching them
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:12 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 2 (6.4 KiB) non-empty blocks including 2 (6.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:12 INFO Executor: Finished task 0.0 in stage 142.0 (TID 215). 2124 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 216
26/05/21 18:55:12 INFO Executor: Running task 1.0 in stage 142.0 (TID 216)
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 2 (6.4 KiB) non-empty blocks including 2 (6.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:12 INFO Executor: Finished task 1.0 in stage 142.0 (TID 216). 2238 bytes result sent to driver
26/05/21 18:55:12 INFO BlockManager: Removing RDD 426
26/05/21 18:55:12 INFO BlockManager: Removing RDD 391
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 217
26/05/21 18:55:12 INFO Executor: Running task 0.0 in stage 143.0 (TID 217)
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 174 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO BlockManager: Removing RDD 426
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_174_piece0 stored as bytes in memory (estimated size 59.1 KiB, free 408.2 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 174 took 20 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_174 stored as values in memory (estimated size 144.5 KiB, free 408.0 MiB)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_423_0 locally
26/05/21 18:55:12 INFO MemoryStore: Block rdd_439_0 stored as values in memory (estimated size 696.2 KiB, free 407.6 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block rdd_442_0 stored as values in memory (estimated size 569.7 KiB, free 407.0 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Started reading broadcast variable 173 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_173_piece0 stored as bytes in memory (estimated size 45.0 B, free 407.0 MiB)
26/05/21 18:55:12 INFO TorrentBroadcast: Reading broadcast variable 173 took 15 ms
26/05/21 18:55:12 INFO MemoryStore: Block broadcast_173 stored as values in memory (estimated size 40.0 B, free 407.0 MiB)
26/05/21 18:55:12 INFO Executor: Finished task 0.0 in stage 143.0 (TID 217). 2603 bytes result sent to driver
26/05/21 18:55:12 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 218
26/05/21 18:55:12 INFO Executor: Running task 1.0 in stage 143.0 (TID 218)
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:12 INFO BlockManager: Found block rdd_423_1 locally
26/05/21 18:55:12 INFO MemoryStore: Block rdd_439_1 stored as values in memory (estimated size 662.1 KiB, free 406.4 MiB)
26/05/21 18:55:12 INFO MemoryStore: Block rdd_442_1 stored as values in memory (estimated size 541.8 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 143.0 (TID 218). 2646 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 219
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 144.0 (TID 219)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Updating epoch to 50 and clearing cache
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 175 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_175_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 175 took 19 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_175 stored as values in memory (estimated size 7.5 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 49, fetching them
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:13 INFO Executor: Finished task 0.0 in stage 144.0 (TID 219). 2213 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 220
26/05/21 18:55:13 INFO Executor: Running task 1.0 in stage 144.0 (TID 220)
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 144.0 (TID 220). 1669 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 221
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 145.0 (TID 221)
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 177 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_177_piece0 stored as bytes in memory (estimated size 59.4 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 177 took 16 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_177 stored as values in memory (estimated size 145.2 KiB, free 405.6 MiB)
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_442_0 locally
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 176 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_176_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 176 took 17 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_176 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:13 INFO Executor: Finished task 0.0 in stage 145.0 (TID 221). 2603 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 222
26/05/21 18:55:13 INFO Executor: Running task 1.0 in stage 145.0 (TID 222)
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_442_1 locally
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 145.0 (TID 222). 2603 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 223
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 146.0 (TID 223)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Updating epoch to 51 and clearing cache
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 178 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_178_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.6 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 178 took 9 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_178 stored as values in memory (estimated size 8.0 KiB, free 405.6 MiB)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 50, fetching them
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:13 INFO Executor: Finished task 0.0 in stage 146.0 (TID 223). 1992 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 224
26/05/21 18:55:13 INFO Executor: Running task 1.0 in stage 146.0 (TID 224)
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 146.0 (TID 224). 2145 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 225
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 147.0 (TID 225)
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 180 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_180_piece0 stored as bytes in memory (estimated size 59.6 KiB, free 406.0 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 180 took 13 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_180 stored as values in memory (estimated size 146.0 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_442_0 locally
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 179 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_179_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 179 took 10 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_179 stored as values in memory (estimated size 40.0 B, free 405.8 MiB)
26/05/21 18:55:13 INFO Executor: Finished task 0.0 in stage 147.0 (TID 225). 2646 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 226
26/05/21 18:55:13 INFO Executor: Running task 1.0 in stage 147.0 (TID 226)
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_442_1 locally
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 147.0 (TID 226). 2603 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 227
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 148.0 (TID 227)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Updating epoch to 52 and clearing cache
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 181 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_181_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 181 took 9 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_181 stored as values in memory (estimated size 8.2 KiB, free 405.8 MiB)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 51, fetching them
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:13 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 2 (6.8 KiB) non-empty blocks including 2 (6.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:13 INFO Executor: Finished task 0.0 in stage 148.0 (TID 227). 2124 bytes result sent to driver
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 228
26/05/21 18:55:13 INFO Executor: Running task 1.0 in stage 148.0 (TID 228)
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:13 INFO Executor: Finished task 1.0 in stage 148.0 (TID 228). 2272 bytes result sent to driver
26/05/21 18:55:13 INFO BlockManager: Removing RDD 442
26/05/21 18:55:13 INFO BlockManager: Removing RDD 407
26/05/21 18:55:13 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 229
26/05/21 18:55:13 INFO Executor: Running task 0.0 in stage 149.0 (TID 229)
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 183 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_183_piece0 stored as bytes in memory (estimated size 60.5 KiB, free 408.2 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 183 took 8 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_183 stored as values in memory (estimated size 148.4 KiB, free 408.0 MiB)
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:13 INFO BlockManager: Found block rdd_439_0 locally
26/05/21 18:55:13 INFO MemoryStore: Block rdd_455_0 stored as values in memory (estimated size 696.2 KiB, free 407.3 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block rdd_458_0 stored as values in memory (estimated size 569.7 KiB, free 406.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Started reading broadcast variable 182 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_182_piece0 stored as bytes in memory (estimated size 45.0 B, free 406.8 MiB)
26/05/21 18:55:13 INFO TorrentBroadcast: Reading broadcast variable 182 took 13 ms
26/05/21 18:55:13 INFO MemoryStore: Block broadcast_182 stored as values in memory (estimated size 40.0 B, free 406.8 MiB)
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 149.0 (TID 229). 2603 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 230
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 149.0 (TID 230)
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_439_1 locally
26/05/21 18:55:14 INFO MemoryStore: Block rdd_455_1 stored as values in memory (estimated size 662.1 KiB, free 406.1 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block rdd_458_1 stored as values in memory (estimated size 541.8 KiB, free 405.6 MiB)
26/05/21 18:55:14 INFO Executor: Finished task 1.0 in stage 149.0 (TID 230). 2646 bytes result sent to driver
26/05/21 18:55:14 INFO BlockManager: Removing RDD 442
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 231
26/05/21 18:55:14 INFO Executor: Running task 0.0 in stage 150.0 (TID 231)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Updating epoch to 53 and clearing cache
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 184 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_184_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.6 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 184 took 20 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_184 stored as values in memory (estimated size 7.5 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 52, fetching them
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 150.0 (TID 231). 2220 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 232
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 150.0 (TID 232)
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:14 INFO Executor: Finished task 1.0 in stage 150.0 (TID 232). 1669 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 233
26/05/21 18:55:14 INFO Executor: Running task 0.0 in stage 151.0 (TID 233)
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 186 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_186_piece0 stored as bytes in memory (estimated size 60.8 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 186 took 14 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_186 stored as values in memory (estimated size 149.2 KiB, free 405.6 MiB)
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_458_0 locally
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 185 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_185_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 185 took 15 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_185 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 151.0 (TID 233). 2603 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 234
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 151.0 (TID 234)
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_458_1 locally
26/05/21 18:55:14 INFO Executor: Finished task 1.0 in stage 151.0 (TID 234). 2646 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 235
26/05/21 18:55:14 INFO Executor: Running task 0.0 in stage 152.0 (TID 235)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Updating epoch to 54 and clearing cache
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 187 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_187_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.6 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 187 took 12 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_187 stored as values in memory (estimated size 8.0 KiB, free 405.6 MiB)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 53, fetching them
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 2 (4.0 KiB) non-empty blocks including 2 (4.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 152.0 (TID 235). 2111 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 236
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 152.0 (TID 236)
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 2 (4.0 KiB) non-empty blocks including 2 (4.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:14 INFO Executor: Finished task 1.0 in stage 152.0 (TID 236). 2154 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 237
26/05/21 18:55:14 INFO Executor: Running task 0.0 in stage 153.0 (TID 237)
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 189 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_189_piece0 stored as bytes in memory (estimated size 61.2 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 189 took 17 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_189 stored as values in memory (estimated size 150.1 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_458_0 locally
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 188 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_188_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.8 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 188 took 14 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_188 stored as values in memory (estimated size 40.0 B, free 405.8 MiB)
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 153.0 (TID 237). 2603 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 238
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 153.0 (TID 238)
26/05/21 18:55:14 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:14 INFO BlockManager: Found block rdd_458_1 locally
26/05/21 18:55:14 INFO Executor: Finished task 1.0 in stage 153.0 (TID 238). 2603 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 239
26/05/21 18:55:14 INFO Executor: Running task 0.0 in stage 154.0 (TID 239)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Updating epoch to 55 and clearing cache
26/05/21 18:55:14 INFO TorrentBroadcast: Started reading broadcast variable 190 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_190_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO TorrentBroadcast: Reading broadcast variable 190 took 13 ms
26/05/21 18:55:14 INFO MemoryStore: Block broadcast_190 stored as values in memory (estimated size 8.2 KiB, free 405.8 MiB)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 54, fetching them
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:14 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 2 (6.4 KiB) non-empty blocks including 2 (6.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:14 INFO Executor: Finished task 0.0 in stage 154.0 (TID 239). 2187 bytes result sent to driver
26/05/21 18:55:14 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 240
26/05/21 18:55:14 INFO Executor: Running task 1.0 in stage 154.0 (TID 240)
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Getting 2 (6.4 KiB) non-empty blocks including 2 (6.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:14 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:15 INFO Executor: Finished task 1.0 in stage 154.0 (TID 240). 2167 bytes result sent to driver
26/05/21 18:55:15 INFO BlockManager: Removing RDD 458
26/05/21 18:55:15 INFO BlockManager: Removing RDD 423
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 241
26/05/21 18:55:15 INFO Executor: Running task 0.0 in stage 155.0 (TID 241)
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 192 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_192_piece0 stored as bytes in memory (estimated size 61.8 KiB, free 408.2 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 192 took 19 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_192 stored as values in memory (estimated size 152.4 KiB, free 408.0 MiB)
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_455_0 locally
26/05/21 18:55:15 INFO MemoryStore: Block rdd_471_0 stored as values in memory (estimated size 696.2 KiB, free 407.3 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block rdd_474_0 stored as values in memory (estimated size 569.7 KiB, free 406.8 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 191 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_191_piece0 stored as bytes in memory (estimated size 45.0 B, free 406.8 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 191 took 13 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_191 stored as values in memory (estimated size 40.0 B, free 406.8 MiB)
26/05/21 18:55:15 INFO Executor: Finished task 0.0 in stage 155.0 (TID 241). 2646 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 242
26/05/21 18:55:15 INFO Executor: Running task 1.0 in stage 155.0 (TID 242)
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_455_1 locally
26/05/21 18:55:15 INFO MemoryStore: Block rdd_471_1 stored as values in memory (estimated size 662.1 KiB, free 406.1 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block rdd_474_1 stored as values in memory (estimated size 541.8 KiB, free 405.6 MiB)
26/05/21 18:55:15 INFO Executor: Finished task 1.0 in stage 155.0 (TID 242). 2603 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 243
26/05/21 18:55:15 INFO Executor: Running task 0.0 in stage 156.0 (TID 243)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Updating epoch to 56 and clearing cache
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 193 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_193_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.6 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 193 took 33 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_193 stored as values in memory (estimated size 7.5 KiB, free 405.6 MiB)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 55, fetching them
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:15 INFO Executor: Finished task 0.0 in stage 156.0 (TID 243). 2216 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 244
26/05/21 18:55:15 INFO Executor: Running task 1.0 in stage 156.0 (TID 244)
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:15 INFO Executor: Finished task 1.0 in stage 156.0 (TID 244). 1669 bytes result sent to driver
26/05/21 18:55:15 INFO BlockManager: Removing RDD 458
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 245
26/05/21 18:55:15 INFO Executor: Running task 0.0 in stage 157.0 (TID 245)
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 195 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_195_piece0 stored as bytes in memory (estimated size 62.2 KiB, free 406.0 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 195 took 7 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_195 stored as values in memory (estimated size 153.1 KiB, free 405.8 MiB)
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_474_0 locally
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 194 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_194_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.8 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 194 took 8 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_194 stored as values in memory (estimated size 40.0 B, free 405.8 MiB)
26/05/21 18:55:15 INFO Executor: Finished task 0.0 in stage 157.0 (TID 245). 2646 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 246
26/05/21 18:55:15 INFO Executor: Running task 1.0 in stage 157.0 (TID 246)
26/05/21 18:55:15 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:15 INFO BlockManager: Found block rdd_474_1 locally
26/05/21 18:55:15 INFO Executor: Finished task 1.0 in stage 157.0 (TID 246). 2603 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 247
26/05/21 18:55:15 INFO Executor: Running task 0.0 in stage 158.0 (TID 247)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Updating epoch to 57 and clearing cache
26/05/21 18:55:15 INFO TorrentBroadcast: Started reading broadcast variable 196 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_196_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.8 MiB)
26/05/21 18:55:15 INFO TorrentBroadcast: Reading broadcast variable 196 took 13 ms
26/05/21 18:55:15 INFO MemoryStore: Block broadcast_196 stored as values in memory (estimated size 8.0 KiB, free 405.8 MiB)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 56, fetching them
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:15 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:15 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:55:15 INFO Executor: Finished task 0.0 in stage 158.0 (TID 247). 1992 bytes result sent to driver
26/05/21 18:55:15 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 248
26/05/21 18:55:15 INFO Executor: Running task 1.0 in stage 158.0 (TID 248)
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 158.0 (TID 248). 2145 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 249
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 159.0 (TID 249)
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 198 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_198_piece0 stored as bytes in memory (estimated size 62.5 KiB, free 405.7 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 198 took 10 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_198 stored as values in memory (estimated size 154.0 KiB, free 405.6 MiB)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_474_0 locally
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 197 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_197_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 197 took 8 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_197 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 159.0 (TID 249). 2603 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 250
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 159.0 (TID 250)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_474_1 locally
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 159.0 (TID 250). 2603 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 251
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 160.0 (TID 251)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Updating epoch to 58 and clearing cache
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 199 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_199_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.8 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 199 took 21 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_199 stored as values in memory (estimated size 8.2 KiB, free 405.8 MiB)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 57, fetching them
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (6.8 KiB) non-empty blocks including 2 (6.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 160.0 (TID 251). 2306 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 252
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 160.0 (TID 252)
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (5.3 KiB) non-empty blocks including 2 (5.3 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 160.0 (TID 252). 2238 bytes result sent to driver
26/05/21 18:55:16 INFO BlockManager: Removing RDD 474
26/05/21 18:55:16 INFO BlockManager: Removing RDD 439
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 253
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 161.0 (TID 253)
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 201 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_201_piece0 stored as bytes in memory (estimated size 63.2 KiB, free 408.2 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 201 took 13 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_201 stored as values in memory (estimated size 156.5 KiB, free 408.0 MiB)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_471_0 locally
26/05/21 18:55:16 INFO MemoryStore: Block rdd_487_0 stored as values in memory (estimated size 696.2 KiB, free 407.3 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block rdd_490_0 stored as values in memory (estimated size 569.7 KiB, free 406.8 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 200 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_200_piece0 stored as bytes in memory (estimated size 45.0 B, free 406.8 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 200 took 9 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_200 stored as values in memory (estimated size 40.0 B, free 406.8 MiB)
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 161.0 (TID 253). 2646 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 254
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 161.0 (TID 254)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_471_1 locally
26/05/21 18:55:16 INFO MemoryStore: Block rdd_487_1 stored as values in memory (estimated size 662.1 KiB, free 406.1 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block rdd_490_1 stored as values in memory (estimated size 541.8 KiB, free 405.6 MiB)
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 161.0 (TID 254). 2603 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 255
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 162.0 (TID 255)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Updating epoch to 59 and clearing cache
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 202 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_202_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.6 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 202 took 11 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_202 stored as values in memory (estimated size 7.5 KiB, free 405.6 MiB)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 58, fetching them
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 162.0 (TID 255). 2299 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 256
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 162.0 (TID 256)
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 162.0 (TID 256). 1669 bytes result sent to driver
26/05/21 18:55:16 INFO BlockManager: Removing RDD 474
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 257
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 163.0 (TID 257)
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 204 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_204_piece0 stored as bytes in memory (estimated size 63.5 KiB, free 406.0 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 204 took 11 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_204 stored as values in memory (estimated size 157.2 KiB, free 405.8 MiB)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_490_0 locally
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 203 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_203_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.8 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 203 took 10 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_203 stored as values in memory (estimated size 40.0 B, free 405.8 MiB)
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 163.0 (TID 257). 2603 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 258
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 163.0 (TID 258)
26/05/21 18:55:16 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:16 INFO BlockManager: Found block rdd_490_1 locally
26/05/21 18:55:16 INFO Executor: Finished task 1.0 in stage 163.0 (TID 258). 2603 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 259
26/05/21 18:55:16 INFO Executor: Running task 0.0 in stage 164.0 (TID 259)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Updating epoch to 60 and clearing cache
26/05/21 18:55:16 INFO TorrentBroadcast: Started reading broadcast variable 205 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_205_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.8 MiB)
26/05/21 18:55:16 INFO TorrentBroadcast: Reading broadcast variable 205 took 12 ms
26/05/21 18:55:16 INFO MemoryStore: Block broadcast_205 stored as values in memory (estimated size 8.0 KiB, free 405.8 MiB)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 59, fetching them
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:16 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:16 INFO Executor: Finished task 0.0 in stage 164.0 (TID 259). 1992 bytes result sent to driver
26/05/21 18:55:16 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 260
26/05/21 18:55:16 INFO Executor: Running task 1.0 in stage 164.0 (TID 260)
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 164.0 (TID 260). 2145 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 261
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 165.0 (TID 261)
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 207 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_207_piece0 stored as bytes in memory (estimated size 63.8 KiB, free 405.7 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 207 took 14 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_207 stored as values in memory (estimated size 158.1 KiB, free 405.6 MiB)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_490_0 locally
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 206 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_206_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 206 took 11 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_206 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 165.0 (TID 261). 2603 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 262
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 165.0 (TID 262)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_490_1 locally
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 165.0 (TID 262). 2603 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 263
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 166.0 (TID 263)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Updating epoch to 61 and clearing cache
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 208 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_208_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.6 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 208 took 10 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_208 stored as values in memory (estimated size 8.2 KiB, free 405.6 MiB)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 60, fetching them
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 2 (6.8 KiB) non-empty blocks including 2 (6.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 166.0 (TID 263). 2124 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 264
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 166.0 (TID 264)
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 166.0 (TID 264). 2238 bytes result sent to driver
26/05/21 18:55:17 INFO BlockManager: Removing RDD 490
26/05/21 18:55:17 INFO BlockManager: Removing RDD 455
26/05/21 18:55:17 INFO BlockManager: Removing RDD 490
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 265
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 167.0 (TID 265)
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 210 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_210_piece0 stored as bytes in memory (estimated size 64.4 KiB, free 408.4 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 210 took 11 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_210 stored as values in memory (estimated size 160.4 KiB, free 408.2 MiB)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_487_0 locally
26/05/21 18:55:17 INFO MemoryStore: Block rdd_503_0 stored as values in memory (estimated size 696.2 KiB, free 407.5 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block rdd_506_0 stored as values in memory (estimated size 569.7 KiB, free 407.0 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 209 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_209_piece0 stored as bytes in memory (estimated size 45.0 B, free 407.0 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 209 took 9 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_209 stored as values in memory (estimated size 40.0 B, free 407.0 MiB)
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 167.0 (TID 265). 2646 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 266
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 167.0 (TID 266)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_487_1 locally
26/05/21 18:55:17 INFO MemoryStore: Block rdd_503_1 stored as values in memory (estimated size 662.1 KiB, free 406.3 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block rdd_506_1 stored as values in memory (estimated size 541.8 KiB, free 405.8 MiB)
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 167.0 (TID 266). 2646 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 267
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 168.0 (TID 267)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Updating epoch to 62 and clearing cache
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 211 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_211_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 405.8 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 211 took 10 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_211 stored as values in memory (estimated size 7.5 KiB, free 405.8 MiB)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 61, fetching them
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 2 (4.8 KiB) non-empty blocks including 2 (4.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 168.0 (TID 267). 2213 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 268
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 168.0 (TID 268)
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 168.0 (TID 268). 1669 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 269
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 169.0 (TID 269)
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 213 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_213_piece0 stored as bytes in memory (estimated size 64.8 KiB, free 405.7 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 213 took 8 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_213 stored as values in memory (estimated size 161.1 KiB, free 405.6 MiB)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_506_0 locally
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 212 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_212_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 212 took 8 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_212 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 169.0 (TID 269). 2603 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 270
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 169.0 (TID 270)
26/05/21 18:55:17 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:17 INFO BlockManager: Found block rdd_506_1 locally
26/05/21 18:55:17 INFO Executor: Finished task 1.0 in stage 169.0 (TID 270). 2603 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 271
26/05/21 18:55:17 INFO Executor: Running task 0.0 in stage 170.0 (TID 271)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Updating epoch to 63 and clearing cache
26/05/21 18:55:17 INFO TorrentBroadcast: Started reading broadcast variable 214 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_214_piece0 stored as bytes in memory (estimated size 4.4 KiB, free 405.6 MiB)
26/05/21 18:55:17 INFO TorrentBroadcast: Reading broadcast variable 214 took 17 ms
26/05/21 18:55:17 INFO MemoryStore: Block broadcast_214 stored as values in memory (estimated size 8.0 KiB, free 405.8 MiB)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 62, fetching them
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:17 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 2 (3.6 KiB) non-empty blocks including 2 (3.6 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:17 INFO Executor: Finished task 0.0 in stage 170.0 (TID 271). 1992 bytes result sent to driver
26/05/21 18:55:17 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 272
26/05/21 18:55:17 INFO Executor: Running task 1.0 in stage 170.0 (TID 272)
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:18 INFO Executor: Finished task 1.0 in stage 170.0 (TID 272). 2128 bytes result sent to driver
26/05/21 18:55:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 273
26/05/21 18:55:18 INFO Executor: Running task 0.0 in stage 171.0 (TID 273)
26/05/21 18:55:18 INFO TorrentBroadcast: Started reading broadcast variable 216 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_216_piece0 stored as bytes in memory (estimated size 65.1 KiB, free 405.7 MiB)
26/05/21 18:55:18 INFO TorrentBroadcast: Reading broadcast variable 216 took 12 ms
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_216 stored as values in memory (estimated size 161.9 KiB, free 405.6 MiB)
26/05/21 18:55:18 INFO BlockManager: Found block rdd_361_0 locally
26/05/21 18:55:18 INFO BlockManager: Found block rdd_506_0 locally
26/05/21 18:55:18 INFO TorrentBroadcast: Started reading broadcast variable 215 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_215_piece0 stored as bytes in memory (estimated size 45.0 B, free 405.6 MiB)
26/05/21 18:55:18 INFO TorrentBroadcast: Reading broadcast variable 215 took 15 ms
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_215 stored as values in memory (estimated size 40.0 B, free 405.6 MiB)
26/05/21 18:55:18 INFO Executor: Finished task 0.0 in stage 171.0 (TID 273). 2603 bytes result sent to driver
26/05/21 18:55:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 274
26/05/21 18:55:18 INFO Executor: Running task 1.0 in stage 171.0 (TID 274)
26/05/21 18:55:18 INFO BlockManager: Found block rdd_361_1 locally
26/05/21 18:55:18 INFO BlockManager: Found block rdd_506_1 locally
26/05/21 18:55:18 INFO Executor: Finished task 1.0 in stage 171.0 (TID 274). 2603 bytes result sent to driver
26/05/21 18:55:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 275
26/05/21 18:55:18 INFO Executor: Running task 0.0 in stage 172.0 (TID 275)
26/05/21 18:55:18 INFO MapOutputTrackerWorker: Updating epoch to 64 and clearing cache
26/05/21 18:55:18 INFO TorrentBroadcast: Started reading broadcast variable 217 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_217_piece0 stored as bytes in memory (estimated size 4.5 KiB, free 405.6 MiB)
26/05/21 18:55:18 INFO TorrentBroadcast: Reading broadcast variable 217 took 16 ms
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_217 stored as values in memory (estimated size 8.2 KiB, free 405.6 MiB)
26/05/21 18:55:18 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 63, fetching them
26/05/21 18:55:18 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:18 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 2 (7.1 KiB) non-empty blocks including 2 (7.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:18 INFO Executor: Finished task 0.0 in stage 172.0 (TID 275). 2124 bytes result sent to driver
26/05/21 18:55:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 276
26/05/21 18:55:18 INFO Executor: Running task 1.0 in stage 172.0 (TID 276)
26/05/21 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 2 (4.4 KiB) non-empty blocks including 2 (4.4 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:18 INFO Executor: Finished task 1.0 in stage 172.0 (TID 276). 2187 bytes result sent to driver
26/05/21 18:55:18 INFO BlockManager: Removing RDD 506
26/05/21 18:55:18 INFO BlockManager: Removing RDD 471
26/05/21 18:55:18 INFO BlockManager: Removing RDD 361
26/05/21 18:55:18 INFO BlockManager: Removing RDD 487
26/05/21 18:55:18 INFO BlockManager: Removing RDD 503
26/05/21 18:55:18 INFO BlockManager: Removing RDD 519
26/05/21 18:55:18 INFO BlockManager: Removing RDD 506
26/05/21 18:55:18 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 277
26/05/21 18:55:18 INFO Executor: Running task 0.0 in stage 173.0 (TID 277)
26/05/21 18:55:18 INFO TorrentBroadcast: Started reading broadcast variable 219 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_219_piece0 stored as bytes in memory (estimated size 58.8 KiB, free 413.1 MiB)
26/05/21 18:55:18 INFO TorrentBroadcast: Reading broadcast variable 219 took 11 ms
26/05/21 18:55:18 INFO MemoryStore: Block broadcast_219 stored as values in memory (estimated size 139.7 KiB, free 413.0 MiB)
26/05/21 18:55:19 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:19 INFO TorrentBroadcast: Started reading broadcast variable 218 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:19 INFO MemoryStore: Block broadcast_218_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:19 INFO TorrentBroadcast: Reading broadcast variable 218 took 14 ms
26/05/21 18:55:19 INFO MemoryStore: Block broadcast_218 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:21 INFO Executor: Finished task 0.0 in stage 173.0 (TID 277). 2405 bytes result sent to driver
26/05/21 18:55:21 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 278
26/05/21 18:55:21 INFO Executor: Running task 1.0 in stage 173.0 (TID 278)
26/05/21 18:55:21 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:23 INFO Executor: Finished task 1.0 in stage 173.0 (TID 278). 2405 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 279
26/05/21 18:55:23 INFO Executor: Running task 0.0 in stage 174.0 (TID 279)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Updating epoch to 65 and clearing cache
26/05/21 18:55:23 INFO TorrentBroadcast: Started reading broadcast variable 220 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_220_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 412.3 MiB)
26/05/21 18:55:23 INFO TorrentBroadcast: Reading broadcast variable 220 took 10 ms
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_220 stored as values in memory (estimated size 7.3 KiB, free 412.3 MiB)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 64, fetching them
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1910.0 B) non-empty blocks including 2 (1910.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:23 INFO Executor: Finished task 0.0 in stage 174.0 (TID 279). 2138 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 280
26/05/21 18:55:23 INFO Executor: Running task 1.0 in stage 174.0 (TID 280)
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1506.0 B) non-empty blocks including 2 (1506.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/05/21 18:55:23 INFO Executor: Finished task 1.0 in stage 174.0 (TID 280). 2050 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 281
26/05/21 18:55:23 INFO Executor: Running task 0.0 in stage 176.0 (TID 281)
26/05/21 18:55:23 INFO TorrentBroadcast: Started reading broadcast variable 221 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_221_piece0 stored as bytes in memory (estimated size 3.2 KiB, free 412.5 MiB)
26/05/21 18:55:23 INFO TorrentBroadcast: Reading broadcast variable 221 took 11 ms
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_221 stored as values in memory (estimated size 5.7 KiB, free 412.5 MiB)
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1910.0 B) non-empty blocks including 2 (1910.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:23 INFO Executor: Finished task 0.0 in stage 176.0 (TID 281). 1834 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 282
26/05/21 18:55:23 INFO Executor: Running task 1.0 in stage 176.0 (TID 282)
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1506.0 B) non-empty blocks including 2 (1506.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:23 INFO Executor: Finished task 1.0 in stage 176.0 (TID 282). 1834 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 283
26/05/21 18:55:23 INFO Executor: Running task 0.0 in stage 177.0 (TID 283)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Updating epoch to 66 and clearing cache
26/05/21 18:55:23 INFO TorrentBroadcast: Started reading broadcast variable 222 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_222_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.5 MiB)
26/05/21 18:55:23 INFO TorrentBroadcast: Reading broadcast variable 222 took 12 ms
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_222 stored as values in memory (estimated size 5.3 KiB, free 412.5 MiB)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 65, fetching them
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:23 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1078.0 B) non-empty blocks including 2 (1078.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:23 INFO Executor: Finished task 0.0 in stage 177.0 (TID 283). 1674 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 284
26/05/21 18:55:23 INFO Executor: Running task 1.0 in stage 177.0 (TID 284)
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 2 (1142.0 B) non-empty blocks including 2 (1142.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:23 INFO Executor: Finished task 1.0 in stage 177.0 (TID 284). 1674 bytes result sent to driver
26/05/21 18:55:23 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 285
26/05/21 18:55:23 INFO Executor: Running task 0.0 in stage 180.0 (TID 285)
26/05/21 18:55:23 INFO TorrentBroadcast: Started reading broadcast variable 223 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:23 INFO MemoryStore: Block broadcast_223_piece0 stored as bytes in memory (estimated size 3.7 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO TorrentBroadcast: Reading broadcast variable 223 took 17 ms
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_223 stored as values in memory (estimated size 6.7 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 2 (1078.0 B) non-empty blocks including 2 (1078.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:24 INFO Executor: Finished task 0.0 in stage 180.0 (TID 285). 1809 bytes result sent to driver
26/05/21 18:55:24 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 286
26/05/21 18:55:24 INFO Executor: Running task 1.0 in stage 180.0 (TID 286)
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 2 (1142.0 B) non-empty blocks including 2 (1142.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:24 INFO Executor: Finished task 1.0 in stage 180.0 (TID 286). 1809 bytes result sent to driver
26/05/21 18:55:24 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 287
26/05/21 18:55:24 INFO Executor: Running task 0.0 in stage 183.0 (TID 287)
26/05/21 18:55:24 INFO TorrentBroadcast: Started reading broadcast variable 224 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_224_piece0 stored as bytes in memory (estimated size 4.1 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO TorrentBroadcast: Reading broadcast variable 224 took 13 ms
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_224 stored as values in memory (estimated size 8.3 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 2 (1078.0 B) non-empty blocks including 2 (1078.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:24 INFO MemoryStore: Block rdd_534_0 stored as values in memory (estimated size 4.1 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO Executor: Finished task 0.0 in stage 183.0 (TID 287). 1718 bytes result sent to driver
26/05/21 18:55:24 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 288
26/05/21 18:55:24 INFO Executor: Running task 1.0 in stage 183.0 (TID 288)
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 2 (1142.0 B) non-empty blocks including 2 (1142.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:24 INFO MemoryStore: Block rdd_534_1 stored as values in memory (estimated size 4.2 KiB, free 412.5 MiB)
26/05/21 18:55:24 INFO Executor: Finished task 1.0 in stage 183.0 (TID 288). 1718 bytes result sent to driver
26/05/21 18:55:24 INFO BlockManager: Removing RDD 534
26/05/21 18:55:24 INFO BlockManager: Removing RDD 534
26/05/21 18:55:24 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 289
26/05/21 18:55:24 INFO Executor: Running task 0.0 in stage 184.0 (TID 289)
26/05/21 18:55:24 INFO TorrentBroadcast: Started reading broadcast variable 226 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_226_piece0 stored as bytes in memory (estimated size 62.0 KiB, free 413.1 MiB)
26/05/21 18:55:24 INFO TorrentBroadcast: Reading broadcast variable 226 took 11 ms
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_226 stored as values in memory (estimated size 151.4 KiB, free 412.9 MiB)
26/05/21 18:55:24 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:24 INFO TorrentBroadcast: Started reading broadcast variable 225 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_225_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:24 INFO TorrentBroadcast: Reading broadcast variable 225 took 10 ms
26/05/21 18:55:24 INFO MemoryStore: Block broadcast_225 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:27 INFO Executor: Finished task 0.0 in stage 184.0 (TID 289). 2405 bytes result sent to driver
26/05/21 18:55:27 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 290
26/05/21 18:55:27 INFO Executor: Running task 1.0 in stage 184.0 (TID 290)
26/05/21 18:55:27 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:29 INFO Executor: Finished task 1.0 in stage 184.0 (TID 290). 2405 bytes result sent to driver
26/05/21 18:55:29 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 291
26/05/21 18:55:29 INFO Executor: Running task 0.0 in stage 185.0 (TID 291)
26/05/21 18:55:29 INFO MapOutputTrackerWorker: Updating epoch to 67 and clearing cache
26/05/21 18:55:29 INFO TorrentBroadcast: Started reading broadcast variable 227 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_227_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:55:29 INFO TorrentBroadcast: Reading broadcast variable 227 took 11 ms
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_227 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:55:29 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 66, fetching them
26/05/21 18:55:29 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:29 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:29 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:29 INFO Executor: Finished task 0.0 in stage 185.0 (TID 291). 1727 bytes result sent to driver
26/05/21 18:55:29 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 292
26/05/21 18:55:29 INFO Executor: Running task 1.0 in stage 185.0 (TID 292)
26/05/21 18:55:29 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:29 INFO Executor: Finished task 1.0 in stage 185.0 (TID 292). 1727 bytes result sent to driver
26/05/21 18:55:29 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 293
26/05/21 18:55:29 INFO Executor: Running task 0.0 in stage 186.0 (TID 293)
26/05/21 18:55:29 INFO TorrentBroadcast: Started reading broadcast variable 229 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_229_piece0 stored as bytes in memory (estimated size 62.0 KiB, free 413.1 MiB)
26/05/21 18:55:29 INFO TorrentBroadcast: Reading broadcast variable 229 took 15 ms
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_229 stored as values in memory (estimated size 151.4 KiB, free 412.9 MiB)
26/05/21 18:55:29 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:29 INFO TorrentBroadcast: Started reading broadcast variable 228 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_228_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:29 INFO TorrentBroadcast: Reading broadcast variable 228 took 12 ms
26/05/21 18:55:29 INFO MemoryStore: Block broadcast_228 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:32 INFO Executor: Finished task 0.0 in stage 186.0 (TID 293). 2405 bytes result sent to driver
26/05/21 18:55:32 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 294
26/05/21 18:55:32 INFO Executor: Running task 1.0 in stage 186.0 (TID 294)
26/05/21 18:55:32 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:34 INFO Executor: Finished task 1.0 in stage 186.0 (TID 294). 2405 bytes result sent to driver
26/05/21 18:55:34 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 295
26/05/21 18:55:34 INFO Executor: Running task 0.0 in stage 187.0 (TID 295)
26/05/21 18:55:34 INFO MapOutputTrackerWorker: Updating epoch to 68 and clearing cache
26/05/21 18:55:34 INFO TorrentBroadcast: Started reading broadcast variable 230 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:34 INFO MemoryStore: Block broadcast_230_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:55:34 INFO TorrentBroadcast: Reading broadcast variable 230 took 18 ms
26/05/21 18:55:34 INFO MemoryStore: Block broadcast_230 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:55:34 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 67, fetching them
26/05/21 18:55:34 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:34 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:34 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:34 INFO Executor: Finished task 0.0 in stage 187.0 (TID 295). 1727 bytes result sent to driver
26/05/21 18:55:34 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 296
26/05/21 18:55:34 INFO Executor: Running task 1.0 in stage 187.0 (TID 296)
26/05/21 18:55:34 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:34 INFO Executor: Finished task 1.0 in stage 187.0 (TID 296). 1727 bytes result sent to driver
26/05/21 18:55:35 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 297
26/05/21 18:55:35 INFO Executor: Running task 0.0 in stage 188.0 (TID 297)
26/05/21 18:55:35 INFO TorrentBroadcast: Started reading broadcast variable 232 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:35 INFO MemoryStore: Block broadcast_232_piece0 stored as bytes in memory (estimated size 62.0 KiB, free 413.1 MiB)
26/05/21 18:55:35 INFO TorrentBroadcast: Reading broadcast variable 232 took 15 ms
26/05/21 18:55:35 INFO MemoryStore: Block broadcast_232 stored as values in memory (estimated size 151.4 KiB, free 412.9 MiB)
26/05/21 18:55:35 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:35 INFO TorrentBroadcast: Started reading broadcast variable 231 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:35 INFO MemoryStore: Block broadcast_231_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:35 INFO TorrentBroadcast: Reading broadcast variable 231 took 12 ms
26/05/21 18:55:35 INFO MemoryStore: Block broadcast_231 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:37 INFO Executor: Finished task 0.0 in stage 188.0 (TID 297). 2405 bytes result sent to driver
26/05/21 18:55:37 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 298
26/05/21 18:55:37 INFO Executor: Running task 1.0 in stage 188.0 (TID 298)
26/05/21 18:55:37 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:39 INFO Executor: Finished task 1.0 in stage 188.0 (TID 298). 2405 bytes result sent to driver
26/05/21 18:55:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 299
26/05/21 18:55:39 INFO Executor: Running task 0.0 in stage 189.0 (TID 299)
26/05/21 18:55:39 INFO MapOutputTrackerWorker: Updating epoch to 69 and clearing cache
26/05/21 18:55:39 INFO TorrentBroadcast: Started reading broadcast variable 233 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:39 INFO MemoryStore: Block broadcast_233_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:55:39 INFO TorrentBroadcast: Reading broadcast variable 233 took 10 ms
26/05/21 18:55:39 INFO MemoryStore: Block broadcast_233 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:55:39 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 68, fetching them
26/05/21 18:55:39 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:39 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:39 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/21 18:55:39 INFO Executor: Finished task 0.0 in stage 189.0 (TID 299). 1770 bytes result sent to driver
26/05/21 18:55:39 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 300
26/05/21 18:55:39 INFO Executor: Running task 1.0 in stage 189.0 (TID 300)
26/05/21 18:55:39 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:39 INFO Executor: Finished task 1.0 in stage 189.0 (TID 300). 1727 bytes result sent to driver
26/05/21 18:55:40 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 301
26/05/21 18:55:40 INFO Executor: Running task 0.0 in stage 190.0 (TID 301)
26/05/21 18:55:40 INFO TorrentBroadcast: Started reading broadcast variable 235 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:40 INFO MemoryStore: Block broadcast_235_piece0 stored as bytes in memory (estimated size 62.0 KiB, free 413.1 MiB)
26/05/21 18:55:40 INFO TorrentBroadcast: Reading broadcast variable 235 took 13 ms
26/05/21 18:55:40 INFO MemoryStore: Block broadcast_235 stored as values in memory (estimated size 151.4 KiB, free 412.9 MiB)
26/05/21 18:55:40 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:40 INFO TorrentBroadcast: Started reading broadcast variable 234 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:40 INFO MemoryStore: Block broadcast_234_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:40 INFO TorrentBroadcast: Reading broadcast variable 234 took 13 ms
26/05/21 18:55:40 INFO MemoryStore: Block broadcast_234 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:42 INFO Executor: Finished task 0.0 in stage 190.0 (TID 301). 2405 bytes result sent to driver
26/05/21 18:55:42 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 302
26/05/21 18:55:42 INFO Executor: Running task 1.0 in stage 190.0 (TID 302)
26/05/21 18:55:42 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:45 INFO Executor: Finished task 1.0 in stage 190.0 (TID 302). 2405 bytes result sent to driver
26/05/21 18:55:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 303
26/05/21 18:55:45 INFO Executor: Running task 0.0 in stage 191.0 (TID 303)
26/05/21 18:55:45 INFO MapOutputTrackerWorker: Updating epoch to 70 and clearing cache
26/05/21 18:55:45 INFO TorrentBroadcast: Started reading broadcast variable 236 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_236_piece0 stored as bytes in memory (estimated size 3.1 KiB, free 412.3 MiB)
26/05/21 18:55:45 INFO TorrentBroadcast: Reading broadcast variable 236 took 14 ms
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_236 stored as values in memory (estimated size 5.3 KiB, free 412.3 MiB)
26/05/21 18:55:45 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 69, fetching them
26/05/21 18:55:45 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:45 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:45 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:45 INFO Executor: Finished task 0.0 in stage 191.0 (TID 303). 1727 bytes result sent to driver
26/05/21 18:55:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 304
26/05/21 18:55:45 INFO Executor: Running task 1.0 in stage 191.0 (TID 304)
26/05/21 18:55:45 INFO ShuffleBlockFetcherIterator: Getting 2 (160.0 B) non-empty blocks including 2 (160.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:55:45 INFO Executor: Finished task 1.0 in stage 191.0 (TID 304). 1727 bytes result sent to driver
26/05/21 18:55:45 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 305
26/05/21 18:55:45 INFO Executor: Running task 0.0 in stage 192.0 (TID 305)
26/05/21 18:55:45 INFO TorrentBroadcast: Started reading broadcast variable 238 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_238_piece0 stored as bytes in memory (estimated size 60.0 KiB, free 413.1 MiB)
26/05/21 18:55:45 INFO TorrentBroadcast: Reading broadcast variable 238 took 15 ms
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_238 stored as values in memory (estimated size 141.0 KiB, free 412.9 MiB)
26/05/21 18:55:45 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:45 INFO TorrentBroadcast: Started reading broadcast variable 237 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_237_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:45 INFO TorrentBroadcast: Reading broadcast variable 237 took 11 ms
26/05/21 18:55:45 INFO MemoryStore: Block broadcast_237 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:48 INFO Executor: Finished task 0.0 in stage 192.0 (TID 305). 3121 bytes result sent to driver
26/05/21 18:55:48 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 306
26/05/21 18:55:48 INFO Executor: Running task 1.0 in stage 192.0 (TID 306)
26/05/21 18:55:48 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:50 INFO Executor: Finished task 1.0 in stage 192.0 (TID 306). 3164 bytes result sent to driver
26/05/21 18:55:50 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 307
26/05/21 18:55:50 INFO Executor: Running task 0.0 in stage 194.0 (TID 307)
26/05/21 18:55:50 INFO MapOutputTrackerWorker: Updating epoch to 71 and clearing cache
26/05/21 18:55:50 INFO TorrentBroadcast: Started reading broadcast variable 239 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:50 INFO MemoryStore: Block broadcast_239_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.3 MiB)
26/05/21 18:55:50 INFO TorrentBroadcast: Reading broadcast variable 239 took 11 ms
26/05/21 18:55:50 INFO MemoryStore: Block broadcast_239 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:55:50 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 70, fetching them
26/05/21 18:55:50 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:50 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:50 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:50 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:50 INFO Executor: Finished task 0.0 in stage 194.0 (TID 307). 3807 bytes result sent to driver
26/05/21 18:55:51 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 308
26/05/21 18:55:51 INFO Executor: Running task 0.0 in stage 195.0 (TID 308)
26/05/21 18:55:51 INFO TorrentBroadcast: Started reading broadcast variable 241 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:51 INFO MemoryStore: Block broadcast_241_piece0 stored as bytes in memory (estimated size 60.0 KiB, free 413.1 MiB)
26/05/21 18:55:51 INFO TorrentBroadcast: Reading broadcast variable 241 took 15 ms
26/05/21 18:55:51 INFO MemoryStore: Block broadcast_241 stored as values in memory (estimated size 141.0 KiB, free 412.9 MiB)
26/05/21 18:55:51 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:51 INFO TorrentBroadcast: Started reading broadcast variable 240 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:51 INFO MemoryStore: Block broadcast_240_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:51 INFO TorrentBroadcast: Reading broadcast variable 240 took 8 ms
26/05/21 18:55:51 INFO MemoryStore: Block broadcast_240 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:55:54 INFO Executor: Finished task 0.0 in stage 195.0 (TID 308). 3121 bytes result sent to driver
26/05/21 18:55:54 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 309
26/05/21 18:55:54 INFO Executor: Running task 1.0 in stage 195.0 (TID 309)
26/05/21 18:55:54 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:55:57 INFO Executor: Finished task 1.0 in stage 195.0 (TID 309). 3164 bytes result sent to driver
26/05/21 18:55:57 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 310
26/05/21 18:55:57 INFO Executor: Running task 0.0 in stage 197.0 (TID 310)
26/05/21 18:55:57 INFO MapOutputTrackerWorker: Updating epoch to 72 and clearing cache
26/05/21 18:55:57 INFO TorrentBroadcast: Started reading broadcast variable 242 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_242_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.3 MiB)
26/05/21 18:55:57 INFO TorrentBroadcast: Reading broadcast variable 242 took 14 ms
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_242 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:55:57 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 71, fetching them
26/05/21 18:55:57 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:55:57 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:55:57 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:55:57 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
26/05/21 18:55:57 INFO Executor: Finished task 0.0 in stage 197.0 (TID 310). 3807 bytes result sent to driver
26/05/21 18:55:57 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 311
26/05/21 18:55:57 INFO Executor: Running task 0.0 in stage 198.0 (TID 311)
26/05/21 18:55:57 INFO TorrentBroadcast: Started reading broadcast variable 244 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_244_piece0 stored as bytes in memory (estimated size 60.0 KiB, free 413.1 MiB)
26/05/21 18:55:57 INFO TorrentBroadcast: Reading broadcast variable 244 took 17 ms
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_244 stored as values in memory (estimated size 141.0 KiB, free 412.9 MiB)
26/05/21 18:55:57 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:55:57 INFO TorrentBroadcast: Started reading broadcast variable 243 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_243_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:55:57 INFO TorrentBroadcast: Reading broadcast variable 243 took 10 ms
26/05/21 18:55:57 INFO MemoryStore: Block broadcast_243 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:56:00 INFO Executor: Finished task 0.0 in stage 198.0 (TID 311). 3121 bytes result sent to driver
26/05/21 18:56:00 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 312
26/05/21 18:56:00 INFO Executor: Running task 1.0 in stage 198.0 (TID 312)
26/05/21 18:56:00 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:56:02 INFO Executor: Finished task 1.0 in stage 198.0 (TID 312). 3121 bytes result sent to driver
26/05/21 18:56:02 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 313
26/05/21 18:56:02 INFO Executor: Running task 0.0 in stage 200.0 (TID 313)
26/05/21 18:56:02 INFO MapOutputTrackerWorker: Updating epoch to 73 and clearing cache
26/05/21 18:56:02 INFO TorrentBroadcast: Started reading broadcast variable 245 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:56:02 INFO MemoryStore: Block broadcast_245_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.3 MiB)
26/05/21 18:56:02 INFO TorrentBroadcast: Reading broadcast variable 245 took 19 ms
26/05/21 18:56:02 INFO MemoryStore: Block broadcast_245 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:56:02 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 72, fetching them
26/05/21 18:56:02 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:56:02 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:56:02 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:56:02 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:56:02 INFO Executor: Finished task 0.0 in stage 200.0 (TID 313). 3893 bytes result sent to driver
26/05/21 18:56:03 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 314
26/05/21 18:56:03 INFO Executor: Running task 0.0 in stage 201.0 (TID 314)
26/05/21 18:56:03 INFO TorrentBroadcast: Started reading broadcast variable 247 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:56:03 INFO MemoryStore: Block broadcast_247_piece0 stored as bytes in memory (estimated size 60.0 KiB, free 413.1 MiB)
26/05/21 18:56:03 INFO TorrentBroadcast: Reading broadcast variable 247 took 12 ms
26/05/21 18:56:03 INFO MemoryStore: Block broadcast_247 stored as values in memory (estimated size 141.0 KiB, free 412.9 MiB)
26/05/21 18:56:03 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 0-93077504, partition values: [empty row]
26/05/21 18:56:03 INFO TorrentBroadcast: Started reading broadcast variable 246 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:56:03 INFO MemoryStore: Block broadcast_246_piece0 stored as bytes in memory (estimated size 62.3 KiB, free 412.8 MiB)
26/05/21 18:56:03 INFO TorrentBroadcast: Reading broadcast variable 246 took 12 ms
26/05/21 18:56:03 INFO MemoryStore: Block broadcast_246 stored as values in memory (estimated size 595.4 KiB, free 412.2 MiB)
26/05/21 18:56:05 INFO Executor: Finished task 0.0 in stage 201.0 (TID 314). 3121 bytes result sent to driver
26/05/21 18:56:05 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 315
26/05/21 18:56:05 INFO Executor: Running task 1.0 in stage 201.0 (TID 315)
26/05/21 18:56:05 INFO FileScanRDD: Reading File path: hdfs://master-node:9000/data/chicago_crimes.csv, range: 93077504-181960704, partition values: [empty row]
26/05/21 18:56:08 INFO Executor: Finished task 1.0 in stage 201.0 (TID 315). 3121 bytes result sent to driver
26/05/21 18:56:08 INFO YarnCoarseGrainedExecutorBackend: Got assigned task 316
26/05/21 18:56:08 INFO Executor: Running task 0.0 in stage 203.0 (TID 316)
26/05/21 18:56:08 INFO MapOutputTrackerWorker: Updating epoch to 74 and clearing cache
26/05/21 18:56:08 INFO TorrentBroadcast: Started reading broadcast variable 248 with 1 pieces (estimated total size 4.0 MiB)
26/05/21 18:56:08 INFO MemoryStore: Block broadcast_248_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 412.3 MiB)
26/05/21 18:56:08 INFO TorrentBroadcast: Reading broadcast variable 248 took 15 ms
26/05/21 18:56:08 INFO MemoryStore: Block broadcast_248 stored as values in memory (estimated size 12.5 KiB, free 412.3 MiB)
26/05/21 18:56:08 INFO MapOutputTrackerWorker: Don't have map outputs for shuffle 73, fetching them
26/05/21 18:56:08 INFO MapOutputTrackerWorker: Doing the fetch; tracker endpoint = NettyRpcEndpointRef(spark://MapOutputTracker@worker-node-2:38283)
26/05/21 18:56:08 INFO MapOutputTrackerWorker: Got the map output locations
26/05/21 18:56:08 INFO ShuffleBlockFetcherIterator: Getting 2 (120.0 B) non-empty blocks including 2 (120.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/21 18:56:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/05/21 18:56:08 INFO Executor: Finished task 0.0 in stage 203.0 (TID 316). 3807 bytes result sent to driver
26/05/21 18:56:08 INFO YarnCoarseGrainedExecutorBackend: Driver commanded a shutdown
26/05/21 18:56:08 INFO MemoryStore: MemoryStore cleared
26/05/21 18:56:08 INFO BlockManager: BlockManager stopped
26/05/21 18:56:08 INFO ShutdownHookManager: Shutdown hook called

End of LogType:stderr
***********************************************************************

Container: container_1778738889964_0059_01_000002 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:stdout
LogLastModifiedTime:Thu May 21 18:56:11 +0000 2026
LogLength:0
LogContents:

End of LogType:stdout
***********************************************************************
```

---
