# SE446 — Project Milestone 1: Chicago Crime Analytics with MapReduce

## Group Name
SE446 Project Lateen's Group

## Team Members
- Lateen Alhurasen — Student ID: 231543
- Layan Alshowaier — Student ID: 231361
- Almaha Alrasheed — Student ID: 231707
- Moudi Alsadoon — Student ID: 231589

## Executive Summary

---
## Task 2: Crime Type Distribution
- Instructions:

mapred command used
```
mapred streaming \
  -files mapperT2.py,reducer.py \
  -mapper "python3 mapperT2.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/lalhurasen/project/m1/task2
```
- Sample Results:

Top 5 lines
```
ARSON   14508
ASSAULT 570382
BATTERY 1547201
BURGLARY        448674
CONCEALED CARRY LICENSE VIOLATION       1709

```
- Interpretation:

Results show that THEFT (1,804,063) is the most common crime type in Chicago, followed by BATTERY (1,547,201) and CRIMINAL DAMAGE (965,727).
- Execution Logs:

MapReduce complete terminal output
```
lalhurasen@master-node:~$ mapred streaming \
  -files mapperT2.py,reducer.py \
  -mapper "python3 mapperT2.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/lalhurasen/project/m1/task2
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob10281167093172876370.jar tmpDir=null
2026-02-19 17:55:15,428 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-19 17:55:15,753 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-19 17:55:16,308 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/lalhurasen/.staging/job_1771402826595_0024
2026-02-19 17:55:17,156 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-19 17:55:17,313 INFO mapreduce.JobSubmitter: number of splits:15
2026-02-19 17:55:17,717 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0024
2026-02-19 17:55:17,717 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-19 17:55:18,039 INFO conf.Configuration: resource-types.xml not found
2026-02-19 17:55:18,039 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-19 17:55:18,174 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0024
2026-02-19 17:55:18,220 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0024/
2026-02-19 17:55:18,223 INFO mapreduce.Job: Running job: job_1771402826595_0024
2026-02-19 17:56:00,082 INFO mapreduce.Job: Job job_1771402826595_0024 running in uber mode : false
2026-02-19 17:56:00,084 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-19 17:56:46,435 INFO mapreduce.Job:  map 4% reduce 0%
2026-02-19 17:56:52,795 INFO mapreduce.Job:  map 9% reduce 0%
2026-02-19 17:56:58,076 INFO mapreduce.Job:  map 11% reduce 0%
2026-02-19 17:56:59,136 INFO mapreduce.Job:  map 15% reduce 0%
2026-02-19 17:57:04,530 INFO mapreduce.Job:  map 24% reduce 0%
2026-02-19 17:57:10,830 INFO mapreduce.Job:  map 26% reduce 0%
2026-02-19 17:57:17,089 INFO mapreduce.Job:  map 29% reduce 0%
2026-02-19 17:57:23,388 INFO mapreduce.Job:  map 31% reduce 0%
2026-02-19 17:57:24,435 INFO mapreduce.Job:  map 33% reduce 0%
2026-02-19 17:57:47,452 INFO mapreduce.Job:  map 37% reduce 0%
2026-02-19 17:57:53,723 INFO mapreduce.Job:  map 41% reduce 0%
2026-02-19 17:57:59,975 INFO mapreduce.Job:  map 47% reduce 0%
2026-02-19 17:58:04,152 INFO mapreduce.Job:  map 51% reduce 0%
2026-02-19 17:58:05,219 INFO mapreduce.Job:  map 53% reduce 0%
2026-02-19 17:58:18,615 INFO mapreduce.Job:  map 53% reduce 18%
2026-02-19 17:58:20,676 INFO mapreduce.Job:  map 56% reduce 18%
2026-02-19 17:58:26,828 INFO mapreduce.Job:  map 60% reduce 18%
2026-02-19 17:58:30,920 INFO mapreduce.Job:  map 60% reduce 20%
2026-02-19 17:58:45,264 INFO mapreduce.Job:  map 64% reduce 20%
2026-02-19 17:58:51,398 INFO mapreduce.Job:  map 67% reduce 20%
2026-02-19 17:58:52,418 INFO mapreduce.Job:  map 69% reduce 20%
2026-02-19 17:58:57,561 INFO mapreduce.Job:  map 70% reduce 20%
2026-02-19 17:58:58,578 INFO mapreduce.Job:  map 73% reduce 20%
2026-02-19 17:59:01,655 INFO mapreduce.Job:  map 76% reduce 20%
2026-02-19 17:59:02,684 INFO mapreduce.Job:  map 82% reduce 20%
2026-02-19 17:59:07,764 INFO mapreduce.Job:  map 84% reduce 27%
2026-02-19 17:59:10,865 INFO mapreduce.Job:  map 87% reduce 27%
2026-02-19 17:59:13,901 INFO mapreduce.Job:  map 87% reduce 29%
2026-02-19 17:59:37,182 INFO mapreduce.Job:  map 90% reduce 29%
2026-02-19 17:59:43,335 INFO mapreduce.Job:  map 93% reduce 29%
2026-02-19 17:59:50,448 INFO mapreduce.Job:  map 96% reduce 29%
2026-02-19 17:59:52,476 INFO mapreduce.Job:  map 98% reduce 29%
2026-02-19 17:59:53,496 INFO mapreduce.Job:  map 100% reduce 29%
2026-02-19 17:59:56,549 INFO mapreduce.Job:  map 100% reduce 38%
2026-02-19 18:00:03,695 INFO mapreduce.Job:  map 100% reduce 64%
2026-02-19 18:00:09,047 INFO mapreduce.Job:  map 100% reduce 67%
2026-02-19 18:00:15,297 INFO mapreduce.Job:  map 100% reduce 68%
2026-02-19 18:00:27,552 INFO mapreduce.Job:  map 100% reduce 75%
2026-02-19 18:00:33,681 INFO mapreduce.Job:  map 100% reduce 81%
2026-02-19 18:00:39,775 INFO mapreduce.Job:  map 100% reduce 87%
2026-02-19 18:00:45,875 INFO mapreduce.Job:  map 100% reduce 95%
2026-02-19 18:00:57,062 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-19 18:00:58,087 INFO mapreduce.Job: Job job_1771402826595_0024 completed successfully
2026-02-19 18:00:58,499 INFO mapreduce.Job: Counters: 55
        File System Counters
                FILE: Number of bytes read=128121071
                FILE: Number of bytes written=261272400
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2007768170
                HDFS: Number of bytes written=725
                HDFS: Number of read operations=50
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Killed map tasks=3
                Launched map tasks=18
                Launched reduce tasks=1
                Data-local map tasks=18
                Total time spent by all maps in occupied slots (ms)=1851888
                Total time spent by all reduces in occupied slots (ms)=423236
                Total time spent by all map tasks (ms)=925944
                Total time spent by all reduce tasks (ms)=211618
                Total vcore-milliseconds taken by all map tasks=925944
                Total vcore-milliseconds taken by all reduce tasks=211618
                Total megabyte-milliseconds taken by all map tasks=474083328
                Total megabyte-milliseconds taken by all reduce tasks=108348416
        Map-Reduce Framework
                Map input records=8493674
                Map output records=8493673
                Map output bytes=111133719
                Map output materialized bytes=128121155
                Input split bytes=1485
                Combine input records=0
                Combine output records=0
                Reduce input groups=34
                Reduce shuffle bytes=128121155
                Reduce input records=8493673
                Reduce output records=34
                Spilled Records=16987346
                Shuffled Maps =15
                Failed Shuffles=0
                Merged Map outputs=15
                GC time elapsed (ms)=10935
                CPU time spent (ms)=135250
                Physical memory (bytes) snapshot=4174069760
                Virtual memory (bytes) snapshot=34992242688
                Total committed heap usage (bytes)=2586865664
                Peak Map Physical memory (bytes)=282120192
                Peak Map Virtual memory (bytes)=2210263040
                Peak Reduce Physical memory (bytes)=373923840
                Peak Reduce Virtual memory (bytes)=2208169984
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=2007766685
        File Output Format Counters
                Bytes Written=725
2026-02-19 18:00:58,499 INFO streaming.StreamJob: Output directory: /user/lalhurasen/project/m1/task2
```
---
## Task 3
- Instructions: 
- Sample Results: 
- Interpretation: 
- Execution Logs: 
---
## Task 4
- Instructions: 
- Sample Results: 
- Interpretation: 
- Execution Logs: 
---
## Task 5
- Instructions: 
- Sample Results: 
- Interpretation: 
- Execution Logs:
---
## Members Contribution:
