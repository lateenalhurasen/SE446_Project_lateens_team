## Step 1 Output: Python scripts are correctly uploaded
```
mapperT5.py                                      100%  687     3.7KB/s   00:00
reducer.py                                       100%  616     3.7KB/s   00:00
```

## Step 2 Output: Successful login into the cluster

```bash
Last login: Wed Feb 18 07:27:24 2026 from 176.44.76.249
```
## Step 3 & 4 Output: Loading hadoop and running a streaming mapreduce using .py scripts
```bash
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob9603392745152245676.jar tmpDir=null
2026-02-20 13:56:00,188 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:56:00,571 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:56:01,112 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mnalsadoon/.staging/job_1771402826595_0033
2026-02-20 13:56:02,033 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-20 13:56:02,250 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-20 13:56:02,743 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0033
2026-02-20 13:56:02,743 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-20 13:56:03,128 INFO conf.Configuration: resource-types.xml not found
2026-02-20 13:56:03,129 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-20 13:56:03,270 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0033
2026-02-20 13:56:03,354 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0033/
2026-02-20 13:56:03,357 INFO mapreduce.Job: Running job: job_1771402826595_0033
2026-02-20 13:56:30,215 INFO mapreduce.Job: Job job_1771402826595_0033 running in uber mode : false
2026-02-20 13:56:30,219 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-20 13:57:19,323 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-20 13:57:36,528 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-20 13:57:37,557 INFO mapreduce.Job: Job job_1771402826595_0033 completed successfully
2026-02-20 13:57:37,967 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=96876
                FILE: Number of bytes written=1136885
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2391502
                HDFS: Number of bytes written=21
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=182086
                Total time spent by all reduces in occupied slots (ms)=30168
                Total time spent by all map tasks (ms)=91043
                Total time spent by all reduce tasks (ms)=15084
                Total vcore-milliseconds taken by all map tasks=91043
                Total vcore-milliseconds taken by all reduce tasks=15084
                Total megabyte-milliseconds taken by all map tasks=46614016
                Total megabyte-milliseconds taken by all reduce tasks=7723008
        Map-Reduce Framework
                Map input records=10001
                Map output records=9809
                Map output bytes=77252
                Map output materialized bytes=96882
                Input split bytes=212
                Combine input records=0
                Combine output records=0
                Reduce input groups=2
                Reduce shuffle bytes=96882
                Reduce input records=9809
                Reduce output records=2
                Spilled Records=19618
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=1710
                CPU time spent (ms)=4430
                Physical memory (bytes) snapshot=652091392
                Virtual memory (bytes) snapshot=6554685440
                Total committed heap usage (bytes)=348106752
                Peak Map Physical memory (bytes)=253251584
                Peak Map Virtual memory (bytes)=2182623232
                Peak Reduce Physical memory (bytes)=150908928
                Peak Reduce Virtual memory (bytes)=2189778944
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=2391290
        File Output Format Counters
                Bytes Written=21
2026-02-20 13:57:37,972 INFO streaming.StreamJob: Output directory: /user/mnalsadoon/project/m1/task5
```
