## Output for step 1: Correctly downloading the mapperT4.py and reducer.py files into the HDFS cluster
 ```
mapperT4.py                           100%  826     3.5KB/s   00:00
reducer.py                            100%  616     3.1KB/s   00:00
 ```

## Output for step 2: Logging in succesfully
It prompted to enter my password then moved on to:
 ```
lalshowaier@134.209.172.50's password:
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-170-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Fri Feb 20 13:46:57 UTC 2026

  System load:  0.0                Processes:             119
  Usage of /:   19.6% of 77.35GB   Users logged in:       1
  Memory usage: 27%                IPv4 address for eth0: 134.209.172.50
  Swap usage:   0%                 IPv4 address for eth0: 10.17.0.5

Expanded Security Maintenance for Applications is not enabled.

6 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


Last login: Wed Feb 18 07:15:25 2026 from 176.44.76.249
 ```
## Output for step 3: Successfully loading the Hadoop 
No visible output for this command

## Output for step 4: Successfully running the Hadoop 
 ```
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob12747843494104654193.jar tmpDir=null
2026-02-20 13:51:37,316 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:51:37,692 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:51:38,217 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/lalshowaier/.staging/job_1771402826595_0032
2026-02-20 13:51:38,998 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-20 13:51:39,159 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-20 13:51:39,581 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0032
2026-02-20 13:51:39,581 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-20 13:51:39,927 INFO conf.Configuration: resource-types.xml not found
2026-02-20 13:51:39,928 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-20 13:51:40,044 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0032
2026-02-20 13:51:40,096 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0032/
2026-02-20 13:51:40,099 INFO mapreduce.Job: Running job: job_1771402826595_0032
2026-02-20 13:54:23,673 INFO mapreduce.Job: Job job_1771402826595_0032 running in uber mode : false
2026-02-20 13:54:23,675 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-20 13:54:52,140 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-20 13:55:11,407 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-20 13:55:12,438 INFO mapreduce.Job: Job job_1771402826595_0032 completed successfully
2026-02-20 13:55:12,970 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=90006
                FILE: Number of bytes written=1123187
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2391502
                HDFS: Number of bytes written=185
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=101688
                Total time spent by all reduces in occupied slots (ms)=32780
                Total time spent by all map tasks (ms)=50844
                Total time spent by all reduce tasks (ms)=16390
                Total vcore-milliseconds taken by all map tasks=50844
                Total vcore-milliseconds taken by all reduce tasks=16390
                Total megabyte-milliseconds taken by all map tasks=26032128
                Total megabyte-milliseconds taken by all reduce tasks=8391680
        Map-Reduce Framework
                Map input records=10001
                Map output records=10000
                Map output bytes=70000
                Map output materialized bytes=90012
                Input split bytes=212
                Combine input records=0
                Combine output records=0
                Reduce input groups=24
                Reduce shuffle bytes=90012
                Reduce input records=10000
                Reduce output records=24
                Spilled Records=20000
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=970
                CPU time spent (ms)=4560
                Physical memory (bytes) snapshot=641437696
                Virtual memory (bytes) snapshot=6559879168
                Total committed heap usage (bytes)=348180480
                Peak Map Physical memory (bytes)=252727296
                Peak Map Virtual memory (bytes)=2185744384
                Peak Reduce Physical memory (bytes)=141570048
                Peak Reduce Virtual memory (bytes)=2189475840
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
                Bytes Written=185
2026-02-20 13:55:12,976 INFO streaming.StreamJob: Output directory: /user/lalshowaier/project/m1/task4
 ```
## Output for step 4: Results for the number of crimes in Chicago
 ```
2001: 4
2002: 2
2003: 1
2004: 6
2005: 19
2006: 4
2007: 7
2008: 16
2009: 5
2010: 5
2011: 7
2012: 9
2013: 10
2014: 16
2015: 28
2016: 20
2017: 49
2018: 28
2019: 36
2020: 25
2021: 83
2022: 135
2023: 9446
2024: 39
 ```
# ADD TOP 5



## Appendix: Full execution

 ```
PS C:\Users\layan\OneDrive\Desktop\SE446_Project_lateens_team> scp mapperT4.py reducer.py lalshowaier@134.209.172.50:~/
C:\windows\System32\OpenSSH\scp.exe: stat local "mapperT4.py": No such file or directory
PS C:\Users\layan\OneDrive\Desktop\SE446_Project_lateens_team> scp Milestone1/src/task4/mapperT4.py Milestone1/src/task4/reducer.py lalshowaier@134.209.172.50:~/
lalshowaier@134.209.172.50's password:
mapperT4.py                           100%  826     3.5KB/s   00:00
reducer.py                            100%  616     3.1KB/s   00:00
PS C:\Users\layan\OneDrive\Desktop\SE446_Project_lateens_team> ssh your_id@134.209.172.50
your_id@134.209.172.50's password:
Permission denied, please try again.
your_id@134.209.172.50's password:
PS C:\Users\layan\OneDrive\Desktop\SE446_Project_lateens_team> ssh lalshowaier@134.209.172.50
lalshowaier@134.209.172.50's password:
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-170-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Fri Feb 20 13:46:57 UTC 2026

  System load:  0.0                Processes:             119
  Usage of /:   19.6% of 77.35GB   Users logged in:       1
  Memory usage: 27%                IPv4 address for eth0: 134.209.172.50
  Swap usage:   0%                 IPv4 address for eth0: 10.17.0.5

Expanded Security Maintenance for Applications is not enabled.

6 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


Last login: Wed Feb 18 07:15:25 2026 from 176.44.76.249
lalshowaier@master-node:~$ source /etc/profile.d/hadoop.sh
lalshowaier@master-node:~$ mapred streaming \
  -files mapperT4.py,reducer.py \
  -mapperT4 "python3 mapperT4.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/lalshowaier/project/m1/task4
2026-02-20 13:50:18,439 ERROR streaming.StreamJob: Unrecognized option: -mapperT4
Usage: $HADOOP_HOME/bin/hadoop jar hadoop-streaming.jar [options]
Options:
  -input          <path> DFS input file(s) for the Map step.
  -output         <path> DFS output directory for the Reduce step.
  -mapper         <cmd|JavaClassName> Optional. Command to be run as mapper.
  -combiner       <cmd|JavaClassName> Optional. Command to be run as combiner.
  -reducer        <cmd|JavaClassName> Optional. Command to be run as reducer.
  -file           <file> Optional. File/dir to be shipped in the Job jar file.
                  Deprecated. Use generic option "-files" instead.
  -inputformat    <TextInputFormat(default)|SequenceFileAsTextInputFormat|JavaClassName>
                  Optional. The input format class.
  -outputformat   <TextOutputFormat(default)|JavaClassName>
                  Optional. The output format class.
  -partitioner    <JavaClassName>  Optional. The partitioner class.
  -numReduceTasks <num> Optional. Number of reduce tasks.
  -inputreader    <spec> Optional. Input recordreader spec.
  -cmdenv         <n>=<v> Optional. Pass env.var to streaming commands.
  -mapdebug       <cmd> Optional. To run this script when a map task fails.
  -reducedebug    <cmd> Optional. To run this script when a reduce task fails.
  -io             <identifier> Optional. Format to use for input to and output
                  from mapper/reducer commands
  -lazyOutput     Optional. Lazily create Output.
  -background     Optional. Submit the job and don't wait till it completes.
  -verbose        Optional. Print verbose output.
  -info           Optional. Print detailed usage.
  -help           Optional. Print help message.

Generic options supported are:
-conf <configuration file>        specify an application configuration file
-D <property=value>               define a value for a given property
-fs <file:///|hdfs://namenode:port> specify default filesystem URL to use, overrides 'fs.defaultFS' property from configurations.
-jt <local|resourcemanager:port>  specify a ResourceManager
-files <file1,...>                specify a comma-separated list of files to be copied to the map reduce cluster
-libjars <jar1,...>               specify a comma-separated list of jar files to be included in the classpath
-archives <archive1,...>          specify a comma-separated list of archives to be unarchived on the compute machines

The general command line syntax is:
command [genericOptions] [commandOptions]


For more details about these options:
Use $HADOOP_HOME/bin/hadoop jar hadoop-streaming.jar -info

Try -help for more information
Streaming Command Failed!
lalshowaier@master-node:~$ mapred streaming \
  -files mapperT4.py,reducer.py \
  -mapper "python3 mapperT4.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/lalshowaier/project/m1/task4
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob12747843494104654193.jar tmpDir=null
2026-02-20 13:51:37,316 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:51:37,692 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:51:38,217 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/lalshowaier/.staging/job_1771402826595_0032
2026-02-20 13:51:38,998 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-20 13:51:39,159 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-20 13:51:39,581 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0032
2026-02-20 13:51:39,581 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-20 13:51:39,927 INFO conf.Configuration: resource-types.xml not found
2026-02-20 13:51:39,928 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-20 13:51:40,044 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0032
2026-02-20 13:51:40,096 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0032/
2026-02-20 13:51:40,099 INFO mapreduce.Job: Running job: job_1771402826595_0032
2026-02-20 13:54:23,673 INFO mapreduce.Job: Job job_1771402826595_0032 running in uber mode : false
2026-02-20 13:54:23,675 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-20 13:54:52,140 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-20 13:55:11,407 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-20 13:55:12,438 INFO mapreduce.Job: Job job_1771402826595_0032 completed successfully
2026-02-20 13:55:12,970 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=90006
                FILE: Number of bytes written=1123187
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2391502
                HDFS: Number of bytes written=185
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=101688
                Total time spent by all reduces in occupied slots (ms)=32780
                Total time spent by all map tasks (ms)=50844
                Total time spent by all reduce tasks (ms)=16390
                Total vcore-milliseconds taken by all map tasks=50844
                Total vcore-milliseconds taken by all reduce tasks=16390
                Total megabyte-milliseconds taken by all map tasks=26032128
                Total megabyte-milliseconds taken by all reduce tasks=8391680
        Map-Reduce Framework
                Map input records=10001
                Map output records=10000
                Map output bytes=70000
                Map output materialized bytes=90012
                Input split bytes=212
                Combine input records=0
                Combine output records=0
                Reduce input groups=24
                Reduce shuffle bytes=90012
                Reduce input records=10000
                Reduce output records=24
                Spilled Records=20000
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=970
                CPU time spent (ms)=4560
                Physical memory (bytes) snapshot=641437696
                Virtual memory (bytes) snapshot=6559879168
                Total committed heap usage (bytes)=348180480
                Peak Map Physical memory (bytes)=252727296
                Peak Map Virtual memory (bytes)=2185744384
                Peak Reduce Physical memory (bytes)=141570048
                Peak Reduce Virtual memory (bytes)=2189475840
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
                Bytes Written=185
2026-02-20 13:55:12,976 INFO streaming.StreamJob: Output directory: /user/lalshowaier/project/m1/task4
lalshowaier@master-node:~$ hdfs dfs -cat /user/lalshowaier/project/m1/ta
sk4/part-00000
2001    4
2002    2
2003    1
2004    6
2005    19
2006    4
2007    7
2008    16
2009    5
2010    5
2011    7
2012    9
2013    10
2014    16
2015    28
2016    20
2017    49
2018    28
2019    36
2020    25
2021    83
2022    135
2023    9446
2024    39
 ```
