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
## Task 3 - Almaha Alrasheed
- Instructions:
First, we upload mapper and reducer py files to the cluster 
```powershell
scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/
```

Then, we log-in into the ssh enter into the cluster. After that, load Hadoop environment.

```bash
ssh amlaalrasheed@134.209.172.50
source /etc/profile.d/hadoop.sh
```

For step 3, we run the MR streaming job

```bash
mapred streaming \
  -files mapperT3.py,reducer.py \
  -mapper "python3 mapperT3.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/amlaalrasheed/project/m1/task3
```

Lastly, we display the full output and the top 5 places with the highest crime rate

```bash
hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000
```

```bash
hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000 \
| sort -k2,2nr \
| head -5
```
- Sample Results:
```bash
STREET  2189093
RESIDENCE       1389343
APARTMENT       1011372
SIDEWALK        763643
OTHER   266754
```

- Interpretation: In this task we try to find the spots in Chicago where crimes happen the most by counting all incidents. So, with using the mapperT3 and reducer code file we have the chance to invistigate the research question ( where is the cities with the most crimes in it ? ) as it appear in the sample result.

So, generaly speaking street locations account for the highest number of crimes (2,189,093), indicating that open public spaces are the most dangerous areas in Chicago, followed by residences and apartments.
  
- Execution Logs:
```powershell
PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/
amlaalrasheed@134.209.172.50's password:
mapperT3.py                                                                           100%  624     2.9KB/s   00:00
reducer.py                                                                            100%  616     3.6KB/s   00:00
PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> ssh amlaalrasheed@134.209.172.50
amlaalrasheed@134.209.172.50's password:
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-170-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Fri Feb 20 13:00:14 UTC 2026

  System load:  0.11               Processes:             113
  Usage of /:   19.6% of 77.35GB   Users logged in:       0
  Memory usage: 27%                IPv4 address for eth0: 134.209.172.50
  Swap usage:   0%                 IPv4 address for eth0: 10.17.0.5

Expanded Security Maintenance for Applications is not enabled.

6 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


Last login: Wed Feb 18 07:27:57 2026 from 176.44.76.249
amlaalrasheed@master-node:~$ source /etc/profile.d/hadoop.sh   
amlaalrasheed@master-node:~$ mapred streaming \
  -files mapperT3.py,reducer.py \
  -mapper "python3 mapperT3.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/amlaalrasheed/project/m1/task3
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob10393567573268189271.jar tmpDir=null
2026-02-20 13:04:48,160 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:04:48,427 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-20 13:04:48,839 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/amlaalrasheed/.staging/job_1771402826595_0030
2026-02-20 13:04:49,440 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-20 13:04:49,608 INFO mapreduce.JobSubmitter: number of splits:15
2026-02-20 13:04:49,959 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0030
2026-02-20 13:04:49,959 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-20 13:04:50,297 INFO conf.Configuration: resource-types.xml not found
2026-02-20 13:04:50,298 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-20 13:04:50,410 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0030
2026-02-20 13:04:50,463 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0030/
2026-02-20 13:04:50,465 INFO mapreduce.Job: Running job: job_1771402826595_0030
2026-02-20 13:05:34,469 INFO mapreduce.Job: Job job_1771402826595_0030 running in uber mode : false
2026-02-20 13:05:34,470 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-20 13:06:20,670 INFO mapreduce.Job:  map 5% reduce 0%
2026-02-20 13:06:27,032 INFO mapreduce.Job:  map 10% reduce 0%
2026-02-20 13:06:32,305 INFO mapreduce.Job:  map 13% reduce 0%
2026-02-20 13:06:34,805 INFO mapreduce.Job:  map 20% reduce 0%
2026-02-20 13:06:35,976 INFO mapreduce.Job:  map 22% reduce 0%
2026-02-20 13:06:41,244 INFO mapreduce.Job:  map 23% reduce 0%
2026-02-20 13:06:42,297 INFO mapreduce.Job:  map 24% reduce 0%
2026-02-20 13:06:47,507 INFO mapreduce.Job:  map 25% reduce 0%
2026-02-20 13:06:48,540 INFO mapreduce.Job:  map 27% reduce 0%
2026-02-20 13:06:54,799 INFO mapreduce.Job:  map 29% reduce 0%
2026-02-20 13:07:00,031 INFO mapreduce.Job:  map 31% reduce 0%
2026-02-20 13:07:01,111 INFO mapreduce.Job:  map 33% reduce 0%
2026-02-20 13:07:16,866 INFO mapreduce.Job:  map 35% reduce 0%
2026-02-20 13:07:17,900 INFO mapreduce.Job:  map 38% reduce 0%
2026-02-20 13:07:23,117 INFO mapreduce.Job:  map 39% reduce 0%
2026-02-20 13:07:24,152 INFO mapreduce.Job:  map 42% reduce 0%
2026-02-20 13:07:29,383 INFO mapreduce.Job:  map 44% reduce 0%
2026-02-20 13:07:30,418 INFO mapreduce.Job:  map 47% reduce 0%
2026-02-20 13:07:34,575 INFO mapreduce.Job:  map 51% reduce 0%
2026-02-20 13:07:35,657 INFO mapreduce.Job:  map 53% reduce 0%
2026-02-20 13:07:56,429 INFO mapreduce.Job:  map 53% reduce 18%
2026-02-20 13:07:57,456 INFO mapreduce.Job:  map 56% reduce 18%
2026-02-20 13:08:03,621 INFO mapreduce.Job:  map 57% reduce 18%
2026-02-20 13:08:07,769 INFO mapreduce.Job:  map 60% reduce 18%
2026-02-20 13:08:08,784 INFO mapreduce.Job:  map 60% reduce 20%
2026-02-20 13:08:15,997 INFO mapreduce.Job:  map 62% reduce 20%
2026-02-20 13:08:17,021 INFO mapreduce.Job:  map 65% reduce 20%
2026-02-20 13:08:23,178 INFO mapreduce.Job:  map 69% reduce 20%
2026-02-20 13:08:29,306 INFO mapreduce.Job:  map 73% reduce 20%
2026-02-20 13:08:34,452 INFO mapreduce.Job:  map 78% reduce 20%
2026-02-20 13:08:35,479 INFO mapreduce.Job:  map 80% reduce 20%
2026-02-20 13:08:39,563 INFO mapreduce.Job:  map 80% reduce 27%
2026-02-20 13:08:43,624 INFO mapreduce.Job:  map 83% reduce 27%
2026-02-20 13:08:49,795 INFO mapreduce.Job:  map 87% reduce 27%
2026-02-20 13:08:51,822 INFO mapreduce.Job:  map 87% reduce 29%
2026-02-20 13:09:15,357 INFO mapreduce.Job:  map 90% reduce 29%
2026-02-20 13:09:21,490 INFO mapreduce.Job:  map 93% reduce 29%
2026-02-20 13:09:27,610 INFO mapreduce.Job:  map 95% reduce 29%
2026-02-20 13:09:33,784 INFO mapreduce.Job:  map 98% reduce 29%
2026-02-20 13:09:35,825 INFO mapreduce.Job:  map 100% reduce 29%
2026-02-20 13:09:40,895 INFO mapreduce.Job:  map 100% reduce 49%
2026-02-20 13:09:46,982 INFO mapreduce.Job:  map 100% reduce 67%
2026-02-20 13:09:53,124 INFO mapreduce.Job:  map 100% reduce 68%
2026-02-20 13:09:59,237 INFO mapreduce.Job:  map 100% reduce 74%
2026-02-20 13:10:06,455 INFO mapreduce.Job:  map 100% reduce 79%
2026-02-20 13:10:11,574 INFO mapreduce.Job:  map 100% reduce 81%
2026-02-20 13:10:17,703 INFO mapreduce.Job:  map 100% reduce 89%
2026-02-20 13:10:23,867 INFO mapreduce.Job:  map 100% reduce 93%
2026-02-20 13:10:36,093 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-20 13:10:37,126 INFO mapreduce.Job: Job job_1771402826595_0030 completed successfully
2026-02-20 13:10:37,409 INFO mapreduce.Job: Counters: 55
        File System Counters
                FILE: Number of bytes read=134211732
                FILE: Number of bytes written=273454346
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2007768170
                HDFS: Number of bytes written=5150
                HDFS: Number of read operations=50
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Killed map tasks=2
                Launched map tasks=17
                Launched reduce tasks=1
                Data-local map tasks=17
                Total time spent by all maps in occupied slots (ms)=1950280
                Total time spent by all reduces in occupied slots (ms)=429606
                Total time spent by all map tasks (ms)=975140
                Total time spent by all reduce tasks (ms)=214803
                Total vcore-milliseconds taken by all map tasks=975140
                Total vcore-milliseconds taken by all reduce tasks=214803
                Total megabyte-milliseconds taken by all map tasks=499271680
                Total megabyte-milliseconds taken by all reduce tasks=109979136
        Map-Reduce Framework
                Map input records=8493674
                Map output records=8478117
                Map output bytes=117255492
                Map output materialized bytes=134211816
                Input split bytes=1485
                Combine input records=0
                Combine output records=0
                Reduce input groups=224
                Reduce shuffle bytes=134211816
                Reduce input records=8478117
                Reduce output records=224
                Spilled Records=16956234
                Shuffled Maps =15
                Failed Shuffles=0
                Merged Map outputs=15
                GC time elapsed (ms)=11256
                CPU time spent (ms)=139880
                Physical memory (bytes) snapshot=4133707776
                Virtual memory (bytes) snapshot=34986532864
                Total committed heap usage (bytes)=2601271296
                Peak Map Physical memory (bytes)=281718784
                Peak Map Virtual memory (bytes)=2207936512
                Peak Reduce Physical memory (bytes)=378081280
                Peak Reduce Virtual memory (bytes)=2207662080
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
                Bytes Written=5150
2026-02-20 13:10:37,410 INFO streaming.StreamJob: Output directory: /user/amlaalrasheed/project/m1/task3
amlaalrasheed@master-node:~$ hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000
"CTA ""L"" PLATFORM"    14
"CTA ""L"" TRAIN"       14
"SCHOOL 195010
"VEHICLE - OTHER RIDE SHARE SERVICE (E.G.       462
"VEHICLE - OTHER RIDE SHARE SERVICE (LYFT       636
ABANDONED BUILDING      12229
AIRCRAFT        1006
AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA 1305
AIRPORT BUILDING NON-TERMINAL - SECURE AREA     933
AIRPORT EXTERIOR - NON-SECURE AREA      1175
AIRPORT EXTERIOR - SECURE AREA  625
AIRPORT PARKING LOT     1819
AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA  2723
AIRPORT TERMINAL LOWER LEVEL - SECURE AREA      1185
AIRPORT TERMINAL MEZZANINE - NON-SECURE AREA    155
AIRPORT TERMINAL UPPER LEVEL - NON-SECURE AREA  1346
AIRPORT TERMINAL UPPER LEVEL - SECURE AREA      5787
AIRPORT TRANSPORTATION SYSTEM (ATS)     182
AIRPORT VENDING ESTABLISHMENT   512
AIRPORT/AIRCRAFT        15459
ALLEY   187775
ANIMAL HOSPITAL 913
APARTMENT       1011372
APPLIANCE STORE 2642
ATHLETIC CLUB   10399
ATM (AUTOMATIC TELLER MACHINE)  8756
AUTO    1396
AUTO / BOAT / RV DEALERSHIP     1661
BANK    33349
BANQUET HALL    2
BAR OR TAVERN   47676
BARBER SHOP/BEAUTY SALON        28
BARBERSHOP      8825
BASEMENT        35
BEACH   1
BOAT / WATERCRAFT       176
BOAT/WATERCRAFT 697
BOWLING ALLEY   827
BRIDGE  532
BUS     29509
CAR WASH        3634
CASINO/GAMBLING ESTABLISHMENT   117
CEMETARY        440
CHA APARTMENT   40613
CHA BREEZEWAY   3
CHA ELEVATOR    3
CHA GROUNDS     52
CHA HALLWAY     40
CHA HALLWAY / STAIRWELL / ELEVATOR      955
CHA HALLWAY/STAIRWELL/ELEVATOR  25021
CHA LOBBY       9
CHA PARKING LOT 61
CHA PARKING LOT / GROUNDS       2519
CHA PARKING LOT/GROUNDS 56036
CHA PLAY LOT    4
CHA STAIRWELL   10
CHURCH  6
CHURCH / SYNAGOGUE / PLACE OF WORSHIP   2531
CHURCH PROPERTY 2
CHURCH/SYNAGOGUE/PLACE OF WORSHIP       15467
CLEANERS/LAUNDROMAT     1
CLEANING STORE  5366
CLUB    18
COACH HOUSE     3
COIN OPERATED MACHINE   1177
COLLEGE / UNIVERSITY - GROUNDS  448
COLLEGE / UNIVERSITY - RESIDENCE HALL   134
COLLEGE/UNIVERSITY GROUNDS      5776
COLLEGE/UNIVERSITY RESIDENCE HALL       1399
COMMERCIAL / BUSINESS OFFICE    69214
CONSTRUCTION SITE       14352
CONVENIENCE STORE       28309
COUNTY JAIL     2
CREDIT UNION    596
CTA BUS 27274
CTA BUS STOP    8923
CTA GARAGE / OTHER PROPERTY     10254
CTA PARKING LOT / GARAGE / OTHER PROPERTY       972
CTA PLATFORM    41356
CTA PROPERTY    13
CTA STATION     7823
CTA SUBWAY STATION      2
CTA TRACKS - RIGHT OF WAY       202
CTA TRAIN       34967
CURRENCY EXCHANGE       12494
DAY CARE CENTER 3810
DELIVERY TRUCK  954
DEPARTMENT STORE        113362
DRIVEWAY        29
DRIVEWAY - RESIDENTIAL  24451
DRUG STORE      40279
DUMPSTER        7
ELEVATOR        2
EXPRESSWAY EMBANKMENT   1
FACTORY 2
FACTORY / MANUFACTURING BUILDING        761
FACTORY/MANUFACTURING BUILDING  6870
FARM    12
FEDERAL BUILDING        1096
FIRE STATION    1314
FISTS   16496
FOREST PRESERVE 472
FUNERAL PARLOR  1
GANGWAY 76
GARAGE  75
GARAGE/AUTO REPAIR      11
GAS STATION     94367
GAS STATION DRIVE/PROP. 71
GOVERNMENT BUILDING     2
GOVERNMENT BUILDING / PROPERTY  2823
GOVERNMENT BUILDING/PROPERTY    14740
GROCERY FOOD STORE      105728
HALLWAY 111
HIGHWAY / EXPRESSWAY    295
HIGHWAY/EXPRESSWAY      1079
HORSE STABLE    3
HOSPITAL        19
HOSPITAL BUILDING / GROUNDS     7283
HOSPITAL BUILDING/GROUNDS       22171
HOTEL   28
HOTEL / MOTEL   7182
HOTEL/MOTEL     29693
HOUSE   719
JAIL / LOCK-UP FACILITY 1274
JUNK YARD/GARBAGE DUMP  1
KENNEL  20
LAGOON  1
LAKE    6
LAKEFRONT / WATERFRONT / RIVERBANK      552
LAKEFRONT/WATERFRONT/RIVERBANK  1177
LAUNDRY ROOM    2
LIBRARY 7634
LICENSING CRIMINAL BACKGROUND EM"       12
LIQUOR STORE    13
LIVERY AUTO     1
LIVERY STAND OFFICE     2
LOADING DOCK    1
MEDICAL / DENTAL OFFICE 1765
MEDICAL/DENTAL OFFICE   7428
MOTEL   7
MOTOR VEH"      5182
MOTOR VEHICLE"  1821
MOVIE HOUSE / THEATER   412
MOVIE HOUSE/THEATER     2730
NEWSSTAND       245
NON-MOTOR VEHICLE"      85
NON-VEH"        494
NURSING / RETIREMENT HOME       4379
NURSING HOME    6
NURSING HOME/RETIREMENT HOME    14649
OFFICE  22
OTHER   266754
OTHER (SPECIFY) 24570
OTHER COMMERCIAL TRANSPORTATION 3448
OTHER RAILROAD PROP / TRAIN DEPOT       5911
OTHER RAILROAD PROPERTY / TRAIN DEPOT   806
PARK PROPERTY   63673
PARKING LOT     299
PARKING LOT / GARAGE (NON RESIDENTIAL)  47775
PARKING LOT/GARAGE(NON.RESID.)  198682
PAWN SHOP       769
POLICE FACILITY 1
POLICE FACILITY / VEHICLE PARKING LOT   4250
POLICE FACILITY/VEH PARKING LOT 18473
POOL ROOM       1006
POOLROOM        1
PORCH   410
PRAIRIE 2
PUBLIC GRAMMAR SCHOOL   2
PUBLIC HIGH SCHOOL      2
RADIO   6
RAILROAD PROPERTY       19
RESIDENCE       1389343
RESIDENCE - GARAGE      15460
RESIDENCE - PORCH / HALLWAY     18828
RESIDENCE - YARD (FRONT / BACK) 16035
RESIDENCE PORCH/HALLWAY 124112
RESIDENCE-GARAGE        134707
RESIDENTIAL YARD (FRONT/BACK)   74526
RESTAURANT      143069
RETAIL STORE    103
RIVER   4
RIVER BANK      6
ROOF    1
ROOMING HOUSE   2
SAVINGS AND LOAN        395
SCHOOL - PRIVATE BUILDING       1196
SCHOOL - PRIVATE GROUNDS        1590
SCHOOL - PUBLIC BUILDING        7624
SCHOOL - PUBLIC GROUNDS 7150
SCHOOL YARD     17
SCOOTER 7733
SEWER   3
SIDEWALK        763643
SMALL RETAIL STORE      171575
SPORTS ARENA / STADIUM  827
SPORTS ARENA/STADIUM    5266
STAIRWELL       28
STREET  2189093
TAVERN  40
TAVERN / LIQUOR STORE   2984
TAVERN/LIQUOR STORE     22455
TAXI CAB        6
TAXICAB 7813
TRAILER 4
TRUCK   9
TRUCKING TERMINAL       1
VACANT LOT      146
VACANT LOT / LAND       4110
VACANT LOT/LAND 24403
VEHICLE - COMMERCIAL    1348
VEHICLE - COMMERCIAL: ENTERTAINMENT / PARTY BUS 17
VEHICLE - COMMERCIAL: TROLLEY BUS       24
VEHICLE - DELIVERY TRUCK        444
VEHICLE - OTHER RIDE SERVICE    319
VEHICLE NON-COMMERCIAL  134111
VEHICLE-COMMERCIAL      5493
VEHICLE-COMMERCIAL - ENTERTAINMENT/PARTY BUS    10
VEHICLE-COMMERCIAL - TROLLEY BUS        10
VESTIBULE       28
WAREHOUSE       10739
WOODED AREA     7
YARD    336
YMCA    3     
amlaalrasheed@master-node:~$ hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000 \
| sort -k2,2nr \
| head -5
STREET  2189093
RESIDENCE       1389343
APARTMENT       1011372
SIDEWALK        763643
OTHER   266754
```
---
## Task 4 - Layan Alshowaier
- Instructions:
- 
Step 1: Upload mapperT4.py and reducer.py using scp.

Step 2: Connect to the server using SSH.

Step 3: Load Hadoop environment:

 ```
source /etc/profile.d/hadoop.sh
```

Step 4: Run Hadoop Streaming job:

 ```
mapred streaming \
-files mapperT4.py,reducer.py \
-mapper "python3 mapperT4.py" \
-reducer "python3 reducer.py" \
-input /data/chicago_crimes_sample.csv \
-output /user/lalshowaier/project/m1/task4
```

Step 5: Display results:
```
hdfs dfs -cat /user/lalshowaier/project/m1/task4/part-00000
```

- Sample Results:

```
2023    9446
2022    135
2021    83
2017    49
2024    39
```

- Interpretation:

The number of crimes in Chicago increases slowly throughout the years with a large spike in 2023 with a very high number of cases recorded in the dataset, making it the year with the highest crime count (9446), and answering the research question of "How has the total number of crimes changed over the years?"

- Execution Logs:
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
lalshowaier@master-node:~$ hdfs dfs -cat /user/lalshowaier/project/m1/task4/part-00000 | sort -k2 -nr | head -5
2023    9446
2022    135
2021    83
2017    49
2024    39
 ```

---
## Task 5
- Instructions:
 ```
1- Upload the files to the cluster: scp mapperT5.py reducer.py mnalsadoon@134.209.172.50:~/
 ```
2-
 ```
- Sample Results: 
- Interpretation: 
- Execution Logs:
  
---
## Members Contribution:

2- Almaha Alrasheed ( 231707 ) - TASK 3 Milestone 1 - Wrote mapperT3, ran cluster job, documented results in script and output folders.


all group member has participated in writing the reducer in python.
