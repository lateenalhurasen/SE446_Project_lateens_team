
## Task 3 – Location Hotspots Output

## From step 1 (i try upload scripts to cluster)

```powershell
scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/
```

## From step 1 (i try upload scripts to cluster)
```powershell
scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/
```
## OUTPUT step 1
```powershell
PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/
amlaalrasheed@134.209.172.50's password:
mapperT3.py                                                                           100%  624     2.9KB/s   00:00
reducer.py                                                                            100%  616     3.6KB/s   00:00
```

## From step 1 (i log-in into the HDFS cluster-> log in successfully)
```powershell
PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> ssh amlaalrasheed@134.209.172.50
amlaalrasheed@134.209.172.50's password:
```

## OUTPUT step 1
```powershell
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
```

## From step 1 (i try upload scripts to cluster)
```powershell
amlaalrasheed@master-node:~$ source /etc/profile.d/hadoop.sh
```
## (NO OUTPUT WILL APPEAR FOR THIS STEP)

## From step 2 (i run the streaming with the mapper, reducer, input and output)
```powershell
 mapred streaming \
  -files mapperT3.py,reducer.py \
  -mapper "python3 mapperT3.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/amlaalrasheed/project/m1/task3
```
## OUTPUT step 2
```powershell
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
```

## From step 3 (i try upload scripts to cluster)
```powershell
amlaalrasheed@master-node:~$ hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000
```
## OUTPUT step 3
```powershell
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
amlaalrasheed@master-node:~$  hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000 | head -5
"CTA ""L"" PLATFORM"    14
"CTA ""L"" TRAIN"       14
"SCHOOL 195010
"VEHICLE - OTHER RIDE SHARE SERVICE (E.G.       462
"VEHICLE - OTHER RIDE SHARE SERVICE (LYFT       636                                                                   
```
## From step 4 (based on the research question -> Research Question: Where do most crimes occur?)
```powershell
amlaalrasheed@master-node:~$ hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000 \
| sort -k2,2nr \
| head -5
```

## OUTPUT step 4
```powershell
STREET  2189093
RESIDENCE       1389343
APARTMENT       1011372
SIDEWALK        763643
OTHER   266754
```
