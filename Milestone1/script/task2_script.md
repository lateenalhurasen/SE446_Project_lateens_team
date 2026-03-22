Log in & set the Hadoop Environment

Firts upload the files into the cluster, access hdfs cluster
```
(base) PS C:\Users\Latee\Desktop\Uni\SE 446\Milestone 1> scp mapperT2.py reducer.py lalhurasen@134.209.172.50:~/

(base) PS C:\Users\Latee\Desktop\Uni\SE 446\Milestone 1> ssh lalhurasen@134.209.172.50
```

---
Hadoop Cluster using mapred streaming

Counts how many times each crime type appears in the full Chicago crimes dataset and saves the counts to HDFS output folder.
```
mapred streaming \
  -files mapperT2.py,reducer.py \
  -mapper "python3 mapperT2.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/lalhurasen/project/m1/task2
```
---
View results

Displays results saved in the HDFS folder
```
hdfs dfs -cat /user/lalhurasen/project/m1/task2/part-00000
```
