

### First, i log in & set the hadoop environment

```bash
PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> scp .\Milestone1\src\task3\mapperT3.py .\Milestone1\src\task3\reducer.py amlaalrasheed@134.209.172.50:~/

PS C:\Users\ALMIHA\OneDrive\Documents\Desktop\SE446_Project_lateens_team> ssh amlaalrasheed@134.209.172.50

amlaalrasheed@master-node:~$ source /etc/profile.d/hadoop.sh
```

### Then, i run the mapReduce streaming 
```bash
mapred streaming \
  -files mapperT3.py,reducer.py \
  -mapper "python3 mapperT3.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/amlaalrasheed/project/m1/task3
```

### Lastly, the result has been displayed by this command
```bash
amlaalrasheed@master-node:~$ hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000
```

### Note, this has been added to display the top 5 
```bash
amlaalrasheed@master-node:~$  hdfs dfs -cat /user/amlaalrasheed/project/m1/task3/part-00000 | head -5
```

