### Step 1: Downloading mapperT4.py and reducer.py to the HDFS cluster
`scp Milestone1/src/task4/mapperT4.py Milestone1/src/task4/reducer.py lalshowaier@134.209.172.50:~/`

then it prompted me to enter my password

### Step 2: Logging in the HDFS cluster
`ssh lalshowaier@134.209.172.50`

 then it prompted me to enter my password

### Step 3: Loading the Hadoop
`source /etc/profile.d/hadoop.sh`


### Step 4: Running the Hadoop
 ```mapred streaming \
  -files mapperT4.py,reducer.py \
  -mapper "python3 mapperT4.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/lalshowaier/project/m1/task4
```

### Step 5: Viewing the output
`hdfs dfs -cat /user/lalshowaier/project/m1/ta
sk4/part-00000`



