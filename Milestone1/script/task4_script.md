
`scp Milestone1/src/task4/mapperT4.py Milestone1/src/task4/reducer.py lalshowaier@134.209.172.50:~/`

then it prompted me to enter my password

`ssh lalshowaier@134.209.172.50`

 then it prompted me to enter my password

`source /etc/profile.d/hadoop.sh`

 ```mapred streaming \
  -files mapperT4.py,reducer.py \
  -mapper "python3 mapperT4.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/lalshowaier/project/m1/task4
```

`hdfs dfs -cat /user/lalshowaier/project/m1/ta
sk4/part-00000`



