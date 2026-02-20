## Upload the files into the cluster
`scp mapperT5.py reducer.py mnalsadoon@134.209.172.50:~/`
## Access hdfs cluster
`ssh mnalsadoon@134.209.172.50`
## Loading Hadoop environment
`source /etc/profile.d/hadoop.sh`
## Run mapreduce using python scripts

```mapred streaming \
  -files mapperT5.py,reducer.py \
  -mapper "python3 mapperT5.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/mnalsadoon/project/m1/task5```

## Viewing Results
`hdfs dfs -cat /user/mnalsadoon/project/m1/task5/part-00000`
