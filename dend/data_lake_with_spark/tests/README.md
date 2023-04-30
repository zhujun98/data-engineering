## Test Spark on an EMR cluster

```sh
aws s3 cp ./cities.csv s3://<bucket_name>/
spark-submit app.py
```

## Test HDFS

- Copy the `csv` file from S3 to Hadoop file system.

```sh
# Make a directory first.
hdfs dfs -mkdir /user/sparkify-data
# Copy the data.
hadoop fs -cp s3://<bucket_name>/cities.csv /user/sparkify_data
```

One can connect to the `HDFS Name Node` web interface to browse the 
HDFS file structure.
