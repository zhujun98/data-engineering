### Test Spark EMR cluster

- Copy data to S3.

```sh
aws s3 cp ./cities.csv s3://<bucket_name>
```

- Copy all the Python scripts to the cluster.

- Run the tests
```sh
spark-submit spark_script1.py
spark-submit spark_script2.py
```

- Copy the `csv` file from S3 to Hadoop file system.

One can connect to the `HDFS Name Node` web interface to browse the 
HDFS file structure.

```sh
# Make a directory first.
hdfs dfs -mkdir /user/sparkify-data
# Copy the data.
hadoop fs -cp s3://<bucket_name>/cities.csv /user/sparkify_data
```