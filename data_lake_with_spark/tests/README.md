## Test Spark on an EMR cluster

### Test 1

```sh
spark-submit spark_script1.py
```

### Test 2

```sh
aws s3 cp ./cities.csv s3://<bucket_name>/
spark-submit spark_script2.py
```

### Test 3

- Copy the `csv` file from S3 to Hadoop file system.

```sh
# Make a directory first.
hdfs dfs -mkdir /user/sparkify-data
# Copy the data.
hadoop fs -cp s3://<bucket_name>/cities.csv /user/sparkify_data
```

One can connect to the `HDFS Name Node` web interface to browse the 
HDFS file structure.

## Test Spark on a standalone Spark cluster locally

```sh
sudo docker build -t spark-cluster-test .

docker run --rm -it spark-cluster-test /bin/bash
python /app/spark_script1.py
```
