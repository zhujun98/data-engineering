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