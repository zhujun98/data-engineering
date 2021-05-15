## Test Spark on a standalone Spark cluster locally

```sh
sudo docker build -t spark-cluster-test .

docker run --network spark_docker_default --rm -it spark-cluster-test /bin/bash
spark-submit --master spark://spark-master:7077 app.py
```