## Test submitting Python job on a standalone Spark cluster locally

```sh
docker build -t spark-python-submit-test .

docker run --network spark_docker_default --rm -it spark-python-submit-test /bin/bash
spark-submit --master spark://spark-master:7077 app.py
```