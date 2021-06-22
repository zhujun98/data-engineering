## Test submitting Java job on a standalone Spark cluster locally

```sh
mvn package

docker build -t spark-java-submit-test .

docker run --network spark_docker_default --rm -it spark-java-submit-test /bin/bash

/usr/bin/spark-3.1.1-bin-hadoop3.2/bin/spark-submit --master spark://spark-master:7077 --class LowerCase java-app-1.0.jar
```