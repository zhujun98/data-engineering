```sh
docker build -t spark-scala-submit .

docker run --network spark_docker_default --rm -it spark-scala-submit /bin/bash

mvn package
spark-submit --master spark://spark-master:7077 --class LowerCase target/scala-app-1.0-SNAPSHOT.jar
```