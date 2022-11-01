```sh
docker build -t spark-java-submit .

docker run --network spark_docker_default --rm -it spark-java-submit /bin/bash

mvn package
spark-submit --master spark://spark-master:7077 --class LowerCase target/app-1.0-SNAPSHOT.jar
```