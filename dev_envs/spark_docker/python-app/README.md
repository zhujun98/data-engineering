```sh
docker build -t spark-python-submit .

docker run --network spark_docker_default --rm -it spark-python-submit /bin/bash

spark-submit --master spark://spark-master:7077 app.py
```