# Spark Cluster Setup with Docker

## Build all the required images

```sh
./build.sh
```

## Run Spark cluster locally

Start the Spark cluster
```sh
docker-compose up
```

Visit [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/) at 
http://127.0.0.1:8888/lab.

Shutdown the Spark cluster
```sh
docker-compose down
```

## Launch a Spark application

* [Python example](spark-submit/python-app)
* [Java example](spark-submit/java-app)
* [Scala example](spark-submit/scala-app)
