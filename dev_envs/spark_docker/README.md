# Spark Cluster Setup with Docker

## Build all the required images

```sh
./build.sh
```

Image hierarchy:
```sh
                                     java-app
                                    /
                               maven
                             /      \
                            /        scala-app
                           /
                spark-base - spark-master
              /            \ 
             /               spark-worker
cluster-base
             \           python-app
              \        /
                pyspark
                       \
                         jupyterlab
```

## Run Spark cluster locally

Make sure `LOCAL_WORKSPACE` in `.env` is correct.

**Caveat**: You will need to run `docker volume rm spark_docker_shared-workspace` after modifying
            `LOCAL_WORKSPACE` in order to make the change take effect.

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

* [Python example](./python)
* [Java example](./java)
* [Scala example](./scala)
