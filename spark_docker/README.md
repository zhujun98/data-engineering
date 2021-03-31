# Spark Cluster Setup with Docker

Modified from https://github.com/big-data-europe/docker-spark

## Build all the required images

```sh
./build.sh
```

## Run Spark cluster locally

### Using Docker Compose

Start the Spark cluster
```sh
docker-compose up
```

Shutdown the Spark cluster
```sh
docker-compose down
```

### Running Docker containers without the init daemon

#### Spark Master
To start a Spark master:

    docker run --name spark-master -h spark-master -d zhujun/spark-local-master

#### Spark Worker
To start a Spark worker:

    docker run --name spark-worker-1 --link spark-master:spark-master -d zhujun/spark-local-worker

### Launch a Spark application

Building and running your Spark application on top of the Spark cluster is as simple as extending a template Docker image. Check the template's README for further documentation.
* [Java template](template/java)
* [Python template](template/python)
* [Scala template](template/scala)
