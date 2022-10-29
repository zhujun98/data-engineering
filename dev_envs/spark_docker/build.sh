#!/bin/bash

COMMON_PREFIX=spark-local

# -- Software Stack Version

SPARK_VERSION=3.3.1
PYSPARK_VERSION=${SPARK_VERSION}
HADOOP_VERSION=3
JUPYTERLAB_VERSION=3.5.0

SHARED_WORKSPACE=/opt/workspace

# -- Building the Images

docker build \
  --build-arg SHARED_WORKSPACE=${SHARED_WORKSPACE} \
  -f cluster-base.Dockerfile \
  -t ${COMMON_PREFIX}-cluster-base .

docker build \
  --build-arg SPARK_VERSION=${SPARK_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  -f spark-base.Dockerfile \
  -t ${COMMON_PREFIX}-spark-base .

docker build \
  -f spark-master.Dockerfile \
  -t ${COMMON_PREFIX}-spark-master .

docker build \
  -f spark-worker.Dockerfile \
  -t ${COMMON_PREFIX}-spark-worker .

docker build \
  --build-arg PYSPARK_VERSION=${PYSPARK_VERSION} \
  --build-arg SHARED_WORKSPACE=${SHARED_WORKSPACE} \
  -f pyspark.Dockerfile \
  -t ${COMMON_PREFIX}-pyspark-base .

docker build \
  --build-arg JUPYTERLAB_VERSION=${JUPYTERLAB_VERSION} \
  -f jupyterlab.Dockerfile \
  -t ${COMMON_PREFIX}-jupyterlab .