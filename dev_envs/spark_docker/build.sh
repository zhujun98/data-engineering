#!/bin/bash

COMMON_PREFIX=spark-local

# -- Software Stack Version

# 11.0.12-jre-slim if JDK is not needed for compiling Java or Scala apps
OPENJDK_DEBIAN_IMAGE_TAG=11.0.12
SPARK_VERSION=3.3.1
PYSPARK_VERSION=${SPARK_VERSION}
HADOOP_VERSION=3
MAVEN_VERSION=3.8.6
JUPYTERLAB_VERSION=3.5.0

SHARED_WORKSPACE=/opt/workspace

# -- Building the Images

docker build \
  --build-arg SHARED_WORKSPACE=${SHARED_WORKSPACE} \
  --build-arg DEBIAN_IMAGE_TAG=${OPENJDK_DEBIAN_IMAGE_TAG} \
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
  --build-arg MAVEN_VERSION=${MAVEN_VERSION} \
  -f maven.Dockerfile \
  -t ${COMMON_PREFIX}-maven .

docker build \
  --build-arg PYSPARK_VERSION=${PYSPARK_VERSION} \
  --build-arg SHARED_WORKSPACE=${SHARED_WORKSPACE} \
  -f pyspark.Dockerfile \
  -t ${COMMON_PREFIX}-pyspark .

docker build \
  --build-arg JUPYTERLAB_VERSION=${JUPYTERLAB_VERSION} \
  -f jupyterlab.Dockerfile \
  -t ${COMMON_PREFIX}-jupyterlab .