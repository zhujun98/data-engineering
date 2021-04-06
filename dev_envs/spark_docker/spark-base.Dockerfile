FROM spark-local-cluster-base

ARG SPARK_VERSION
ARG HADOOP_VERSION

ENV SPARK_HOME=/usr/bin/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV PYSPARK_PYTHON python3

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar -xvzf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /usr/bin/ && \
    mkdir ${SPARK_HOME}/logs && \
    rm spark.tgz

WORKDIR ${SPARK_HOME}