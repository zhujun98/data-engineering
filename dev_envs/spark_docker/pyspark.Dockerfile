FROM spark-local-cluster-base

ARG PYSPARK_VERSION
ARG SHARED_WORKSPACE

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install pyspark==${PYSPARK_VERSION}

WORKDIR ${SHARED_WORKSPACE}
