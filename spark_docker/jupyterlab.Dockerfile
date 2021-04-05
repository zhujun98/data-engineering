FROM spark-local-cluster-base

ARG PYSPARK_VERSION
ARG JUPYTERLAB_VERSION
ARG SHARED_WORKSPACE

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install pyspark==${PYSPARK_VERSION} jupyterlab==${JUPYTERLAB_VERSION}

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
