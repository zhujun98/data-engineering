FROM spark-local-pyspark-base

ARG JUPYTERLAB_VERSION

RUN pip3 install jupyterlab==${JUPYTERLAB_VERSION} matplotlib pandas

EXPOSE 8888

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
