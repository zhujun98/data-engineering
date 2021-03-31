FROM zhujun/spark-local-base

RUN python3 -m pip install --upgrade pip && \
    pip3 install pyspark==${SPARK_VERSION} jupyterlab

EXPOSE 8888

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
