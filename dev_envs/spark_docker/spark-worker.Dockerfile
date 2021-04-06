FROM spark-local-spark-base

EXPOSE ${SPARK_WORKER_WEBUI_PORT}

CMD bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port ${SPARK_WORKER_WEBUI_PORT} \
    spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> logs/spark-worker.out
