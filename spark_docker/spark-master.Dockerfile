FROM spark-local-spark-base

EXPOSE ${SPARK_MASTER_WEBUI_PORT} ${SPARK_MASTER_PORT}

CMD bin/spark-class org.apache.spark.deploy.master.Master \
    --ip ${SPARK_MASTER_HOST} \
    --port ${SPARK_MASTER_PORT} \
    --webui-port ${SPARK_MASTER_WEBUI_PORT} >> logs/spark-master.out
