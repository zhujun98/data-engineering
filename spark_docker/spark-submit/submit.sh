#!/bin/bash

SPARK_MASTER_URL=spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}

if [[ -f "${SPARK_APPLICATION_JAR_LOCATION}" ]]; then
    echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    ${SPARK_HOME}/bin/spark-submit \
        --class ${SPARK_APPLICATION_MAIN_CLASS} \
        --master ${SPARK_MASTER_URL} \
        ${SPARK_SUBMIT_ARGS} ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
elif [[ -f "${SPARK_APPLICATION_PYTHON_LOCATION}" ]]; then
    echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    ${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER_URL} \
        ${SPARK_SUBMIT_ARGS} ${SPARK_APPLICATION_PYTHON_LOCATION} ${SPARK_APPLICATION_ARGS}
else
    echo "Not recognized application."
fi
