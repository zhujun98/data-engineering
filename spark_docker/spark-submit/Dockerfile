FROM spark-local-spark-base

ENV SPARK_APPLICATION_JAR_LOCATION=/app/application.jar
ENV SPARK_APPLICATION_PYTHON_LOCATION=/app/app.py
ENV SPARK_APPLICATION_MAIN_CLASS=my.main.Application
ENV SPARK_APPLICATION_ARGS=""

COPY spark-submit/submit.sh /

CMD ["/bin/bash", "/submit.sh"]
