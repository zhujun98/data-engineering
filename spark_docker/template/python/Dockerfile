FROM zhujun/spark-local-submit

RUN python3 -m pip install --upgrade pip && \
    pip3 install pyspark==${SPARK_VERSION}

ONBUILD COPY . /app
ONBUILD RUN if [[ -f /app/requirements.txt ]]; then pip3 install -r /app/requirements.txt; fi

CMD ["/bin/bash"]