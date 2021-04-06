FROM spark-local-spark-submit

ARG PYSPARK_VERSION

RUN apt-get install -y python3-pip && \
    pip3 install pyspark==${PYSPARK_VERSION}

ONBUILD COPY . /app
ONBUILD RUN /bin/bash -c "if [[ -f '/app/requirements.txt' ]]; then pip3 install -r /app/requirements.txt; fi"

CMD ["/bin/bash", "./submit.sh"]