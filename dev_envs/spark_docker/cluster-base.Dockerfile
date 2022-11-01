ARG DEBIAN_IMAGE_TAG

FROM openjdk:${DEBIAN_IMAGE_TAG}

# -- Layer: OS + Python 3.7

ARG SHARED_WORKSPACE

RUN mkdir -p ${SHARED_WORKSPACE} && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# -- Runtime

VOLUME ${SHARED_WORKSPACE}
CMD ["bash"]