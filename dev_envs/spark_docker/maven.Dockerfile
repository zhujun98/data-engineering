FROM spark-local-spark-base

ARG MAVEN_VERSION

RUN curl https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz -o maven.tar.gz && \
    tar -xzf maven.tar.gz && \
    mv apache-maven-${MAVEN_VERSION} /opt/ && \
    rm maven.tar.gz && \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/bin/mvn
