FROM zhujun/spark-local-submit

ENV SPARK_APPLICATION_JAR_NAME application-1.0

COPY template.sh /

RUN apk add --no-cache openjdk8 maven\
      && chmod +x /template.sh \
      && mkdir -p /java \
      && mkdir -p /usr/src/java

# Copy the POM-file first, for separate dependency resolving and downloading
ONBUILD COPY pom.xml /usr/src/java
ONBUILD RUN cd /usr/src/java \
      && mvn dependency:resolve
ONBUILD RUN cd /usr/src/java \
      && mvn verify

# Copy the source code and build the application
ONBUILD COPY . /usr/src/java
ONBUILD RUN cd /usr/src/java \
      && mvn clean package

CMD ["/bin/bash", "/template.sh"]
