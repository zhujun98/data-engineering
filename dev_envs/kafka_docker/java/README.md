# Kafka Java API

### Setup

```bash
mvn compile
```

Create the topic `purchases-java`:
```sh
docker exec broker kafka-topics --bootstrap-server localhost:29092 --create --replication-factor 1 --partitions 2 --topic purchases-java
```
```

### Synchronous producer and consumer:

```sh
# terminal 1
mvn exec:java -Dexec.mainClass=ConsumerExample -Dexec.args=config.properties  
# terminal 2
mvn exec:java -Dexec.mainClass=ProducerExample -Dexec.args=config.properties  
```