# Kafka Java API

### Setup

```bash
mvn compile
```

### Synchronous producer and consumer:

```sh
# terminal 1
mvn exec:java -Dexec.mainClass=ConsumerExample -Dexec.args=config.properties  
# terminal 2
mvn exec:java -Dexec.mainClass=ProducerExample -Dexec.args=config.properties  
```