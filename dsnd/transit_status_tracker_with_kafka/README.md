# Transit Status Tracker with Apache Kafka

Jun Zhu
___

The Chicago Transit Authority (CTA) has asked us to develop a dashboard 
displaying system status for its commuters. We have decided to use Kafka 
and ecosystem tools like REST Proxy and Kafka Connect to accomplish this task.

Our architecture will look like so:

![Project Architecture](./architecture.jpg)

## Start the Kafka ecosystem

```bash
docker-compose up
```

Once docker-compose is ready, the following services will be available:

| Service | Host URL | Docker URL
| --- | --- | --- |
| Public Transit Status | [http://localhost:8888](http://localhost:8888) | n/a |
| Landoop Kafka Connect UI | [http://localhost:8084](http://localhost:8084) | http://connect-ui:8084 |
| Landoop Kafka Topics UI | [http://localhost:8085](http://localhost:8085) | http://topics-ui:8085 |
| Landoop Schema Registry UI | [http://localhost:8086](http://localhost:8086) | http://schema-registry-ui:8086 |
| Kafka | PLAINTEXT://localhost:9092 | PLAINTEXT://kafka0:9092 |
| REST Proxy | [http://localhost:8082](http://localhost:8082/) | http://rest-proxy:8082/ |
| Schema Registry | [http://localhost:8081](http://localhost:8081/ ) | http://schema-registry:8081/ |
| Kafka Connect | [http://localhost:8083](http://localhost:8083) | http://kafka-connect:8083 |
| KSQL | [http://localhost:8088](http://localhost:8088) | http://ksql:8088 |
| PostgreSQL | jdbc:postgresql://localhost:5432/cta | jdbc:postgresql://postgres:5432/cta |


Check the topics at the Kafka Topics UI (http://localhost:8085) or use the CLI tools
```sh
docker exec broker kafka-topics --zookeeper zookeeper:2181 --list
```
If all the services started correctly, you should be able to see the following
list of topics:
```
__confluent.support.metrics
__consumer_offsets
_confluent-ksql-ksql_service_docker_command_topic
_schemas
connect-config
connect-offset
connect-status
```

## Start the data pipeline and the web server

```sh
conda create -n dsnd python==3.7.5
conda activate dsnd
pip install -r requirements.txt
```

Kafka topics and system parameters are defined in `config.ini`.

Start producers by:
```sh
python start_simulation.py
```

**Note**: You may need to increase the maximum number of opening 
files in the current session by, for example, `ulimit -n 65536`)

Start stream processing with Faust:
```sh
python start_faust_app.py worker
```

Start stream processing with KSQL:
```sh
python start_ksql.py
```

Start consumers and launch the web monitor:
```sh
python start_server.py
```

Open the browser to monitor the [CTA Transit Status](http://localhost:8888).

![](./misc/screenshot.jpg)

## Debug the system

### Test the producer alone with
```sh
python simple_consumer.py
```

### Check the Postgres database
```
docker exec -it postgresdb psql -U cta_admin cta
# List all tables.
\dt+
# Do your SQL queries.
```

Check Kafka connect status at the Kafka Connect UI (http://localhost:8084)
or use the CLI tools.
```
# Check the status of "stations" connector.
curl http://localhost:8083/connectors/stations/status | python -m json.tool
# Check the task status
curl http://localhost:8083/connectors/stations/tasks/0/status
```

### Use the ksqlDB CLI
```sh
# start ksqlDB CLI
docker exec -it ksqldb-server ksql
# list topics
SHOW TOPICS;
# list tables
SHOW TABLES;
```
