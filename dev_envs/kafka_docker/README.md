# Kafka Setup with Docker

Follow the `Step 1` provided by [Confluent](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html).

## Command line

List topics:
```sh
docker exec broker kafka-topics --list --zookeeper zookeeper:2181
```

Create two new topics `users` and `pageviews`:
```sh
docker exec broker kafka-topics --zookeeper zookeeper:2181 --create --replication-factor 1 --partitions 2 --topic users
docker exec broker kafka-topics --zookeeper zookeeper:2181 --create --replication-factor 1 --partitions 2 --topic pageviews
```

Check topic details
```sh
docker exec broker kafka-topics --zookeeper zookeeper:2181 --describe --topic users,pageviews
```

Create a producer
```sh
sudo docker exec -it broker kafka-console-producer --bootstrap-server PLAINTEXT://localhost:29092 --topic users
```
and type
```sh
> first message
> second message
```

Create a consumer
```sh
sudo docker exec broker kafka-console-consumer --bootstrap-server PLAINTEXT://localhost:29092 --topic users
```
and you should be able to see the above messages produced by the producer.

## Python API

```sh
pip install -r requirements.txt
```

Run synchronous producer and consumer:
```sh
# Terminal 1
python producer_consumer.py --sync
```
```sh
# Terminal 2
python producer_consumer.py --sync --consumer
```