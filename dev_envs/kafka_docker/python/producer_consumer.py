import argparse
import configparser
from dataclasses import dataclass, field
import json
import socket
import random

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


@dataclass
class Purchase:
    _faker = Faker()

    username: str = field(default_factory=_faker.user_name)
    currency: str = field(default_factory=_faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


def produce_sync(broker_url, topic, *, count):
    """Produces data synchronously into the Kafka Topic"""
    conf = {
        "bootstrap.servers": broker_url,
        "client.id": socket.gethostname()
    }

    p = Producer(conf)
    for _ in range(count):
        p.produce(topic, value=Purchase().serialize())
        p.flush()


def consumer_sync(broker_url, topic, *, partitions):
    conf = {
        "bootstrap.servers": broker_url,
        "group.id": "Jun"
    }

    c = Consumer(conf)
    c.subscribe([topic])
    try:
        while True:
            msg = c.poll(timeout=0.1)
            if msg is None:
                continue

            msg_err = msg.error()
            if not msg_err:
                assert(msg.topic() == topic)
                assert(msg.key() is None)
                assert(0 <= msg.partition() <= partitions)
                print(f"Received message: {json.loads(msg.value())}")
            else:
                if msg_err.code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"{msg.topic()} [{msg.partition()}] reached end "
                          f"at offset {msg.offset()}")
                else:
                    raise KafkaException(msg_err)

    finally:
        c.close()


def maybe_create_topic(broker_url, topic, *, partitions=1):
    client = AdminClient({"bootstrap.servers": broker_url})

    topics = [v.topic for v in client.list_topics(timeout=5).topics.values()]

    if topic in topics:
        print(f"Topic {topic} already exists!")
    else:
        futures = client.create_topics(
            [NewTopic(topic=topic,
                      num_partitions=partitions,
                      replication_factor=1)]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                pass


if __name__ == "__main__":
    """Checks for topic and creates the topic if it does not exist"""
    parser = argparse.ArgumentParser(description="kafka-producer-consumer")

    parser.add_argument('--sync',
                        action='store_true',
                        help="True for asynchronous and False for synchronous "
                             "producer and consumer")
    parser.add_argument('--consumer',
                        action='store_true',
                        help='False for producer and True for consumer '
                             '(used only for synchronous producer and consumer)')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('config.ini')

    BROKER_URL = config['CLUSTER']['BROKER_URL']
    TOPIC = config['TOPIC']['NAME']
    PARTITIONS = int(config['TOPIC']['PARTITIONS'])
    MAX_MESSAGES = int(config['TOPIC']['MAX_MESSAGES'])

    maybe_create_topic(BROKER_URL, TOPIC, partitions=PARTITIONS)
    try:
        if not args.sync:
            raise NotImplemented
        else:
            if not args.consumer:
                produce_sync(BROKER_URL, TOPIC, count=MAX_MESSAGES)
            else:
                consumer_sync(BROKER_URL, TOPIC, partitions=PARTITIONS)
    except KeyboardInterrupt as e:
        print("shutting down")
