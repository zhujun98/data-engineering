import argparse
import configparser
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
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


def produce_sync(broker_url, topic):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": broker_url})

    while True:
        p.produce(topic, Purchase().serialize())
        p.flush()


def consumer_sync(broker_url, topic):
    c = Consumer({
        "bootstrap.servers": broker_url,
        "group.id": "user1"
    })
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                print('Received message: {0}'.format(msg.value()))
            else:
                print('Error occured: {0}'.format(msg.error().str()))

    except KeyboardInterrupt:
        pass

    finally:
        c.close()


def maybe_create_topic(broker_url, topic):
    client = AdminClient({"bootstrap.servers": broker_url})

    topics = [v.topic for v in client.list_topics(timeout=5).topics.values()]

    if topic in topics:
        print(f"Topic {topic} already exists!")
    else:
        futures = client.create_topics(
            [NewTopic(topic=topic, num_partitions=5, replication_factor=1)]
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
    TOPIC = config['CLUSTER']['TOPIC']

    maybe_create_topic(BROKER_URL, TOPIC)
    try:
        if not args.sync:
            raise NotImplemented
        else:
            if not args.consumer:
                produce_sync(BROKER_URL, TOPIC)
            else:
                consumer_sync(BROKER_URL, TOPIC)
    except KeyboardInterrupt as e:
        print("shutting down")
