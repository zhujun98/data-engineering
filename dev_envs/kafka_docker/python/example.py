import argparse
import asyncio
import configparser
from dataclasses import dataclass, field
import functools
import json
import os.path as osp
import random
import socket
import time

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


def _producer_configurations(broker_url):
    return {
        "bootstrap.servers": broker_url,
        # for better debugging experience
        "client.id": socket.gethostname(),
        "acks": "1",
    }


async def produce(broker_url, topic, *, num_messages):
    p = Producer(_producer_configurations(broker_url))

    produce_func = functools.partial(p.produce, topic,
                                     value=Purchase().serialize())
    loop = asyncio.get_running_loop()
    t0 = time.time()
    for i in range(1, num_messages+1):
        await loop.run_in_executor(None, produce_func)
        if i % (num_messages // 10) == 0:
            t1 = time.time()
            print(f"Produced {i} messages. Time elapsed: {t1 - t0:.2f} s")
            t0 = t1
        await asyncio.sleep(0.001)
    p.flush()


def produce_sync(broker_url, topic, *, num_messages):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer(_producer_configurations(broker_url))
    t0 = time.time()
    for i in range(1, num_messages+1):
        p.produce(topic, value=Purchase().serialize())
        p.flush()
        if i % (num_messages // 10) == 0:
            t1 = time.time()
            print(f"Produced {i} messages. Time elapsed: {t1 - t0:.2f} s")
            t0 = t1


def _cosumer_configurations(broker_url):
    return {
        "bootstrap.servers": broker_url,
        "group.id": "A",  # mandatory
    }


async def consume(broker_url, topic, batch_size=1, timeout=0.1, *, partitions):
    c = Consumer(_cosumer_configurations(broker_url))
    c.subscribe([topic])
    consume_func = functools.partial(c.consume, batch_size, timeout=timeout)
    loop = asyncio.get_running_loop()
    try:
        counter = 0
        while True:
            msgs = await loop.run_in_executor(None, consume_func)
            if len(msgs) > 0:
                msg0 = msgs[0]
                assert (msg0.topic() == topic)
                assert (msg0.key() is None)
                assert (0 <= msg0.partition() <= partitions)
                counter += len(msgs)
                print(f"Received {counter} messages, "
                      f"the first message: {json.loads(msg0.value())}")
            await asyncio.sleep(0.001)
    finally:
        c.close()


def consume_sync(broker_url, topic, *, partitions):
    c = Consumer(_cosumer_configurations(broker_url))
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
                      replication_factor=1,  # cannot be larger than No. brokers
                      config={
                          # fast compression and decompression
                          "compression.type": "lz4"})
             ]
        )
        for _, future in futures.items():
            try:
                future.result()
                print("New topic created!")
            except Exception as e:
                pass


if __name__ == "__main__":
    """Checks for topic and creates the topic if it does not exist"""
    parser = argparse.ArgumentParser(description="kafka-producer-consumer")

    parser.add_argument('--sync',
                        action='store_true',
                        help="True for asynchronous and False for synchronous "
                             "producer and consumer")
    parser.add_argument('--produce', type=int, default=None)

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(osp.join(osp.dirname(__file__), 'config.ini'))

    BROKER_URL = config['CLUSTER']['BROKER_URL']
    TOPIC = config['TOPIC1']['NAME']
    PARTITIONS = int(config['TOPIC1']['PARTITIONS'])

    maybe_create_topic(BROKER_URL, TOPIC, partitions=PARTITIONS)
    try:
        if args.sync:
            if args.produce is None:
                consume_sync(BROKER_URL, TOPIC, partitions=PARTITIONS)
            else:
                produce_sync(BROKER_URL, TOPIC, num_messages=args.produce)
        else:
            asyncio.run(asyncio.wait([
                produce(BROKER_URL, TOPIC, num_messages=args.produce),
                consume(BROKER_URL, TOPIC,
                        batch_size=args.produce // 10,
                        partitions=PARTITIONS)
            ]))

    except KeyboardInterrupt as e:
        print("shutting down")
