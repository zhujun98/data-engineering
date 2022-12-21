import argparse
import asyncio
import configparser
from dataclasses import asdict, dataclass, field
import os.path as osp
import socket
import random

from confluent_kafka.avro import (
    AvroConsumer, AvroProducer, CachedSchemaRegistryClient, loads
)
from faker import Faker


@dataclass
class ClickAttribute:
    _faker = Faker()

    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=_faker.bs)

    @classmethod
    def attributes(cls):
        return {cls._faker.uri_page(): ClickAttribute()
                for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    _faker = Faker()

    email: str = field(default_factory=_faker.email)
    timestamp: str = field(default_factory=_faker.iso8601)
    uri: str = field(default_factory=_faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    schema = loads("""{
        "type": "record",
        "name": "click_event",
        "namespace": "click",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"},
            {
                "name": "attributes",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "attribute",
                        "fields": [
                            {"name": "element", "type": "string"},
                            {"name": "content", "type": "string"}
                        ]
                    }
                }
            }
        ]
    }""")


async def produce(broker_url, topic, schema_registry_url, *, num_messages):
    schema_registry = CachedSchemaRegistryClient({"url": schema_registry_url})
    conf = {
        "bootstrap.servers": broker_url,
        "client.id": socket.gethostname()
    }
    p = AvroProducer(conf, schema_registry=schema_registry)
    for _ in range(num_messages):
        p.produce(
            topic=topic,
            value=asdict(ClickEvent()),
            value_schema=ClickEvent.schema
        )
        await asyncio.sleep(0.01)


async def consume(broker_url, topic, schema_registry_url):
    schema_registry = CachedSchemaRegistryClient({"url": schema_registry_url})
    conf = {
        "bootstrap.servers": broker_url,
        "group.id": "A"
    }
    c = AvroConsumer(conf, schema_registry=schema_registry)
    c.subscribe([topic])
    while True:
        message = c.poll(1.)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(0.01)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="kafka-producer-consumer")

    parser.add_argument('--produce', type=int, default=1000)

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(osp.join(osp.dirname(__file__), 'config.ini'))

    BROKER_URL = config['CLUSTER']['BROKER_URL']
    SCHEMA_REGISTRY_URL = config['CLUSTER']['SCHEMA_REGISTRY_URL']

    # Note: Topic will be automatically create if not existing yet.
    TOPIC = config['TOPIC2']['NAME']

    asyncio.run(asyncio.wait([
        produce(BROKER_URL, TOPIC, SCHEMA_REGISTRY_URL, num_messages=args.produce),
        consume(BROKER_URL, TOPIC, SCHEMA_REGISTRY_URL)
    ]))
