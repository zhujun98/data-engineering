import configparser

from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient


def consume(broker_url, schema_registry_url):
    schema_registry = CachedSchemaRegistryClient({"url": schema_registry_url})
    conf = {
        "bootstrap.servers": broker_url,
        "group.id": "0",
        "auto.offset.reset": "earliest"
    }

    consumer = AvroConsumer(conf, schema_registry=schema_registry)
    consumer.subscribe(["^tracking.*"])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("No message received")
        else:
            print(msg.value())


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    BROKER_URL = config['KAFKA']['BROKER_URL']
    SCHEMA_REGISTRY_URL = config['KAFKA']['SCHEMA_REGISTRY_URL']

    consume(BROKER_URL, SCHEMA_REGISTRY_URL)
