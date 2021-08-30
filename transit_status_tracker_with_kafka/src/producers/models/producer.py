import abc
import socket
import time

from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

from ...config import config
from ...utils import client, create_topic
from ..logger import logger


class Producer:
    """Defines and provides common functionality amongst Producers"""

    _existing_topics = set()

    def __init__(self, topic_name, key_schema, value_schema,
                 num_partitions=1, num_replicas=1):
        self._broker_url = config['KAFKA']['BROKER_URL']
        self._schema_registry_url = config['KAFKA']['SCHEMA_REGISTRY_URL']
        self._topic_name = topic_name
        self._key_schema = key_schema
        self._value_schema = value_schema
        self._num_partitions = num_partitions
        self._num_replicas = num_replicas

        self._maybe_create_topic()

        conf = {
            "bootstrap.servers": self._broker_url,
            "client.id": socket.gethostname()
            # TODO
        }
        schema_registry = CachedSchemaRegistryClient(
            {"url": self._schema_registry_url})
        self._producer = AvroProducer(conf, schema_registry=schema_registry)

    def _maybe_create_topic(self):
        """Creates the producer topic if it does not exist."""
        if not self._existing_topics:
            # Retrieve the existing topics only once in the initialization step.
            self._existing_topics.update([
                v.topic for v in client.list_topics(timeout=5).topics.values()])
            logger.info("Found existing topics: %s", self._existing_topics)

        if self._topic_name in self._existing_topics:
            return

        try:
            create_topic(self._topic_name,
                         num_partitions=self._num_partitions,
                         num_replicas=self._num_replicas)
            self._existing_topics.add(self._topic_name)
            logger.info("New topic created: %s", self._topic_name)
        except Exception as e:
            logger.fatal("Failed to create topic '%s': %s",
                         self._topic_name, repr(e))
            exit(1)

    @abc.abstractmethod
    def run(self):
        pass

    def close(self):
        """Prepare to exit by cleaning up the producer."""
        self._producer.flush()

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events."""
        return int(round(time.time() * 1000))
