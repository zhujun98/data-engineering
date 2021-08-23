"""Producer base-class providing common utilites and functionality"""
import configparser
import logging
from pathlib import Path
import socket
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read(Path(__file__).parents[0].joinpath('..', 'config.ini'))

BROKER_URL = config['CLUSTER']['BROKER_URL']
SCHEMA_REGISTRY_URL = config['CLUSTER']['SCHEMA_REGISTRY_URL']


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self, topic_name, key_schema,
                 value_schema=None,
                 num_partitions=1,
                 num_replicas=1):
        self._topic_name = topic_name
        self._key_schema = key_schema
        self._value_schema = value_schema
        self._num_partitions = num_partitions
        self._num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "client.id": socket.gethostname()
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self._topic_name not in self.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self._topic_name)

        self.producer = AvroProducer(
            self.broker_properties, schema_registry=SCHEMA_REGISTRY_URL
        )

    def create_topic(self):
        """Creates the producer topic."""
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        # TODO: maybe add config for NewTopic
        future = client.create_topics([
            NewTopic(topic=self._topic_name,
                     num_partitions=self._num_partitions,
                     replication_factor=self._num_replicas)
        ])
        try:
            future.result()
            logger.info(f"New topic created: {self._topic_name}")
        except Exception as e:
            logger.error(f"Failed to create topic: {self._topic_name}")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
