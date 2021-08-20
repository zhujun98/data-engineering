"""Producer base-class providing common utilites and functionality"""
import logging
import socket
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self, topic_name, key_schema,
                 value_schema=None,
                 num_partitions=1,
                 num_replicas=1):
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_url = "PLAINTEXT://localhost:9092"
        self.broker_properties = {
            "bootstrap.servers": self.broker_url,
            "client.id": socket.gethostname()
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in self.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties, schema_registry="http://localhost:8081"
        )

    def create_topic(self):
        """Creates the producer topic."""
        client = AdminClient({"bootstrap.servers": self.broker_url})
        # TODO: maybe add config for NewTopic
        future = client.create_topics([
            NewTopic(topic=self.topic_name,
                     num_partitions=self.num_partitions,
                     replication_factor=self.num_replicas)
        ])
        try:
            future.result()
            logger.info(f"New topic created: {self.topic_name}")
        except Exception as e:
            logger.error(f"Failed to create topic: {self.topic_name}")

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
