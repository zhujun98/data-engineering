"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

from ..config import config


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(self,
                 topic_name_pattern,
                 message_handler,
                 is_avro=True,
                 offset_earliest=False):
        """Creates a consumer object for asynchronous use"""
        self._topic_name_pattern = topic_name_pattern
        self._message_handler = message_handler
        self._offset_earliest = offset_earliest

        conf = {
            "bootstrap.servers": config["KAFKA"]["BROKER_URL"],
            "group.id": "udacity"
        }
        if is_avro is True:
            schema_registry = CachedSchemaRegistryClient(
                {"url": config["KAFKA"]["SCHEMA_REGISTRY_URL"]})
            self._consumer = AvroConsumer(conf, schema_registry=schema_registry)
        else:
            self._consumer = Consumer(conf)

        self._consumer.subscribe(
            [self._topic_name_pattern], on_assign=self._on_assign)

    def _on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place."""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            pass

        logger.info("partitions assigned for %s", self._topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic."""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(1.0)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        logger.info("_consume is incomplete - skipping")
        return 0

    def close(self):
        """Close down and terminate the held Kafka consumer."""
        self._consumer.close()
