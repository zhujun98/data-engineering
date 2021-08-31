"""Defines core consumer functionality"""
import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

from ..config import config
from .logger import logger


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
            "group.id": "0"
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
        """Callback to provide handling of customized offsets."""
        if self._offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

        logger.info("Partitions assigned for %s", self._topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic."""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(1.0)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self._consumer.poll(1.)
        if message is None:
            logger.debug("No message received by consumer")
            return 0
        if message.error() is not None:
            logger.error("Error from consumer: %s", message.error())
            return 0
        self._message_handler(message)
        return 1

    def close(self):
        """Close down and terminate the held Kafka consumer."""
        self._consumer.close()
