"""Defines core consumer functionality"""
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from tornado import gen

from ..config import config
from .logger import logger


class KafkaConsumer:
    def __init__(self,
                 topic_name_pattern,
                 message_handler,
                 is_avro=True,
                 offset_earliest=True):
        """Creates a consumer object for asynchronous use"""
        self._topic_name_pattern = topic_name_pattern
        self._message_handler = message_handler
        self._offset_earliest = offset_earliest

        self._time_interval = float(
            config["PARAM"]["CONSUMER_POLL_TIME_INTERVAL"])

        conf = {
            "bootstrap.servers": config["KAFKA"]["BROKER_URL"],
            "group.id": "0",
            "auto.offset.reset": "earliest"
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
        # TODO: Improve
        while True:
            while True:
                message = self._consumer.poll(self._time_interval)
                if message is None:
                    logger.debug("No message received by consumer")
                    break
                elif message.error() is not None:
                    logger.error("Error from consumer: %s", message.error())
                    break
                else:
                    self._message_handler(message)

            await gen.sleep(self._time_interval)

    def close(self):
        """Close down and terminate the held Kafka consumer."""
        self._consumer.close()
