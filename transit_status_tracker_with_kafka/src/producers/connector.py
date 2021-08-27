import json

import requests

from ..config import config
from .logger import logger


class Connector:
    """Kafka connector."""
    def __init__(self):
        self._connect_url = config["CONNECTOR"]["KAFKA_CONNECT_URL"]
        self._name = config["CONNECTOR"]["CONNECTOR_NAME"]

    def start(self):
        """Start a kafka JDBC connector.

        Delete the old connector if it already exists.
        """
        connector = f"{self._connect_url}/{self._name}"
        r = requests.get(connector)
        if r.status_code == 200:
            r = requests.delete(connector)
            try:
                r.raise_for_status()
            except Exception as e:
                logger.error("Unexpected error: ", repr(e))
            logger.info("Deleted existing connector!")

        # Caveat: The Docker URL of PostgresDB should be used for
        #         "connection.url" when running with docker-compose in your
        #         local machine.
        r = requests.post(
           self._connect_url,
           headers={"Content-Type": "application/json"},
           data=json.dumps({
               "name": self._name,
               "config": {
                   "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                   "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                   "key.converter.schemas.enable": "false",
                   "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                   "value.converter.schemas.enable": "false",
                   "batch.max.rows": "500",
                   "connection.url": "jdbc:postgresql://postgres:5432/cta",
                   "connection.user": "cta_admin",
                   "connection.password": "chicago",
                   "table.whitelist": "stations",
                   "mode": "incrementing",
                   "incrementing.column.name": "stop_id",
                   "topic.prefix": "connect.",
                   "poll.interval.ms": "10000",
               }
           }),
        )

        try:
            r.raise_for_status()
        except Exception as e:
            logger.critical("Failed when creating the kafka connector: ", repr(e))
            exit(1)

        logger.info("Connector created successfully")
