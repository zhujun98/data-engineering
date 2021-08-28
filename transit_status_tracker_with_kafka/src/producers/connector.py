import json

import requests

from ..config import config
from .logger import logger


class PostgresConnector:
    """Kafka Postgres connector."""
    def __init__(self):
        self._url = config["KAFKA"]["CONNECT_URL"] + "/connectors"

        self._name = config["CONNECTOR"]["NAME"]
        self._topic_prefix = config["CONNECTOR"]["PREFIX"]

        self._dbname = config['POSTGRES']['DBNAME']
        self._username = config['POSTGRES']['USERNAME']
        self._password = config['POSTGRES']['PASSWORD']
        self._endpoint = config['POSTGRES']['ENDPOINT']

    def start(self):
        """Start a kafka JDBC connector.

        Delete the old connector if it already exists.
        """
        connector = f"{self._url}/{self._name}"
        r = requests.get(connector)
        if r.status_code == 200:
            logger.info(f"Connector already exists: {self._name}")
            return

        # Caveat: The Docker URL of PostgresDB should be used for
        #         "connection.url" when running with docker-compose in your
        #         local machine.
        r = requests.post(
           self._url,
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
                   "connection.url": f"jdbc:postgresql://{self._endpoint}/{self._dbname}",
                   "connection.user": self._username,
                   "connection.password": self._password,
                   "table.whitelist": "stations",
                   "mode": "incrementing",
                   "incrementing.column.name": "stop_id",
                   "topic.prefix": self._topic_prefix,
                   "poll.interval.ms": "10000",
               }
           }),
        )

        try:
            r.raise_for_status()
        except Exception as e:
            logger.critical(
                "Failed when creating the kafka connector: ", repr(e))
            exit(1)

        logger.info(f"Connector created successfully: {self._name}")
