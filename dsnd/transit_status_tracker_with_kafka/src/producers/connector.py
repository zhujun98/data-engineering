"""
Load the station data from postgresDB with Kafka JDBC connector.
"""
import json

import requests

from ..config import config
from ..utils import topic_exists
from .logger import logger


class PostgresConnector:
    """Kafka Postgres connector."""
    def __init__(self):
        self._url = config["KAFKA"]["CONNECT_URL"] + "/connectors"

        self._topic_name = config["TOPIC"]["STATION_RAW"]
        topic_name_splitted = self._topic_name.split(".")
        self._name = topic_name_splitted[-1]
        self._topic_prefix = ".".join(topic_name_splitted[:-1]) + "."

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
            logger.info("Connector '%s' already exists", connector)
            if not topic_exists(self._topic_name):
                logger.fatal("Topic '%s' not found! Delete and recreate the "
                             "connector.", self._topic_name)
                exit(1)
            return

        # Caveat: 1. The Docker URL of PostgresDB should be used for
        #         "connection.url" when running with docker-compose in your
        #         local machine.
        #         2. The topic (prefix + table name) must be created by
        #         Kafka connect. Namely, one cannot create a topic with the
        #         same name beforehand. In addition, automatically creating
        #         topic in Kafka must be enabled.
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
                   "table.whitelist": self._name,
                   # In practice it should be set to "incrementing". However,
                   # it is convenient to set "bulk" here for debugging and
                   # monitoring since the postgresDB never changes.
                   "mode": "bulk",
                   "incrementing.column.name": "stop_id",
                   "topic.prefix": self._topic_prefix,
                   # In practice it can be as high as one hour.
                   "poll.interval.ms": "60000",
               }
           }),
        )

        try:
            r.raise_for_status()
            logger.info("Connector '%s' created", connector)
        except Exception:
            logger.fatal("Failed when creating the kafka connector: %s, %s",
                         r.reason, r.text)
            exit(1)
