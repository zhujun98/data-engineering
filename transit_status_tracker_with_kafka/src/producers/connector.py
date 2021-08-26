import json
import logging

import requests

from ..config import config
from .logger import logger

KAFKA_CONNECT_URL = config["CLUSTER"]["KAFKA_CONNECT_URL"]
CONNECTOR_NAME = "stations"


def create_jdbc_connector():
    """Start a kafka JDBC connector.

    Delete the old connector if it already exists.
    """
    connector = f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}"
    r = requests.get(connector)
    if r.status_code == 200:
        requests.delete(connector)
        try:
            r.raise_for_status()
        except Exception as e:
            logger.error("Unexpected error: ", repr(e))
        logger.info("Deleted existing connector!")

    r = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://localhost:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "connect-",
               "poll.interval.ms": "5000",
           }
       }),
    )

    try:
        r.raise_for_status()
    except Exception as e:
        logger.critical("Failed when creating the kafka connector: ", repr(e))
        exit(1)

    logger.info("Connector created successfully")
