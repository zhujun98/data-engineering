import json
import logging

import requests

from ..config import config
from .logger import logger
from ..utils import topic_exists


# Caveat: Don't use double quotes within a ksql statement.
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT
) WITH (
    KAFKA_TOPIC=config['TOPIC']['TURNSTILE'],
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT='JSON'
) AS
    SELECT station_id, COUNT(*) AS count 
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_exists("TURNSTILE_SUMMARY"):
        return

    logger.info("Executing ksql statement...")

    resp = requests.post(
        f"{config['KSQL']['URL']}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {
                    "ksql.streams.auto.offset.reset": "earliest"
                },
            }
        ),
    )

    try:
        resp.raise_for_status()
    except Exception as e:
        logger.fatal("Failed to start ksql stream processing!", repr(e))


if __name__ == "__main__":
    execute_statement()
