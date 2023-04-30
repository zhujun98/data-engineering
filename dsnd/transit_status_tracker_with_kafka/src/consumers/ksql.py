"""
Process the turnstile data stream with ksqlDB.
"""
import json

import requests

from ..config import config
from ..utils import topic_exists
from .logger import logger


topic = config['TOPIC']['TURNSTILE']
turnstile_table = config['TOPIC']['TURNSTILE_TABLE']

# Caveat: 1. Don't use double quotes within a ksql statement.
#         2. One needs to TERMINATE the ksql query (SHOW QUERIES) before
#            dropping the created tables.
# TODO: why the second table will be turned into a Kafka topic automatically?
KSQL_STATEMENT = (f"""
DROP TABLE IF EXISTS TURNSTILE;

DROP TABLE IF EXISTS {turnstile_table} DELETE TOPIC;

CREATE TABLE TURNSTILE (
    station_id INTEGER,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{topic}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE {turnstile_table}
WITH (
    VALUE_FORMAT='JSON'
) AS
    SELECT line, station_id, COUNT(*) AS count
    FROM TURNSTILE
    GROUP BY line, station_id;
""")


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_exists(turnstile_table):
        logger.info("Table '%s' already exists!", turnstile_table)
        return

    logger.info("Executing ksql statement ...")

    r = requests.post(
        f"{config['KSQL']['URL']}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {
                    "ksql.streams.auto.offset.reset": "earliest"
                }
            }
        ),
    )

    try:
        r.raise_for_status()
        logger.info("Table '%s' created!", turnstile_table)
    except Exception:
        logger.fatal("Failed to execute ksql statement: %s, %s",
                     r.reason, r.text)
        exit(1)
