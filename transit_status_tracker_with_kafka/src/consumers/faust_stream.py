import faust

from ..config import config
from .logger import logger

BROKER_URL = config['CLUSTER']['BROKER_URL']


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=BROKER_URL, store="memory://")

topic = app.topic("TODO", value_type=Station)

output_topic = app.topic("TODO", partitions=1)

table = app.Table(
   # "TODO",
   # default=TODO,
   partitions=1,
   changelog_topic=output_topic
)

#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#


if __name__ == "__main__":
    app.main()
