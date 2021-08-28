import faust

from ..config import config


# Faust will ingest records from Kafka in this format
class Station(faust.Record, validation=True, serializer="json"):
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
class TransformedStation(faust.Record, validation=True, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=config['KAFKA']['BROKER_ENDPOINT'])

topic = app.topic(config['TOPIC']['STATION_RAW'], value_type=Station)

out_topic_name = config['TOPIC']['STATION_TABLE']
out_topic = app.topic(out_topic_name, partitions=1)

table = app.Table(
   out_topic_name,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic
)


@app.agent(topic)
async def process(stream):
    async for v in stream:
        if v.red:
            line = 'red'
        elif v.blue:
            line = 'blue'
        elif v.green:
            line = 'green'
        else:
            # There are stations which do not belong to any of the above lines.
            line = None

        table[v.station_id] = TransformedStation(
            v.station_id, v.station_name, v.order, line
        )
