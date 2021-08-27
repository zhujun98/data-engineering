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

connector_name = config['KAFKA']['CONNECTOR_NAME']
topic_prefix = config['KAFKA']['CONNECTOR_TOPIC_PREFIX']

in_topic = app.topic(f"{topic_prefix}{connector_name}",
                     value_type=Station)

out_topic_name = f"{topic_prefix}.{connector_name}.table"
out_topic = app.topic(out_topic_name, partitions=1)

table = app.Table(
   out_topic_name,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic
)


@app.agent(in_topic)
async def process(stream):
    async for v in stream:
        if v.red:
            line = 'red'
        elif v.blue:
            line = 'blue'
        elif v.green:
            line = 'green'
        else:
            raise RuntimeError

        table[v.station_id] = TransformedStation(
            v.station_id, v.station_name, v.order, line
        )
