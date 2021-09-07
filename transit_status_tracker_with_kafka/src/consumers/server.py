"""Defines a Tornado Server that consumes Kafka Event data for display"""
from dataclasses import dataclass, field
import json
from pathlib import Path

import tornado.ioloop
import tornado.template
import tornado.web

from ..config import config
from ..utils import topic_exists
from .consumer import KafkaConsumer
from .logger import logger


@dataclass
class Weather:
    temperature: float
    status: str


@dataclass
class Line:
    color_code: str
    stations: dict = field(default_factory=dict)


@dataclass
class Train:
    id: str
    status: str

@dataclass
class Station:
    id: int
    name: str
    order: int
    direction_a: Train = None
    direction_b: Train = None
    turnstile_entries: int = 0


weather = Weather(-1, "unknown")
lines = {
    "red": Line("#DC143C"),
    "blue": Line("#1E90FF"),
    "green": Line("#32CD32")
}


def process_weather(msg):
    value = msg.value()
    weather.temperature = value['temperature']
    weather.status = value['status']


def process_station_table(msg):
    value = json.loads(msg.value())
    line = value["line"]
    station_id = value["station_id"]
    if line in lines and station_id not in lines[line].stations:
        lines[line].stations[station_id] = Station(
            station_id, value["station_name"], value["order"]
        )


def process_turnstile(msg):
    value = json.loads(msg.value())
    # Caveat: keys are all capital letters from KSQL
    line = value["LINE"]
    try:
        lines[line].stations[value["STATION_ID"]].turnstile_entries += value["COUNT"]
    except KeyError:
        pass


def process_arrival(msg):
    value = msg.value()
    line = value["line"]

    try:
        # handle departure
        prev_station = lines[line].stations[value["prev_station_id"]]
        if value["prev_direction"] == "a":
            prev_station.direction_a = None
        else:
            prev_station.direction_b = None

        # handle arrival
        station = lines[line].stations[value["station_id"]]
        train = Train(value["train_id"], value["train_status"])
        if value["direction"] == "a":
            station.direction_a = train
        else:
            station.direction_b = train
    except KeyError:
        pass


class MainHandler(tornado.web.RequestHandler):

    loader = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template = loader.load("status.html")

    def get(self):
        """Responds to get requests"""
        logger.debug("rendering and writing handler template")
        self.write(self.template.generate(weather=weather, lines=lines))


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if not topic_exists(config["TOPIC"]["TURNSTILE_TABLE"]):
        logger.fatal("Ensure that the KSQL Command has run successfully "
                     "before running the web server!")
        exit(1)
    if not topic_exists(config["TOPIC"]["STATION_TABLE"]):
        logger.fatal("Ensure that Faust Streaming is running successfully "
                     "before running the web server!")
        exit(1)

    # Build kafka consumers
    consumers = [
        KafkaConsumer(
            config["TOPIC"]["WEATHER"],
            process_weather,
        ),
        KafkaConsumer(
            config["TOPIC"]["STATION_TABLE"],
            process_station_table,
            is_avro=False,
        ),
        KafkaConsumer(
            config['TOPIC']['ARRIVAL'],
            process_arrival,
        ),
        KafkaConsumer(
            config['TOPIC']['TURNSTILE_TABLE'],
            process_turnstile,
            is_avro=False,
        )
    ]

    # a wrapper around asyncio event loop
    loop = tornado.ioloop.IOLoop.current()

    application = tornado.web.Application(
        [(r"/", MainHandler)], debug=True
    )
    application.listen(8888)

    try:
        logger.info("Open a web browser to http://localhost:8888 "
                    "to see the Transit Status Page")
        for consumer in consumers:
            loop.add_callback(consumer.run)
        loop.start()
    except KeyboardInterrupt:
        logger.info("Shutting down server ...")
        loop.stop()
        for consumer in consumers:
            consumer.close()
