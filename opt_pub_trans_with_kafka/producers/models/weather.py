import configparser
from enum import IntEnum
import json
from pathlib import Path
import datetime
import random
import urllib.parse

import requests

from .producer import Producer

config = configparser.ConfigParser()
config.read(Path(__file__).parents[2].joinpath('config.ini'))
REST_PROXY_URL = config["CLUSTER"]["REST_PROXY_URL"]


class Weather(Producer):
    """Defines a simulated weather station."""

    class Status(IntEnum):
        SUNNY = 0
        PARTLY_CLOUDY = 1
        CLOUDY = 2
        WINDY = 3
        PRECIPITATION = 4

    with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as fp:
        key_schema = json.load(fp)
    with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as fp:
        value_schema = json.load(fp)

    winter_months = {0, 1, 2, 3, 10, 11}
    summer_months = {6, 7, 8}

    def __init__(self):
        topic_name = "weather.weather"
        super().__init__(topic_name,
                         key_schema=self.key_schema,
                         value_schema=self.value_schema)

        self._status = None
        self._temp = None

        self._initialized = False

    def _update_status(self):
        month = datetime.datetime.now().month
        if not self._initialized:
            if month in self.winter_months:
                self._temp = 40.0
            elif month in self.summer_months:
                self._temp = 85.0
            else:
                self._temp = 70.0

            self._status = random.choice(list(self.Status))

            self._initialized = True

        # This algorithm is from the original code ...
        mode = 0.0
        if month in self.winter_months:
            mode = -1.0
        elif month in self.summer_months:
            mode = 1.0
        self._temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)),
                          100.0)

        self._status = random.choice(list(self.Status))

    def run(self):
        """Override."""
        self._update_status()

        # resp = requests.post(
        #    f"{REST_PROXY_URL}/TODO",
        #    headers={"Content-Type": "TODO"},
        #    data=json.dumps(
        #        {
        #            #
        #            #
        #            # TODO: Provide key schema, value schema, and records
        #            #
        #            #
        #        }
        #    ),
        # )
        # resp.raise_for_status()

        # logger.debug(
        #     f"sent weather data to kafka, "
        #     f"temp: {self._temp}, status: {self._status.name}",
        # )
