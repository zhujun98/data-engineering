"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from .producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    class Status(IntEnum):
        SUNNY = 0
        PARTLY_CLOUDY = 1
        CLOUDY = 2
        WINDY = 3
        PRECIPITATION = 4

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = {0, 1, 2, 3, 10, 11}
    summer_months = {6, 7, 8}

    def __init__(self, month):
        super().__init__("weather", # TODO: Come up with a better topic name
                         key_schema=None,
                         value_schema=None)

        self._status = self.Status.SUNNY
        if month in self.winter_months:
            self.temp = 40.0
        elif month in self.summer_months:
            self.temp = 85.0
        else:
            self.temp = 70.0

        if self.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                self.key_schema = json.load(f)

        if self.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                self.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in self.winter_months:
            mode = -1.0
        elif month in self.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self._status = random.choice(list(self.Status))

    def run(self, month):
        self._set_weather(month)

        #
        #
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #
        logger.info("weather kafka proxy integration incomplete - skipping")
        #resp = requests.post(
        #    #
        #    #
        #    # TODO: What URL should be POSTed to?
        #    #
        #    #
        #    f"{Weather.rest_proxy_url}/TODO",
        #    #
        #    #
        #    # TODO: What Headers need to bet set?
        #    #
        #    #
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
        #)
        #resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self._status.name,
        )
