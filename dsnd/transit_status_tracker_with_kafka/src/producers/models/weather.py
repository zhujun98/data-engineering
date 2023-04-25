import asyncio
from enum import IntEnum
import json
from pathlib import Path
import random

import requests

from ...config import config
from ..logger import logger
from .producer import Producer
from .timer import timer


class Weather(Producer):
    """Defines a simulated weather station."""

    class Status(IntEnum):
        SUNNY = 0
        PARTLY_CLOUDY = 1
        CLOUDY = 2
        WINDY = 3
        PRECIPITATION = 4

    with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as fp:
        # Load schema from file and convert it to a str.
        key_schema = json.dumps(json.load(fp))
    with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as fp:
        value_schema = json.dumps(json.load(fp))

    _avg_monthly_temp = [25.2, 26.4, 34.8, 46.0, 58.1, 69.2,
                         74.8, 73.7, 67.3, 54.8, 42.6, 31.1]

    def __init__(self):
        topic_name = config["TOPIC"]["WEATHER"]
        super().__init__(topic_name,
                         key_schema=self.key_schema,
                         value_schema=self.value_schema)

        self._status = None
        self._temp = None

        self._time_interval = float(
            config['PARAM']['TIMER_UPDATE_TIME_INTERVAL'])

    def _update_status(self):
        self._temp = self._avg_monthly_temp[timer.month] + random.uniform(-3, 3)
        self._status = random.choice(list(self.Status))

    async def run(self):
        """Override."""
        rest_proxy_url = config['KAFKA']['REST_PROXY_URL']
        while True:
            self._update_status()
            # TODO: run_in_executor
            r = requests.post(
               f"{rest_proxy_url}/topics/{self._topic_name}",
               headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
               data=json.dumps({
                   "key_schema": self.key_schema,
                   "value_schema": self.value_schema,
                   "records": [{
                        "key": {
                            "timestamp": self.time_millis()
                        },
                        "value": {
                            "temperature": self._temp,
                            "status": self._status.name
                        },
                    }]
                })
            )
            try:
                r.raise_for_status()
            except Exception as e:
                logger.error("Failed when posting weather data: %s", repr(e))

            logger.info("Update weather - temp: %s, status: %s",
                        self._temp, self._status.name)

            await asyncio.sleep(self._time_interval)
