import asyncio
from pathlib import Path
import random

import pandas as pd
from confluent_kafka import avro

from ...config import config
from ..logger import logger
from .producer import Producer
from .timer import timer


class Turnstile(Producer):
    """Defines a turnstile in a train station."""

    key_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    curve_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_curve.csv"
    )
    seed_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_seed.csv"
    )

    def __init__(self, station_id: int, station_name: str, color: str):
        super().__init__(config["TOPIC"]["TURNSTILE"],
                         key_schema=self.key_schema,
                         value_schema=self.value_schema,
                         num_partitions=1,
                         num_replicas=1)

        self._station_name = station_name
        self._station_id = station_id
        self._color = color

        self._metrics_df = self.seed_df[
            self.seed_df["station_id"] == station_id]
        self._weekday_ridership = int(
            round(self._metrics_df.iloc[0]["avg_weekday_rides"])
        )
        self._saturday_ridership = int(
            round(self._metrics_df.iloc[0]["avg_saturday_rides"])
        )
        self._sunday_ridership = int(
            round(self._metrics_df.iloc[0]["avg_sunday-holiday_rides"])
        )

        self._steps_per_hour = \
            float(config['PARAM']['TIMER_UPDATE_TIME_INTERVAL']) / \
            float(config['PARAM']['CTA_LINE_UPDATE_INTERVAL'])

    def _get_entries(self):
        """Returns the number of turnstile entries."""
        dow = timer.weekday
        if dow >= 0 or dow < 5:
            num_riders = self._weekday_ridership
        elif dow == 6:
            num_riders = self._saturday_ridership
        else:
            num_riders = self._sunday_ridership

        hour_curve = self.curve_df[self.curve_df["hour"] == timer.hour]
        hour_ratio = hour_curve.iloc[0]["ridership_ratio"]

        num_entries = num_riders * hour_ratio / self._steps_per_hour
        num_entries *= random.uniform(0.8, 1.2)
        return int(num_entries)

    async def _produce(self):
        self._producer.produce(
            topic=self._topic_name,
            key={"timestamp": self.time_millis()},
            key_schema=self._key_schema,
            value={
                "station_id": self._station_id,
                "station_name": self._station_name,
                "line": self._color
            },
            value_schema=self._value_schema
        )

    async def run(self):
        """Override."""
        n_entries = self._get_entries()
        ret = asyncio.create_task(asyncio.sleep(0))
        if n_entries > 0:
            ret = asyncio.gather(*[asyncio.create_task(self._produce())
                                 for _ in range(n_entries)])

        logger.debug(f"{n_entries} entries in {self._station_name}")
        return ret
