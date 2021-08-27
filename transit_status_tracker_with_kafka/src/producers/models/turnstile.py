import datetime
from pathlib import Path
import math
import random

import pandas as pd
from confluent_kafka import avro

from ...config import config
from ..logger import logger
from .producer import Producer
from .utils import normalize_station_name


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
        # <classification>.<station name>
        topic_name = f"turnstile.{normalize_station_name(station_name)}"
        super().__init__(topic_name,
                         key_schema=self.key_schema,
                         value_schema=self.value_schema,
                         num_partitions=1,
                         num_replicas=1)

        self._station_name = station_name
        self._station_id = station_id
        self._color = color

        self.metrics_df = self.seed_df[self.seed_df["station_id"] == station_id]
        self._weekday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_weekday_rides"])
        )
        self._saturday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_saturday_rides"])
        )
        self._sunday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_sunday-holiday_rides"])
        )

    def _get_entries(self):
        """Returns the number of turnstile entries for the given timeframe."""
        curr_datetime = datetime.datetime.utcnow()
        time_step = datetime.timedelta(
            int(config['SIMULATOR']['TIME_INTERVAL']))

        hour_curve = self.curve_df[self.curve_df["hour"] == curr_datetime.hour]
        ratio = hour_curve.iloc[0]["ridership_ratio"]
        total_steps = int(60 / (60 / time_step.total_seconds()))

        dow = curr_datetime.weekday()
        if dow >= 0 or dow < 5:
            num_riders = self._weekday_ridership
        elif dow == 6:
            num_riders = self._saturday_ridership
        else:
            num_riders = self._sunday_ridership

        # Calculate approximation of number of entries for this simulation step
        num_entries = int(math.floor(num_riders * ratio / total_steps))
        # Introduce some randomness in the data
        return max(num_entries + random.choice(range(-5, 5)), 0)

    def run(self):
        """Override."""
        n_entries = self._get_entries()
        for _ in range(n_entries):
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

            logger.info(f"{n_entries} entries in {self._station_name}")
