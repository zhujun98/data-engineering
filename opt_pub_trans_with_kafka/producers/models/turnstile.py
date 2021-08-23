"""Creates a turnstile data producer"""
import datetime
import logging
from pathlib import Path
import math
import random

import pandas as pd
from confluent_kafka import avro

from .producer import Producer

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = None
    value_schema = None
    # key_schema = avro.load(
    #     f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    #
    # value_schema = avro.load(
    #    f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    # )

    curve_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_curve.csv"
    )
    seed_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_seed.csv"
    )

    def __init__(self, station):
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(f"{station_name}", # TODO: Come up with a better topic name
                         key_schema=self.key_schema,
                         value_schema=self.value_schema,
                         num_partitions=1,
                         num_replicas=1)
        self.station = station

        self.metrics_df = self.seed_df[
            self.seed_df["station_id"] == station.station_id
        ]
        self.weekday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_weekday_rides"])
        )
        self.saturday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_saturday_rides"])
        )
        self.sunday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_sunday-holiday_rides"])
        )

    def _get_entries(self, curr_datetime, time_step):
        """Returns the number of turnstile entries for the given timeframe."""
        hour_curve = self.curve_df[self.curve_df["hour"] == curr_datetime.hour]
        ratio = hour_curve.iloc[0]["ridership_ratio"]
        total_steps = int(60 / (60 / time_step.total_seconds()))

        num_riders = 0
        dow = curr_datetime.weekday()
        if dow >= 0 or dow < 5:
            num_riders = self.weekday_ridership
        elif dow == 6:
            num_riders = self.saturday_ridership
        else:
            num_riders = self.sunday_ridership

        # Calculate approximation of number of entries for this simulation step
        num_entries = int(math.floor(num_riders * ratio / total_steps))
        # Introduce some randomness in the data
        return max(num_entries + random.choice(range(-5, 5)), 0)

    def run(self, curr_datetime: datetime.datetime,
            time_step: datetime.timedelta):
        """Simulates riders entering through the turnstile."""
        num_entries = self._get_entries(curr_datetime, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
