import logging
import math
from pathlib import Path
import random

import pandas as pd

logger = logging.getLogger(__name__)


class TurnstileHardware:
    curve_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_curve.csv"
    )
    seed_df = pd.read_csv(
        f"{Path(__file__).parents[1]}/data/ridership_seed.csv"
    )

    def __init__(self, station):
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

    def get_entries(self, timestamp, time_step):
        """Returns the number of turnstile entries for the given timeframe."""
        hour_curve = self.curve_df[self.curve_df["hour"] == timestamp.hour]
        ratio = hour_curve.iloc[0]["ridership_ratio"]
        total_steps = int(60 / (60 / time_step.total_seconds()))

        num_riders = 0
        dow = timestamp.weekday()
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
