"""
Defines a time simulation responsible for executing any registered producers.
"""
import datetime
import time
import logging
from logging.config import fileConfig
from pathlib import Path

import pandas as pd

from .connector import configure_connector
from .models import CTALine, Weather

# Import logging before models to ensure configuration is picked up
fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)


class DataSimulator:

    def __init__(self, time_interval: int = 5, num_trains: int = 10):
        """Initialization.

        :param time_interval: Time interval in seconds between two simulations.
        :param num_trains: Number of trains for each CTA line.
        """
        self._time_interval = time_interval

        self._raw_df = pd.read_csv(
            f"{Path(__file__).parents[0]}/data/cta_stations.csv"
        ).sort_values("order")

        # simulated data
        self._cta_lines = [
            CTALine('blue', self._raw_df, num_trains=num_trains),
            CTALine('red', self._raw_df, num_trains=num_trains),
            CTALine('green', self._raw_df, num_trains=num_trains),
        ]
        # REST proxy
        self._weather_station = Weather()
        # kafka connect
        # configure_connector()

    def start(self):
        logger.info("Beginning simulation, press Ctrl+C to exit at any time")
        logger.info("Loading kafka connect jdbc source connector")

        logger.info("Beginning CTA train simulation")
        try:
            while True:
                logger.debug(f"Simulation running: "
                             f"{datetime.datetime.utcnow().isoformat()}")

                self._weather_station.run()

                for line in self._cta_lines:
                    line.run()

                time.sleep(self._time_interval)

        except KeyboardInterrupt as e:
            logger.info("Shutting down ...")
            for line in self._cta_lines:
                line.close()
