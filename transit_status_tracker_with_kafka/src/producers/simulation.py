import asyncio
from pathlib import Path

import pandas as pd

from ..config import config
from .connector import PostgresConnector
from .logger import logger
from .models import CTALine, Weather, timer


class StreamSimulation:

    def __init__(self, num_trains: int = None):
        """Initialization.

        :param num_trains: Number of trains for each CTA line.
        """
        self._raw_df = pd.read_csv(
            f"{Path(__file__).parents[0]}/data/cta_stations.csv"
        ).sort_values("order")

        # simulated train and turnstile data via AvroProducer
        if num_trains is None:
            num_trains = int(config["PARAM"]["NUM_TRAINS"])
        self._cta_lines = [
            CTALine(c, self._raw_df, num_trains=num_trains)
            for c in ('blue', 'red', 'green')
        ]

        # simulated weather data via REST proxy
        self._weather_station = Weather()

        # kafka JDBC connector
        self._connector = PostgresConnector()

    def start(self):
        logger.info("Beginning simulation, press Ctrl+C to exit at any time")

        self._connector.start()

        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.gather(*[
                timer.run(),
                self._weather_station.run(),
                *[line.run() for line in self._cta_lines]
            ]))
        except KeyboardInterrupt:
            logger.info("Shutting down ...")
            self._weather_station.close()
            for line in self._cta_lines:
                line.close()
