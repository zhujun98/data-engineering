import time
from pathlib import Path

import pandas as pd

from ..config import config
from .connector import PostgresConnector
from .logger import logger
from .models import CTALine, Weather


class StreamSimulation:

    def __init__(self, num_trains: int = None):
        """Initialization.

        :param num_trains: Number of trains for each CTA line.
        """
        self._time_interval = int(config['SIMULATOR']['TIME_INTERVAL'])

        self._raw_df = pd.read_csv(
            f"{Path(__file__).parents[0]}/data/cta_stations.csv"
        ).sort_values("order")

        # simulated train and turnstile data via AvroProducer
        if num_trains is None:
            num_trains = int(config["SIMULATOR"]["NUM_TRAINS"])
        self._cta_lines = [CTALine(c, self._raw_df, num_trains=num_trains)
                           for c in ('blue', 'red', 'green')]

        # simulated weather data via REST proxy
        self._weather_station = Weather()

        # kafka JDBC connector
        self._connector = PostgresConnector()

    def start(self):
        logger.info("Beginning simulation, press Ctrl+C to exit at any time")

        self._connector.start()

        try:
            while True:
                self._weather_station.run()

                for line in self._cta_lines:
                    line.run()

                time.sleep(self._time_interval)

        except KeyboardInterrupt as e:
            logger.info("Shutting down ...")
            for line in self._cta_lines:
                line.close()
