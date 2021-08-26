import datetime
import time
from pathlib import Path

import pandas as pd

from ..config import config
from .connector import configure_connector
from .logger import logger
from .models import CTALine, Weather


TIME_INTERVAL = int(config['SIMULATOR']['TIME_INTERVAL'])
NUM_TRAINS = int(config["SIMULATOR"]["NUM_TRAINS"])


class DataSimulator:

    def __init__(self, num_trains: int = None):
        """Initialization.

        :param num_trains: Number of trains for each CTA line.
        """
        self._time_interval = TIME_INTERVAL

        self._raw_df = pd.read_csv(
            f"{Path(__file__).parents[0]}/data/cta_stations.csv"
        ).sort_values("order")

        # simulated data
        if num_trains is None:
            num_trains = NUM_TRAINS
        self._cta_lines = [CTALine(c, self._raw_df, num_trains=num_trains)
                           for c in ('blue', 'red', 'green')]

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
