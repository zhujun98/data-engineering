import asyncio
from pandas import DataFrame
import time

from ...config import config
from ..logger import logger
from .station import Station
from .train import Train


class CTALine:
    """Chicago Transit Authority (CTA) 'L' (Train) system."""

    _colors = frozenset(['red', 'blue', 'green'])

    def __init__(self, color: str, station_df: DataFrame, num_trains: int):
        self._color = color.lower()
        if self._color not in self._colors:
            raise ValueError(f"CTALine color must be one of: {self._colors}")

        self._stations = self._initialize_line(station_df)

        if num_trains > len(self._stations) / 2:
            raise ValueError("Too many trains!")
        self._num_trains = num_trains

        self._time_interval = float(config['PARAM']['CTA_LINE_UPDATE_INTERVAL'])

    def _initialize_line(self, station_df: DataFrame):
        """Initialize stations on the line."""
        stations_df = station_df[station_df[self._color.lower()]]
        station_ids = stations_df["station_id"].unique()

        # build a doubly linked list
        line = []
        prev_station = None
        for station_id in station_ids:
            station_data = station_df[station_df["station_id"] == station_id]

            name = station_data["station_name"].unique()[0]
            # id: int64 -> int
            curr_station = Station(int(station_id), name, self._color)
            curr_station.a_station = prev_station
            if prev_station is not None:
                prev_station.b_station = curr_station
            prev_station = curr_station
            line.append(curr_station)
        return line

    async def _initialize_trains(self):
        """Initialize trains for stations."""
        # Evenly distribute the trains at initialization.
        n_stops = len(self._stations) - 1
        step_size = int(2 * n_stops / self._num_trains)
        loc = 0
        b_dir = True
        for tid in range(self._num_trains):
            train = Train(f"{self._color.capitalize()}L{str(tid).zfill(3)}",
                          Train.Status.IN_SERVICE)

            if b_dir and loc >= n_stops:
                loc = 2 * n_stops - loc
                b_dir = False
            elif not b_dir and loc < 0:
                loc = -loc
                b_dir = True

            if b_dir:
                await self._stations[loc].set_b_train(train)
                loc += step_size
            else:
                await self._stations[loc].set_a_train(train)
                loc -= step_size

    async def run(self, cycles=None):
        """Override."""
        await self._initialize_trains()

        count = 1
        while True:
            for station in self._stations:
                station.advance()

            t0 = time.time()
            await asyncio.gather(
                asyncio.create_task(asyncio.sleep(self._time_interval)),
                *[s.run() for s in self._stations])
            # The time consumption should be roughly self._time_interval.
            logger.info(f"Finished updating Line {self._color.capitalize()}: "
                        f"takes {time.time() - t0} s")

            # for test
            if cycles is not None and count == cycles:
                break
            count += 1

    def close(self):
        """Override."""
        for station in self._stations:
            station.close()

    def __str__(self):
        return "\n".join(str(station) for station in self._stations)

    def __repr__(self):
        return str(self)
