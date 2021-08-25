"""Defines functionality relating to train lines"""
import logging

from pandas import DataFrame

from .producer import Producer
from .station import Station
from .train import Train

logger = logging.getLogger(__name__)


class CTALine(Producer):
    """Chicago Transit Authority (CTA) 'L' (Train) system."""

    _colors = frozenset(['red', 'blue', 'green'])

    def __init__(self, color: str, station_df: DataFrame, num_trains: int):
        self._color = color.lower()
        if self._color not in self._colors:
            raise ValueError(f"CTALine color must be one of: {self._colors}")
        topic_name = f"line.{self._color}"
        super().__init__(topic_name, None, None)

        self._stations = self._initialize_line(station_df)

        if num_trains > len(self._stations) / 2:
            raise ValueError("Too many trains!")
        self._num_trains = num_trains

        self._initialize_trains()

    def _initialize_line(self, station_df: DataFrame):
        """Initialize stations on the line."""
        stations_df = station_df[station_df[self._color.lower()]]
        station_names = stations_df["station_name"].unique()

        # build a doubly linked list
        line = []
        prev_station = None
        for name in station_names:
            station_data = station_df[station_df["station_name"] == name]
            curr_station = Station(
                station_data["station_id"].unique()[0],
                name,
                self._color
            )
            curr_station.a_station = prev_station
            if prev_station is not None:
                prev_station.b_station = curr_station
            prev_station = curr_station
            line.append(curr_station)
        return line

    def _initialize_trains(self):
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
                self._stations[loc].set_b_train(train)
                loc += step_size
            else:
                self._stations[loc].set_a_train(train)
                loc -= step_size

    def run(self):
        """Override."""
        for station in self._stations:
            station.advance()
        for station in self._stations:
            station.run()

    def close(self):
        """Override."""
        for station in self._stations:
            station.close()

    def __str__(self):
        return "\n".join(str(station) for station in self._stations)

    def __repr__(self):
        return str(self)
