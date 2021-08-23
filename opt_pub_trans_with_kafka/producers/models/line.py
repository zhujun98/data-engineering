"""Defines functionality relating to train lines"""
import datetime
import logging

from pandas import DataFrame

from .station import Station
from .train import Train

logger = logging.getLogger(__name__)


class CTALine:
    """Chicago Transit Authority (CTA) 'L' (Train) system."""

    _colors = frozenset(['red', 'blue', 'green'])

    num_directions = 2

    def __init__(self, color: str, station_df: DataFrame, num_trains: int = 10):
        self._color = color.lower()
        if self._color not in self._colors:
            raise ValueError(f"CTALine color must be one of: {self._colors}")

        self._num_trains = num_trains
        self._stations = self._build_line(station_df)
        self._num_stops = len(self._stations) - 1
        self.trains = self._build_trains()

    def _build_line(self, station_df: DataFrame):
        """Constructs all stations on the line."""
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
            curr_station.dir_a = prev_station
            if prev_station is not None:
                prev_station.dir_b = curr_station
            prev_station = curr_station
            line.append(curr_station)
        return line

    def _build_trains(self):
        """Constructs and assigns train objects to stations"""
        trains = []
        curr_loc = 0
        b_dir = True
        for train_id in range(self._num_trains):
            tid = str(train_id).zfill(3)
            train = Train(f"{self._color}L{tid}", Train.Status.IN_SERVICE)
            trains.append(train)

            if b_dir:
                self._stations[curr_loc].arrive_b(train, None, None)
            else:
                self._stations[curr_loc].arrive_a(train, None, None)
            curr_loc, b_dir = self._get_next_idx(curr_loc, b_dir)

        return trains

    def run(self, curr_datetime: datetime.datetime,
            time_step: datetime.timedelta):
        for station in self._stations:
            station.turnstile.run(curr_datetime, time_step)

        self._advance_trains()

    def _advance_trains(self):
        """Advances trains between stations"""
        # Find the first b train
        curr_train, curr_index, b_direction = self._next_train()
        self._stations[curr_index].b_train = None

        trains_advanced = 0
        while trains_advanced < self._num_trains - 1:
            # The train departs the current station
            if b_direction:
                self._stations[curr_index].b_train = None
            else:
                self._stations[curr_index].a_train = None

            prev_station = self._stations[curr_index].station_id
            prev_dir = "b" if b_direction else "a"

            # Advance this train to the next station
            curr_index, b_direction = self._get_next_idx(
                curr_index, b_direction, step_size=1
            )
            if b_direction:
                self._stations[curr_index].arrive_b(curr_train, prev_station, prev_dir)
            else:
                self._stations[curr_index].arrive_a(curr_train, prev_station, prev_dir)

            # Find the next train to advance
            move = 1 if b_direction else -1
            next_train, curr_index, b_direction = self._next_train(
                curr_index + move, b_direction
            )
            if b_direction:
                curr_train = self._stations[curr_index].b_train
            else:
                curr_train = self._stations[curr_index].a_train

            curr_train = next_train
            trains_advanced += 1

        # The last train departs the current station
        if b_direction:
            self._stations[curr_index].b_train = None
        else:
            self._stations[curr_index].a_train = None

        # Advance last train to the next station
        prev_station = self._stations[curr_index].station_id
        prev_dir = "b" if b_direction else "a"
        curr_index, b_direction = self._get_next_idx(
            curr_index, b_direction, step_size=1
        )
        if b_direction:
            self._stations[curr_index].arrive_b(curr_train, prev_station, prev_dir)
        else:
            self._stations[curr_index].arrive_a(curr_train, prev_station, prev_dir)

    def _next_train(self, start_index=0, b_direction=True, step_size=1):
        """Given a starting index, finds the next train in either direction"""
        if b_direction:
            curr_index = self._next_train_b(start_index, step_size)

            if curr_index == -1:
                curr_index = self._next_train_a(len(self._stations) - 1, step_size)
                b_direction = False
        else:
            curr_index = self._next_train_a(start_index, step_size)

            if curr_index == -1:
                curr_index = self._next_train_b(0, step_size)
                b_direction = True

        if b_direction:
            return self._stations[curr_index].b_train, curr_index, True
        return self._stations[curr_index].a_train, curr_index, False

    def _next_train_b(self, start_index, step_size):
        """Finds the next train in the b direction, if any"""
        for i in range(start_index, len(self._stations), step_size):
            if self._stations[i].b_train is not None:
                return i
        return -1

    def _next_train_a(self, start_index, step_size):
        """Finds the next train in the a direction, if any"""
        for i in range(start_index, 0, -step_size):
            if self._stations[i].a_train is not None:
                return i
        return -1

    def _get_next_idx(self, curr_index, b_direction, step_size=None):
        """Calculates the next station index.

        Returns next index and if it is b direction.
        """
        if step_size is None:
            step_size = int((self._num_stops * self.num_directions) / self._num_trains)
        if b_direction:
            next_index = curr_index + step_size
            if next_index < self._num_stops:
                return next_index, True
            return self._num_stops - (next_index % self._num_stops), False
        else:
            next_index = curr_index - step_size
            if next_index > 0:
                return next_index, False
            return abs(next_index), True

    def close(self):
        """Called to stop the simulation"""
        for station in self._stations:
            station.close()

    def __str__(self):
        return "\n".join(str(station) for station in self._stations)

    def __repr__(self):
        return str(self)
