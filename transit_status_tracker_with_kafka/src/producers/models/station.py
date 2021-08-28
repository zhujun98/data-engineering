from pathlib import Path

from confluent_kafka import avro

from ...config import config
from ...utils import normalize_station_name
from ..logger import logger
from .turnstile import Turnstile
from .producer import Producer


class Station(Producer):
    """Defines a single train station."""

    key_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id: int, station_name: str, color: str):
        self._name = station_name

        topic_root = config["TOPIC"]["ARRIVAL_ROOT"]
        topic_name = f"{topic_root}.{normalize_station_name(station_name)}.0"
        super().__init__(
            topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            num_partitions=1,
            num_replicas=1
        )

        self._id = station_id
        self._color = color
        self._a_train = None  # train ready to move in the a direction
        self._b_train = None  # train ready to move in the b direction
        # If given, it is a tuple of
        # (train, previous station ID, previous direction)
        self._a_arriving = None
        self._b_arriving = None
        self._turnstile = Turnstile(station_id, station_name, color)

        self.a_station = None  # next station in the a direction
        self.b_station = None  # next station in the b direction

    def run(self):
        """Override."""
        self._turnstile.run()

        if self._a_arriving is not None:
            if self.a_station is not None:
                self.set_a_train(*self._a_arriving)
            else:
                # The direction of the train flips at the end of the line.
                self.set_b_train(*self._a_arriving)
            self._a_arriving = None

        if self._b_arriving is not None:
            if self.b_station is not None:
                self.set_b_train(*self._b_arriving)
            else:
                # end of line
                self.set_a_train(*self._b_arriving)
            self._b_arriving = None

    def set_a_train(self, train, prev_station_id=None, prev_direction=None):
        """Register a train that will travel to the a direction."""
        self._a_train = train
        self._produce_message(train.train_id, "a", train.status.name,
                              prev_station_id, prev_direction)
        logger.info(f"{self._name} -> {self.a_station._name}: {train.train_id}")

    def set_b_train(self, train, prev_station_id=None, prev_direction=None):
        """Register a train that will travel to the b direction."""
        self._b_train = train
        self._produce_message(train.train_id, "b", train.status.name,
                              prev_station_id, prev_direction)
        logger.info(f"{self._name} -> {self.b_station._name}: {train.train_id}")

    def _produce_message(self, train_id, direction, status,
                         prev_station_id, prev_direction):
        self._producer.produce(
            topic=self._topic_name,
            key={"timestamp": self.time_millis()},
            key_schema=self._key_schema,
            value={
                "station_id": self._id,
                "train_id": train_id,
                "direction": direction,
                "line": self._color,
                "train_status": status,
                "prev_station_id": prev_station_id,
                "prev_direction": prev_direction
            },
            value_schema=self._value_schema
        )

    def advance(self):
        """Move trains to next stations."""
        if self._a_train is not None:
            self.a_station._a_arriving = (self._a_train, self._id, "a")
            self._a_train = None

        if self._b_train is not None:
            self.b_station._b_arriving = (self._b_train, self._id, "b")
            self._b_train = None

    def close(self):
        """Override."""
        self._turnstile.close()
        super().close()

    def __str__(self):
        return f"Station" \
               f" | {self._id:^5}" \
               f" | {self._name:<30}" \
               f" | Direction A: | {str(self._a_train) if self._a_train is not None else '---':<30}" \
               f" | departing to {self.a_station._name if self.a_station is not None else '---':<30}" \
               f" | Direction B: | {str(self._b_train) if self._b_train is not None else '---':<30}" \
               f" | departing to {self.b_station._name if self.b_station is not None else '---':<30} | "

    def __repr__(self):
        return str(self)
