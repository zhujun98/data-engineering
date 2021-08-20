"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from .producer import Producer
from .turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
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
                         key_schema=Turnstile.key_schema,
                         value_schema=Turnstile.value_schema,
                         num_partitions=1,
                         num_replicas=1)
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
