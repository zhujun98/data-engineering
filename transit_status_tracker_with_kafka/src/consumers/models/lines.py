"""Contains functionality related to Lines"""
import json

from ...config import config
from ..logger import logger
from .line import Line


class Lines:
    """Contains all train lines"""

    STATION_TABLE_TOPIC = config["TOPIC"]["STATION_TABLE"]
    TURNSTILE_TABLE_TOPIC = config["TOPIC"]["TURNSTILE_TABLE"]

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        # TODO: better solution?
        if "cta.station" in message.topic():
            value = message.value()
            if message.topic() == self.STATION_TABLE_TOPIC:
                value = json.loads(value)

            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif self.TURNSTILE_TABLE_TOPIC == message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())
