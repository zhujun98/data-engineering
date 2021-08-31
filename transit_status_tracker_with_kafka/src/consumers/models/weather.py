"""Contains functionality related to Weather"""
from ..logger import logger


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        self.temperature = message['temperature']
        self.status = message['status']
