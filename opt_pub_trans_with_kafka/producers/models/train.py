"""Defines CTA Train Model"""
from enum import IntEnum
import logging


logger = logging.getLogger(__name__)


class Train:
    """Defines CTA Train Model"""

    class Status(IntEnum):
        OUT_OF_SERVICE = 0
        IN_SERVICE = 1
        BROKEN_DOWN = 2

    def __init__(self, train_id: str, status: int = 0):
        self._train_id = train_id
        self._status = self.Status(status)

    def set_status(self, status: int):
        self._status = self.Status(status)

    def __str__(self):
        return f"Train ID {self._train_id} is " \
               f"{self._status.name.replace('_', ' ')}"

    def __repr__(self):
        return str(self)
