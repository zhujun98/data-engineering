"""Defines CTA Train Model"""
from enum import IntEnum
import logging


logger = logging.getLogger(__name__)


class Train:
    """Defines a CTA Train."""

    class Status(IntEnum):
        OUT_OF_SERVICE = 0
        IN_SERVICE = 1
        BROKEN_DOWN = 2

    def __init__(self, train_id: str, status: int = 0):
        self.train_id = train_id

        self._status = None
        self.status = status

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status: int):
        self._status = self.Status(status)

    def __str__(self):
        return f"{self.train_id}, {self._status.name.replace('_', ' ')}"

    def __repr__(self):
        return f"Train ({self.__str__()})"
