import asyncio
import datetime

from ..logger import logger
from ...config import config


class DatetimeSimulator:
    def __init__(self):
        self._dt = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0)
        self._time_interval = float(
            config['PARAM']['TIMER_UPDATE_TIME_INTERVAL'])
        self._prev_dt = self._dt

    @property
    def month(self):
        return self._dt.month

    @property
    def weekday(self):
        return self._dt.weekday()

    @property
    def hour(self):
        return self._dt.hour

    async def run(self):
        while True:
            await asyncio.sleep(self._time_interval)
            self._dt += datetime.timedelta(hours=1)

            if self._dt.weekday() != self._prev_dt.weekday():
                logger.info(f"Weakday changes from {self._prev_dt.weekday()} "
                            f"to {self._dt.weekday()}")
            if self._dt.month != self._prev_dt.month:
                logger.info(f"Month changes from {self._prev_dt.month} "
                            f"to {self._dt.month}")

            self._prev_dt = self._dt


timer = DatetimeSimulator()
