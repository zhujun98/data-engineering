import logging
from logging.config import fileConfig
from pathlib import Path

fileConfig(f"{Path(__file__).parents[2]}/logging.ini")

logger = logging.getLogger("producer")
logger.setLevel("DEBUG")
