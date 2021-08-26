import logging
from logging.config import fileConfig
from pathlib import Path

fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)
