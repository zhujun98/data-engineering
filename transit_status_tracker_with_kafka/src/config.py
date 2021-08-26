import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read(Path(__file__).parents[1].joinpath("config.ini"))
