from os import path
from configparser import ConfigParser


def load_config() -> ConfigParser:
    config = ConfigParser()
    config.read(path.join(path.dirname(__file__), '../config/config.ini'))

    return config
