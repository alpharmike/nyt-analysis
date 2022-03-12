import configparser
from src.utils.utils import ROOT_DIR


def parse_config(filename):
    config = configparser.ConfigParser()
    config.read(f"{ROOT_DIR}/config/{filename}.ini")
    return config


default_config = parse_config("default")
