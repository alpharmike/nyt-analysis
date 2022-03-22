import sys
from pymongo import MongoClient
import logging
from src.utils.config_parser import default_config, parse_config

db_config = parse_config("database")
logger = logging.getLogger(__name__)


def init_db():
    try:
        client = MongoClient(db_config["MONGO"]["URI"])
        db_name = db_config["MONGO"]["DB_NAME"]
        collection_name = db_config["MONGO"]["DB_COLLECTION"]
        db = client[db_name]
        collection = db[collection_name]
    except Exception as error:
        logger.critical(f"Could not connect to database - {str(error)}")
        sys.exit(1)
