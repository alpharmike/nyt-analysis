from src.utils.database import DatabaseUtils
from src.utils.config_parser import parse_config

db_config = parse_config("database")

database = DatabaseUtils(uri=db_config["MONGO"]["URI"], db=db_config["MONGO"]["DB_NAME"],
                         collection=db_config["MONGO"]["DB_COLLECTION"])
