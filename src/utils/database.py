import sys
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import logging
from src.utils.singleton import SingletonMeta


class DatabaseUtils(metaclass=SingletonMeta):
    def __init__(self, uri=None, db=None, collection=None):
        self.logger = logging.getLogger(__name__)
        self.client: MongoClient = None
        self.db: Database = None
        self.collection: Collection = None
        self.init_db(uri, db, collection)

    def init_db(self, uri, db, collection):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[db]
            self.collection = self.db[collection]
            self.logger.info("Successfully established connection to database")
        except Exception as error:
            self.logger.critical(f"Could not connect to database - {str(error)}")
            sys.exit(1)

    def creat_new_collection(self, collection):
        try:
            new_collection = self.db[collection]
        except Exception as error:
            self.logger.error(f"Could not create new collection - {str(error)}")

    def watch_for_change(self):
        try:
            with self.collection.watch([{'$match': {'operationType': 'insert'}}]) as stream:
                for insert_change in stream:
                    pass
        except Exception as error:
            self.logger.error(f"Could not watch for new record - {str(error)}")
