import os
import sys
import certifi
import pymongo
from src.constant.constant import DATABASE_NAME, MONGO_DB_URL_ENV_KEY
from src.exception import FinanceException

ca = certifi.where()


class MongoDBClient:
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGO_DB_URL_ENV_KEY)
                if mongo_db_url is None:
                    raise Exception(
                        f"Environment Key: {MONGO_DB_URL_ENV_KEY} is not set."
                    )
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)

            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name

        except Exception as e:
            raise FinanceException(e, sys) from e
