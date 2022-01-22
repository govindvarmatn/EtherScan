import pymongo
import certifi
import pandas as pd
import logging
from lib.ConfigReader import ConfigReader

logger = logging.getLogger('lib.ExecutionLogger')


class MongoDBConn(object):

    def __init__(self, app_name):
        self.app_name = app_name
        ConfigReader.init_conf(app_name)
        self.mongo_user_name = ConfigReader.get_config('MongoDBSection', 'mongo.db.username')
        self.mongo_password = ConfigReader.get_config('MongoDBSection', 'mongo.db.password')
        self.mongo_database_name = ConfigReader.get_config('MongoDBSection', 'mongo.db.dbname')
        self.mongo_collection = ConfigReader.get_config('MongoDBSection', 'mongo.db.colname')

    def create_client(self):
        logger.debug('creating mongoDB client')
        my_client = pymongo.MongoClient(f"mongodb+srv://{self.mongo_user_name}:{self.mongo_password}"
                                        + "@cluster0.ucein.mongodb.net/test",
                                        tlsCAFile=certifi.where())
        logger.debug('completed creating mongoDB client')
        return my_client

    def get_table_details(self,client):
        return {
                'DB': client[self.mongo_database_name],
                'COLLECTION': client[self.mongo_database_name][self.mongo_collection]
                }

    @staticmethod
    def close_connection(client):
        logger.debug('terminating mongoDB connection')
        client.close()

    def upsert_one_doc(self, transaction):
        if len(transaction) == 0:
            logger.warning('no records to insert, skipping insert step')
        else:
            client = self.create_client()
            table_details = self.get_table_details(client)
            key_query = {
                "address": transaction['address'],
                "token_code": transaction['token_code']
            }
            if not len(list(table_details['COLLECTION'].find(key_query))):
                logger.info('inserting document to mongoDB')
                table_details['COLLECTION'].insert_one(transaction)
                logger.info(f'completed inserting document (with address = {transaction["address"]}) to mongoDB')
            else:
                logger.info('updating document in MongoDB')
                table_details['COLLECTION'].replace_one(key_query, transaction, upsert=True)
                logger.info(f'completed updating address ({transaction["address"]}) in mongoDB')
            self.close_connection(client)

    def get_history_data(self,query):
        client = self.create_client()
        table_details = self.get_table_details(client)
        logger.info('fetching history data')
        cursor = table_details['COLLECTION'].find(query)
        df = pd.DataFrame(cursor)
        self.close_connection(client)
        logger.info('completed fetching data from history')
        return df

    def delete_docs(self, query):
        client = self.create_client()
        table_details = self.get_table_details(client)
        logger.warning('deleting documents from mongoDB')
        del_num = table_details['COLLECTION'].delete_many(query)
        self.close_connection(client)
        logger.warning(f'deleted {del_num.deleted_count} documents from mongoDB')

    def update_history_data(self, fetch_query, update_query):
        logger.warning('updating history mongoDB documents')
        client = self.create_client()
        table_details = self.get_table_details(client)
        table_details['COLLECTION'].update_one(fetch_query, update_query)
        self.close_connection(client)
        logger.warning('completed updating history mongoDB documents')















