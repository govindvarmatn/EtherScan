from sqlalchemy import Column, VARCHAR, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import pandas as pd
import logging
from lib.ConfigReader import ConfigReader

logger = logging.getLogger('lib.ExecutionLogger')


class SingleStoreConn(object):
    
    def __init__(self, app_name):
        self.app_name = app_name
        ConfigReader.init_conf(app_name)
        self.single_store_user_name = ConfigReader.get_config('LookupDBSection', 'lookup.db.username')
        self.single_store_password = ConfigReader.get_config('LookupDBSection', 'lookup.db.password')
        self.single_store_db_name = ConfigReader.get_config('LookupDBSection', 'lookup.db.dbnam')
        self.single_store_table_name = ConfigReader.get_config('LookupDBSection', 'lookup.db.tablemame')
        self.single_store_engine_string = ''
        self.Base = declarative_base()
        self.meta = MetaData()
        self.lookup_table = Table(
            self.single_store_table_name, self.meta,
            Column('ADDRESS', VARCHAR(200), primary_key=True),
            Column('NAME', VARCHAR(200))
        )

    def create_engine(self):
        logger.debug('creating SQLAlchemy engine')
        self.single_store_engine_string = f"mysql+pymysql://{self.single_store_user_name}:{self.single_store_password}@svc-7c4e0ffd-9123-47c7-a80c-0b509e8fa322-ddl.gcp-mumbai-1.svc.singlestore.com"
        engine = create_engine(self.single_store_engine_string)
        logger.debug('completed creating SQlAlchemy engine')
        return engine

    @staticmethod
    def remove_engine(engine):
        logger.debug('terminating SQLAlchemy connection')
        engine.dispose()

    def create_db(self):
        logger.info(f'creating db {self.single_store_db_name}')
        engine = self.create_engine()
        engine.execute(f'CREATE DATABASE {self.single_store_db_name}')
        engine.execute(f'USE {self.single_store_db_name}')
        logger.info(f'completed creating {self.single_store_db_name}')
        self.remove_engine(engine)

    def create_table(self):
        logger.info(f'creating table {self.single_store_table_name}')
        engine = self.create_engine()
        engine.execute(f'USE {self.single_store_db_name}')
        self.meta.create_all(engine)
        self.Base.metadata.create_all(engine)
        logger.info(f'completed creating table {self.single_store_table_name}')
        self.remove_engine(engine)

    def insert_data(self, df):
        logger.info(f'inserting data to table {self.single_store_table_name}')
        engine = self.create_engine()
        engine.execute(f'USE {self.single_store_db_name}')
        engine.execute(f'truncate table {self.single_store_table_name}')
        ins_list = [{k: v.strip() for k, v in dict(row).items()} for _, row in df.iterrows()]
        engine.execute(self.lookup_table.insert(), ins_list)
        logger.info(f'inserted {df.shape[0]} records to table {self.single_store_table_name}')
        logger.info(f'completed inserting data to table : {self.single_store_table_name}')
        self.remove_engine(engine)

    def get_data_lookup(self):
        engine = self.create_engine()
        logger.info('fetching lookup data')
        df = pd.read_sql(f'select * from {self.single_store_db_name}.{self.single_store_table_name}', con=engine)
        self.remove_engine(engine)
        return df



