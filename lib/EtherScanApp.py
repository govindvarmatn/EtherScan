from lib.SingleStoreConn import SingleStoreConn
from lib.MongoDBConn import MongoDBConn
import requests
import pandas as pd
import json
from datetime import datetime
from time import sleep
from dateutil import tz
import logging
from lib.ConfigReader import ConfigReader
from time import sleep

logger = logging.getLogger('lib.ExecutionLogger')


class EtherScanApp(SingleStoreConn, MongoDBConn):
    def __init__(self, app_name):
        self.app_name = app_name
        SingleStoreConn.__init__(self, app_name)
        MongoDBConn.__init__(self, app_name)
        ConfigReader.init_conf(app_name)
        self.sleep_sec = int(ConfigReader.get_config('ExectionSection', 'exec.sleep.sec'))
        self.retry_sec = int(ConfigReader.get_config('ExectionSection', 'exec.retry.sec'))
        self.api_key = ConfigReader.get_config('ExectionSection', 'exec.api.key')
        self.erc20token_action_code = ConfigReader.get_config('ExectionSection', 'exec.erc20.ext')
        self.erc721token_action_code = ConfigReader.get_config('ExectionSection', 'exec.erc721.ext')
        self.ether_scan_api_root_url = ConfigReader.get_config('ExectionSection', 'exec.api.root').replace(' ','') \
                                                                                                  .replace('\t','') \
                                                                                                  .replace('<<apikey>>',self.api_key)
        

    @staticmethod
    def convert_timestamp_totime(val):
        ts = int(val)
        from_zone = tz.tzutc()
        to_zone = tz.gettz('Asia/Kolkata')
        date_ts = datetime.utcfromtimestamp(ts)
        utc = date_ts.replace(tzinfo=from_zone)
        central = utc.astimezone(to_zone).strftime('%b-%d-%Y %I:%M:%S %p')
        return central

    @staticmethod
    def transaction_calc(from_, to_, address_):
        if address_.strip().upper() == to_.strip().upper():
            return 'IN'
        else:
            return 'OUT'

    @staticmethod
    def create_mongo_hist_dict(df,address, token_code, name=None):
        logger.info('preparing mongoDB history document')
        hist_dict = {
            'address': address,
            'token_code': token_code,
            'name': name,
            'transaction_hash': df['hash_unique'].values.tolist()
        }
        logger.info('completed preparing mongoDB history document')
        return hist_dict

    @staticmethod
    def sleep_animation(time):
        if time != 0:
            sleep(time)

    def get_erc721_data(self, address, name=None):
        token_code = 'erc721'
        with requests.Session() as session:
            logger.info('fetching erc721token data from the api')
            url = self.ether_scan_api_root_url.replace('<<action_code>>', self.erc721token_action_code) \
                                              .replace('<<address>>', address)
            r = session.get(url)
            if r.status_code != 200:
                raise ValueError(r.text)

            if json.loads(r.text)['result'] == 'Error! Invalid address format':
                raise ValueError('Error! Invalid address format')

            df = pd.DataFrame(json.loads(r.text)['result'])

            if df.shape[0] == 0:
                logger.warning(f'No records present for the address {address} ')
                return {
                    'df': pd.DataFrame([]),
                    'hist': {'address': address,
                            'token_code': token_code,
                            'name': name,
                            'transaction_hash': []
                            }
                }
            df = df[
                ['blockNumber', 'hash', 'timeStamp', 'tokenName', 'tokenSymbol', 'from', 'to']
            ]
            df['hash_unique'] = df['hash'] + '-' + df['blockNumber'] + '-' + df['timeStamp'] + '-' + df['tokenSymbol'] + '-' + df['from'] + '-' + df['to']
            hist_data = self.create_mongo_hist_dict(df, address, token_code, name)
            df['transaction_time'] = df['timeStamp'].apply(lambda x: self.convert_timestamp_totime(x))
            df['transaction'] = df.apply(lambda x: self.transaction_calc(x['from'], x['to'], address), axis=1)
            df['token_code'] = token_code
            df.drop(['timeStamp', 'from', 'to', 'blockNumber'], axis=1, inplace=True)
            return {
                'df': df,
                'hist': hist_data
            }

    def get_erc20_data(self, address, name=None):
        token_code = 'erc20'
        retry_num = 0
        with requests.Session() as session:
            while True:
                logger.info('fetching erc20token data from the api')
                url = self.ether_scan_api_root_url.replace('<<action_code>>', self.erc20token_action_code) \
                                                .replace('<<address>>', address)
                r = session.get(url)
                if r.status_code != 200:
                    raise ValueError(r.text)

                if json.loads(r.text)['result'] == 'Error! Invalid address format':
                    raise ValueError('Error! Invalid address format')

                df = pd.DataFrame(json.loads(r.text)['result'])

                if df.shape[0] == 0:
                    break
                else:
                    if float(df['value'].iloc[0]) == 0.0:
                        retry_num += 1
                        logger.warning(f'RETRY {retry_num} ::: erc20 tracking for address {address}')
                        sleep(5)
                        if retry_num > 3:
                            logger.warning('retry limit exceeded!')
                            logger.warning('exiting retry block for erc20 tracking; value of transaction is still 0')
                            break
                        else:
                            continue
                    else:
                        break
                break

            if df.shape[0] == 0:
                logger.warning(f'No records present for the address {address} ')
                return {
                    'df': pd.DataFrame([]),
                    'hist': {'address': address,
                            'token_code': token_code,
                            'name': name,
                            'transaction_hash': []
                            }
                }
            df = df[
                ['blockNumber', 'hash', 'value', 'timeStamp', 'tokenName', 'tokenSymbol', 'from', 'to']
            ]
            df['hash_unique'] = df['hash'] + '-' + df['blockNumber'] + '-' + df['timeStamp'] + '-' + df['tokenSymbol'] + '-' + df['from'] + '-' + df['to']
            hist_data = self.create_mongo_hist_dict(df, address, token_code, name)
            df['transaction_time'] = df['timeStamp'].apply(lambda x: self.convert_timestamp_totime(x))
            df['transaction'] = df.apply(lambda x: self.transaction_calc(x['from'], x['to'], address), axis=1)
            df['value'] = df['value'].astype(float)/10**18
            df['token_code'] = token_code
            df.drop(['timeStamp', 'from', 'to', 'blockNumber'], axis=1, inplace=True)
            return {
                'df': df,
                'hist': hist_data
            }

    def add_new_elements(self, look_up_df, history_df, token_code):
        new_elements = set(look_up_df['ADDRESS']) - set(history_df['address'])
        if len(new_elements) > 0:
            logger.info('inserting new history records')
            extra_df = look_up_df[look_up_df['ADDRESS'].isin(list(new_elements))]
            for _, row in extra_df.iterrows():
                try:
                    logger.info(f"adding to history - name : {row['NAME']}, address : {row['ADDRESS']}")
                    data_dict = self.fetch_etherscan_data(row, token_code)
                    self.upsert_one_doc(data_dict['hist'])
                    sleep(1)
                except Exception as e:
                    logger.exception(f'error while saving initial history to mongoDB')
                    self.sleep_animation(5)
                    continue
            logger.info('extra history documents added')

    def del_history_elements(self, look_up_df, history_df, token_code):
        del_elements = set(history_df['address']) - set(look_up_df['ADDRESS'])
        if len(del_elements) > 0:
            logger.info('deleting unwanted elements from mongoDB history')
            del_df = history_df[history_df['address'].isin(list(del_elements))]
            for _, row in del_df.iterrows():
                address = row['address']
                t_code = row['token_code']
                name = row['name']
                if t_code == token_code:
                    logger.info(f'deleting from history - name : {name}, address : {address}')
                    self.delete_docs({'address': address, 'token_code': token_code})
                else:
                    logger.error(f'cannot delete name : {name}, address : {address}')
            logger.info('completed deleting unwanted elements from mongoDB history')

    def fetch_etherscan_data(self, row, token_code):
        if token_code == 'erc20':
            data_dict = self.get_erc20_data(row['ADDRESS'], row['NAME'])
        else:
            data_dict = self.get_erc721_data(row['ADDRESS'], row['NAME'])
        return data_dict

    def update_mongo(self, look_up_df, history_df, token_code):
        for _, row in look_up_df.iterrows():
            address = row['ADDRESS']
            name = row['NAME']
            fetch_query = {'address': address, 'token_code': token_code}
            history_data = history_df[(history_df['address'] == address) & (history_df['token_code'] == token_code)]
            if history_data.shape[0] > 0:
                history_name = history_data['name'].iloc[0]
                if history_name != name:
                    logger.info(f'updating name of address ({address}) : from "{history_name}" to "{name}"')
                    update_query = {"$set": {"name": name}}
                    self.update_history_data(fetch_query, update_query)

    def history_create(self, token_code):
        logger.info(f'executing history creation for {token_code} token')
        look_up_df = self.get_data_lookup()
        history_mongo_query = {'token_code': token_code}
        history_df = self.get_history_data(history_mongo_query)
        if history_df.shape[0] == 0:
            logger.info('creating history data')
            for _, row in look_up_df.iterrows():
                try:
                    logger.info(f"adding to history - name : {row['NAME']}, address : {row['ADDRESS']}")
                    data_dict = self.fetch_etherscan_data(row, token_code)
                    self.upsert_one_doc(data_dict['hist'])
                    sleep(1)
                except Exception as e:
                    logger.exception(f'error while saving initial history to mongoDB')
                    self.sleep_animation(5)
                    continue
        else:
            logger.info('updating history data')
            self.add_new_elements(look_up_df, history_df, token_code)
            self.del_history_elements(look_up_df, history_df, token_code)
            history_df = self.get_history_data(history_mongo_query)
            self.update_mongo(look_up_df, history_df, token_code)
            logger.info('completed updating history data')
        logger.info(f'completed executing history creation for {token_code} token')
        