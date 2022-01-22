from lib.EtherScanApp import EtherScanApp
import logging
import datetime
import threading
import gc
from lib.EmailNotification import EmailNotification
from time import sleep
logger = logging.getLogger('lib.ExecutionLogger')
from tqdm import tqdm


class MonitorEngine(EtherScanApp):

    def __init__(self, app_name):
        self.app_name = app_name
        EtherScanApp.__init__(self, app_name)

    def monitor(self, token_code):
        t_start = datetime.datetime.now().strftime("%Y/%m/%d %I:%M:%S %p")
        logger.info(f'starting executing at {t_start}')
        look_up_df = self.get_data_lookup()
        self.history_create(token_code)
        logger.info(f'scanning for {look_up_df.shape[0]} wallets....')

        for _, row in tqdm(look_up_df.iterrows(), total=look_up_df.shape[0], desc=f'Tracking {token_code}'):
            try:
                print()
                print('----------------------------------------------------------------------------------------------------------')

                logger.info(f'Monitoring transaction for --> {row["NAME"]}')
                address = row['ADDRESS']
                query = {'address': address, 'token_code': token_code}
                current_transactions_df = self.get_history_data(query)
                new_transactions_dict = self.fetch_etherscan_data(row, token_code)
                new_transactions_df = new_transactions_dict['df']
                new_transactions_hist = new_transactions_dict['hist']

                current_trans_set = set(current_transactions_df['transaction_hash'].iloc[0])
                if new_transactions_df.shape[0] == 0 and len(new_transactions_hist['transaction_hash']) == 0:
                    new_trans_set = set()
                else:
                    new_trans_set = set(new_transactions_hist['transaction_hash'])

                logger.info('calculating extra transactions')
                diff_trans_set = new_trans_set - current_trans_set
                if len(diff_trans_set) > 0:
                    self.upsert_one_doc(new_transactions_hist)
                    diff_df = new_transactions_df[new_transactions_df['hash_unique'].isin(list(diff_trans_set))]
                    logger.info('new transactions made..')
                    email_obj = EmailNotification(self.app_name)
                    email_obj.send_email(diff_df, address, row["NAME"], token_code)
                else:
                    logger.info(f'no new transaction for --> {row["NAME"]}')

                print('----------------------------------------------------------------------------------------------------------')
                print()
                self.sleep_animation(self.sleep_sec)
            except Exception as e:
                logger.exception(f'Exception while monitoring transaction for {row["NAME"]}')
                logger.warning('continue execution in 1 min')
                self.sleep_animation(self.retry_sec)
                continue

    def execute(self):
        while True:
            try:
                thread_sleep_time = self.sleep_sec//2
                t1 = threading.Thread(target=self.monitor, args=('erc20',))
                t1.name = 'erc20_thread'
                t2 = threading.Thread(target=self.monitor, args=('erc721',))
                t2.name = 'erc721_thread'
                t1.start()
                sleep(thread_sleep_time)
                t2.start()
                t1.join()
                t2.join()
            except Exception as e:
                logger.critical(f'Exception while monitoring in main execute function')
                logger.exception(f'Exception while monitoring in main execute function')
                collected = gc.collect()
                logger.warning(f'garbage collected in monitoring function : {collected}')
                logger.warning('continue execution in 1 min')
                self.sleep_animation(60)
                continue



