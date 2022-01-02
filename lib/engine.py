import datetime
import lib.applib as applib
from lib.applib import appLib
import gc

class engine( appLib ):

    def __init__(self,app_name):
        appLib.__init__(self,app_name)

    def monitor(self):
        l_tot = 0
        for usr_ in self.json_hist:
            try:
                user_name = usr_['username']
                name = usr_['name']
                set_followee = set(usr_['followee'])
                print(f'time : {datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}, user : {user_name}, name : {name}, monitoring_followees : {len(set_followee)}')
                new_followee_val = self.get_all_followers_id(user_name)
                diff = new_followee_val - set_followee

                if len(diff) > 0:
                    usr_['followee'] = list(new_followee_val)
                    new_followee_dict = self.get_userdetails_from_id(diff)
                    message = f'user {user_name} started following below accounts \n {new_followee_dict}'
                    print(message)
                    self.create_html_messag_and_send(user_name, name, new_followee_dict)
                    self.save_hist(self.json_hist)
                applib.sleep_func(self.sleep_time_sec)
                l_tot += 1

            except Exception as e:
                print('Monitoring module error : ',e)
                print(f'continue after {self.sleep_time_sec} sec')
                applib.sleep_func(self.sleep_time_sec)
                if l_tot == len(self.json_hist//2):
                    self.send_message_telegram(f'WARNING !! continuous error while monitoring; Error :: {e}')
                continue

    def execute(self):
        start_message = f"starting monitoring at {datetime.datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}"
        print(start_message)
        self.send_message_telegram(start_message)
        c = 0
        while True:
            try:
                if c != 0:
                    self.get_followers_hist()
                print(f'executing for {len(self.json_hist)} users')
                self.monitor()
                c += 1
            except Exception as e:
                collected = gc.collect()
                print(f'garbage collected while executing app engine: {collected}')
                print('Error while executing Monitor Function : ', e)
                print(f're execute in {self.retry_time_sec} seconds')
                self.re_initialize_twitter_clint()
                self.send_message_telegram(f'App Stopped at time {datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")} !, \
                                                    re-execute in {self.retry_time_sec} \
                                                    seconds due to the error :: {e}')

                applib.sleep_func(self.retry_time_sec)
                continue





