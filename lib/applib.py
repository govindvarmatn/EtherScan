
import tweepy
import pandas as pd
import requests
import json
import configparser
import os
from tqdm import tqdm
from pathlib import Path
from time import sleep
import gc



def sleep_func(sec):
    for i in tqdm(range(0,sec),total = sec):
        sleep(1)



class appLib( object ):

    def __init__(self, app_name):
        self.app_name = app_name
        print(f'Initializing {self.app_name} App!')
        self.config_path = os.path.join(os.getcwd(), 'config', f'{self.app_name}.properties')
        print(f'Config Path : {self.config_path}')
        self.config = configparser.ConfigParser(interpolation=None)
        self.config.read(self.config_path)
        os.makedirs('./hist', exist_ok=True)
        self.hist_file_location = os.path.join('hist', f'{app_name}-hist.json')
        self.client = None
        self.api_key = None
        self.api_secret = None
        self.bearer_toekn = None
        self.access_token = None
        self.access_token_secret = None
        self.sleep_time_sec = None
        self.retry_time_sec = None
        self.telegram_token = None
        self.telegram_chat_id = None
        self.telegram_send_message_url = None
        self.json_hist = []

        print('\n------------------------------------------------')
        print(f'\nStarting App : {self.app_name}\n')
        print('------------------------------------------------\n')

    def get_config(self, section, param):
        return self.config[section][param].strip()

    def getClient(self):
        client = tweepy.Client(
            bearer_token=self.bearer_toekn,
            consumer_key=self.api_key,
            consumer_secret=self.api_secret,
            access_token=self.access_token,
            access_token_secret=self.access_token_secret,
            wait_on_rate_limit=True
        )
        return client

    def initialize_parameters(self):
        print('initializing parameters')
        self.api_key = self.get_config('TwitterSection', 'twitter.api.key')
        self.api_secret = self.get_config('TwitterSection', 'twitter.api.key.secret')
        self.bearer_toekn = self.get_config('TwitterSection', 'twitter.bearer.token')
        self.access_token = self.get_config('TwitterSection', 'twitter.access.token')
        self.access_token_secret = self.get_config('TwitterSection', 'twitter.access.token.secret')
        self.sleep_time_sec = int(float(self.get_config('SleeptimeSection', 'monitor.sleep.mins')) * 60)
        self.retry_time_sec = int(float(self.get_config('SleeptimeSection', 'monitor.retry.mins')) * 60)
        self.telegram_token = self.get_config('TelegramSection', 'telegram.token.code')
        self.telegram_chat_id = self.get_config('TelegramSection', 'telegram.chat.id')
        self.telegram_send_message_url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage?chat_id={self.telegram_chat_id}&text=<message>&parse_mode=html"
        print('Intializing Twitter client')
        self.client = self.getClient()
        print('completed initializing client')

    def divide_chunks(self, l, n=100):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def get_userdetails_from_id(self,ids):
        list_ids = list(ids)
        chunked_list = list(self.divide_chunks(list_ids))
        user_info = []
        for i in chunked_list:
            data = self.client.get_users(ids=i)
            user_info.extend([{'id': i.id, 'name': i.name, 'username': i.username} for i in data[0]])
        return pd.DataFrame(user_info)

    def getuserinfo(self, user_name):
        user = self.client.get_user(username=user_name)
        return {
            'id': user.data.id,
            'name': user.data.name,
            'username': user_name
        }

    def re_initialize_twitter_clint(self):
        del self.client
        collected = gc.collect()
        print(f'garbage collected while re-initialing twitter client : {collected}')
        self.client = None
        sleep(180)
        self.client = self.getClient()
        print('re-initialed twitter client')


    def send_message_telegram(self, message):
        url = self.telegram_send_message_url.replace('<message>', message)
        print('sending message to telegram')
        r = requests.get(url)
        if r.status_code == 200:
            print('message sent successfully !')
        else:
            print('message was not sent, Ran into some problem')
            print(f'Response status : {r.text}')

    def create_html_messag_and_send(self, usr, name, followdf):
        for _, row in followdf.iterrows():
            user_link = f'https://twitter.com/{usr}'
            user_anchor = f'<a href="{user_link}">{name}</a>'
            followee_usrname = row["username"]
            followee_name = row["name"]
            following_link = f'https://twitter.com/{followee_usrname}'
            following_anchor = f'<a href="{following_link}">{followee_name}</a>'
            notification = f"User {user_anchor} started following {following_anchor})"
            self.send_message_telegram(notification)

    def get_all_followers_id(self,user_name, limit=100):
        user_ = self.getuserinfo(user_name)
        if limit is not None:
            followee = self.client.get_users_following(user_['id'], max_results=limit)
            if followee[0] == None:
                return set([])
            if len(followee[0]) > 0:
                followee_id = [i.id for i in followee[0]]
                return set(followee_id)
        else:
            followee = self.client.get_users_following(user_['id'], max_results=1000)
            if followee[0] == None:
                return set([])
            if len(followee[0]) > 0:
                followee_id = [i.id for i in followee[0]]
                meta = followee[-1]
                while True:
                    if 'next_token' not in meta:
                        break
                    else:
                        f_new = self.client.get_users_following(user_['id'], max_results=1000,
                                                                pagination_token=meta['next_token'])

                        followee_id.extend([i.id for i in f_new[0]])
                        meta = f_new[-1]
                return set(followee_id)
            else:
                return set([])

    def save_hist(self,followee_lookup_dict):
        with open(self.hist_file_location, 'w') as fp:
            print('saving history results!')
            json.dump(followee_lookup_dict, fp)

    def get_user_val(self, username):
        return {
            'username': username,
            'name': self.getuserinfo(username)['name'],
            'followee': list(self.get_all_followers_id(username))
        }

    def get_extra_users(self,new_users,json_hist):
        if len(new_users) > 0:
            print(f'adding new users = {new_users}')

            for k in new_users:
                try:
                    json_hist.append(self.get_user_val(k))
                    sleep_func(self.sleep_time_sec)
                    self.save_hist(json_hist)
                except Exception as e:
                    print('Error occured while adding new users to history :',e)
                    print(f'continue in {self.retry_time_sec} seconds')
                    sleep_func(self.retry_time_sec)
                    continue

        return json_hist

    def calc_deleteed_users(self, json_hist, df_lookup):
        current_users = [i['username'] for i in json_hist]
        deleted_users = set(current_users) - set(df_lookup)

        if len(deleted_users) == 0:
            return json_hist

        print(f'deleting users {deleted_users} from lookup')

        rem_list = []
        for i in json_hist:
            if i['username'] in deleted_users:
                rem_list.append(i)
                print(f'removing {i["username"]} from monitoring lookup')

        ret_json_list = [i for i in json_hist if i not in rem_list]
        return ret_json_list

    def get_followers_hist(self):
        print('processing followers history data')
        df_lookup = pd.read_excel('./lookup/Lookup.xlsx', sheet_name=self.app_name)['USER_NAME']
        df_lookup = pd.Series([i.strip() for i in df_lookup])

        if Path(self.hist_file_location).is_file():
            with open(self.hist_file_location, 'r') as f:
                json_hist = json.load(f)

        else:
            print(f'history file : {self.hist_file_location} is not present')
            json_hist = []

        if len(json_hist) == 0:
            print('Creating History file')
            for i in df_lookup:
                try:
                    json_hist.append(self.get_user_val(i))
                    sleep_func(self.sleep_time_sec)
                    self.save_hist(json_hist)
                except Exception as e:
                    print('Error occured while freshly populating history')
                    self.re_initialize_twitter_clint()
                    print(f'Continue in {self.retry_time_sec}')
                    sleep_func(self.retry_time_sec)
                    continue


        else:
            print('updating history file')
            current_users = [i['username'] for i in json_hist]
            new_users = list(set(df_lookup) - set(current_users))
            json_hist = self.get_extra_users(new_users,json_hist)
            json_hist = self.calc_deleteed_users(json_hist,df_lookup)
            self.save_hist(json_hist)

        print('completed processing followers history data')
        self.json_hist = json_hist


    def __dell__(self):
        del self.client
        self.client = None









