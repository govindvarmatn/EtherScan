import sys
from lib.engine import engine
from time import sleep
import gc

def main():
    args = sys.argv
    if len(args) >= 2:
        app_name = args[1]
    else:
        app_name = 'Govind1stApp'

    while True:
        try:
            engine_ = engine(app_name)
            engine_.initialize_parameters()
            engine_.get_followers_hist()
            engine_.execute()
        except Exception as e:
            collected = gc.collect()
            print(f'garbage collected in main function : {collected}')
            print('ALERT !! error in main function : ', e)
            print('retry after 15 mins')
            sleep(15*60)
            continue


if __name__ == "__main__":
    main()




