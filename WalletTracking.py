from lib.MonitorEngine import MonitorEngine
from lib.ExecutionLogger import Logger
from time import sleep
import logging
import gc
import sys


def main():

    args = sys.argv
    if len(args) > 1:
        app_name = sys.argv[1]
    else:
        app_name = 'EtherScan'

    Logger.init_logger(app_name)
    logger = logging.getLogger('lib.ExecutionLogger')

    while True:
        try:
            engine_ = MonitorEngine(app_name)
            engine_.execute()
        except Exception as e:
            logger.critical("Alert !! execution error at main")
            logger.exception("Alert !! execution error at main")
            collected = gc.collect()
            logger.warning(f'garbage collected in main function : {collected}')
            logger.warning('continue execution in 5 mins')
            sleep(300)
            continue


if __name__ == '__main__':
    main()





