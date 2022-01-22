import configparser
import os


class ConfigReader(object):

    parser = None
    conf_file = None

    @classmethod
    def init_conf(cls, app_name):
        cls.app_name = app_name
        cls.parser = configparser.ConfigParser(interpolation=None)
        cls.conf_file = os.path.join('.', 'config', f'{app_name}.properties')
        cls.parser.read(cls.conf_file)

    @classmethod
    def get_config(cls,section,attribute):
        return cls.parser[section][attribute].strip()