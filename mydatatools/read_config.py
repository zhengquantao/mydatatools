import os
from configparser import ConfigParser


configfile_path = os.path.join(".", 'config.ini')

assert os.path.exists(configfile_path), f"{os.getcwd()} 配置文件缺失"

cf = ConfigParser()
cf.read(configfile_path, encoding='utf-8')

RUN_FARM_CODES = cf.get('run_params', 'farm_codes')
RUN_TURBINES = cf.get('run_params', 'turbines')
RUN_START_DATE = cf.get('run_params', 'start_date')
RUN_END_DATE = cf.get('run_params', 'end_date')
RUN_ALARM_START_DATE = cf.get('run_params', 'alarm_start_date')

IB_HOST = cf.get('conn_IB', 'host')
IB_PORT = cf.getint('conn_IB', 'port')
IB_USER = cf.get('conn_IB', 'user')
IB_PASSWORD = cf.get('conn_IB', 'password')
IB_DB = cf.get('conn_IB', 'db')
IB_TABLE = cf.get('conn_IB', 'table_name')
IB_TURBINE_TYPE = cf.get('conn_IB', 'turbine_type')
IB_TABLE_FORMAT = cf.get('conn_IB', 'table_format')
IB_FIELD_FORMAT = cf.get('conn_IB', 'field_format')

BASE_HOST = cf.get('conn_base', 'host')
BASE_PORT = cf.getint('conn_base', 'port')
BASE_USER = cf.get('conn_base', 'user')
BASE_PASSWORD = cf.get('conn_base', 'password')
BASE_DB = cf.get('conn_base', 'db')

RESULT_HOST = cf.get('conn_result', 'host')
RESULT_PORT = cf.getint('conn_result', 'port')
RESULT_USER = cf.get('conn_result', 'user')
RESULT_PASSWORD = cf.get('conn_result', 'password')
RESULT_DB = cf.get('conn_result', 'db')
RESULT_TABLE = cf.get('conn_result', 'table_name')

SFTP_HOST = cf.get('conn_sftp', 'host')
SFTP_PORT = cf.getint('conn_sftp', 'port')
SFTP_USER = cf.get('conn_sftp', 'user')
SFTP_PASSWORD = cf.get('conn_sftp', 'password')
