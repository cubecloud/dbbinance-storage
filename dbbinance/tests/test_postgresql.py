import logging

from dbbinance.fetcher import SQLMeta

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('test_postgresql.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logging.getLogger('apscheduler').setLevel(logging.DEBUG)

""" Start testing resample_to_timeframe method and 'cached' option"""
psg = SQLMeta(host='192.168.1.104',
              database='dragbase',
              user='draggame',
              password='Maxim_2010',
              )
psg.get_conn()
