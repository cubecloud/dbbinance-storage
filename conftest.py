from dotenv import load_dotenv
import os

_base_dir = os.path.dirname(__file__)

load_dotenv(os.path.join(_base_dir, "PSGSQL_KEY.env"))
load_dotenv(os.path.join(_base_dir, "PSGSQLKEYS.env"))
load_dotenv(os.path.join(_base_dir, "BINANCE_KEY.env"))
load_dotenv(os.path.join(_base_dir, "BINANCEKEYS.env"))
