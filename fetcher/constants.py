from typing import Tuple
from dataclasses import dataclass

__version__ = 0.019


@dataclass
class Constants:
    time_intervals = ['1m', '3m', '5m', '10m', '15m', '20m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d',
                      '1w', '1M']
    binsizes = {'1m': 1, '3m': 3, '5m': 5, '10m': 10, '15m': 15, '20m': 20, '30m': 30,
                '1h': 60, '2h': 120, '4h': 240, '6h': 360, '8h': 480, '12h': 720, '24h': 1440,
                '48h': 2880, '72h': 4320, '96h': 5760, '120h': 7200, '168h': 1080,
                '1d': 1440, '3d': 4320, '1w': 10080, '30d': 43200
                }

    default_datetime_format: str = "%Y-%m-%d %H:%M:%S"

    ohlcv_cols: Tuple = ('open_time', 'open', 'high', 'low', 'close', 'volume',)
    ohlcv_dtypes = {'open_time': int, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float}

    binance_cols: Tuple = ("open_time",
                           "open",
                           "high",
                           "low",
                           "close",
                           "volume",
                           "close_time",
                           "quote_asset_volume",
                           "trades",
                           "taker_buy_base",
                           "taker_buy_quote",
                           "ignored"
                           )

    binance_dtypes = {'open_time': int,
                      'open': float,
                      'high': float,
                      'low': float,
                      'close': float,
                      'volume': float,
                      'close_time': int,
                      'quote_asset_volume': float,
                      'trades': int,
                      'taker_buy_base': float,
                      'taker_buy_quote': float,
                      'ignored': float,
                      }

    sql_cols: Tuple = ("id",
                       "open_time",
                       "open",
                       "high",
                       "low",
                       "close",
                       "volume",
                       "close_time",
                       "quote_asset_volume",
                       "trades",
                       "taker_buy_base",
                       "taker_buy_quote",
                       "ignored"
                       )

    sql_dtypes = {"id": int,
                  'open_time': int,
                  'open': float,
                  'high': float,
                  'low': float,
                  'close': float,
                  'volume': float,
                  'close_time': int,
                  'quote_asset_volume': float,
                  'trades': int,
                  'taker_buy_base': float,
                  'taker_buy_quote': float,
                  'ignored': float,
                  }

    base_tables_names: tuple = ("spot_data",
                                # "futures_data"
                                )
