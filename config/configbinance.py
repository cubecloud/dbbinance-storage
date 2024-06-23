from secure_apikey.secure import Secure

__version__ = 0.004

secure_key = Secure()
_binance_api_key, _binance_api_secret = secure_key.get_key('BINANCE', use_env_salt=True)


class ConfigBinance:
    BINANCE_API_KEY = _binance_api_key
    BINANCE_API_SECRET = _binance_api_secret
