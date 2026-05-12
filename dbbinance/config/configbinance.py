import os

from secureapikey import Secure

__version__ = 0.005

# Mirror configpostgresql: graceful fallback when SALT decryption fails
# (no SALT env var set, non-interactive TTY, or secureapikey unavailable).
# In those cases consumers can provide BINANCE_API_KEY / BINANCE_API_SECRET
# directly via env vars — typical for CI / docker --env-file mounts.
try:
    secure_key = Secure()
    _binance_api_key, _binance_api_secret = secure_key.get_key(
        'BINANCE', use_env_salt=True,
    )
except Exception:
    _binance_api_key = os.getenv("BINANCE_API_KEY", "")
    _binance_api_secret = os.getenv("BINANCE_API_SECRET", "")


class ConfigBinance:
    BINANCE_API_KEY = _binance_api_key
    BINANCE_API_SECRET = _binance_api_secret
