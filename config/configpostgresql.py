from secureapikey.secureaes import Secure

__version__ = 0.006

secure_key = Secure()
_postgresql_user, _postgresql_password = secure_key.get_username_password('PSGSQL', use_env_salt=True)


class ConfigPostgreSQL:
    HOST = "localhost"
    DATABASE = "binance_data"
    USER = _postgresql_user
    PASSWORD = _postgresql_password
