from secureapikey import Secure
from dbbinance.config.dockerized import get_host_ip, is_running_in_docker

__version__ = 0.008

secure_key = Secure()
_postgresql_user, _postgresql_password = secure_key.get_username_password('PSGSQL', use_env_salt=True)


class ConfigPostgreSQL:
    if is_running_in_docker():
        HOST = get_host_ip()
    else:
        HOST = "localhost"

    DATABASE = "binance_data"
    USER = _postgresql_user
    PASSWORD = _postgresql_password
