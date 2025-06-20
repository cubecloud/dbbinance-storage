import os
from secureapikey import Secure
from dbbinance.config.dockerized import get_host_ip, is_running_in_docker, validate_ip
from multiprocessing import get_logger

__version__ = 0.009

logger = get_logger()

secure_key = Secure()
_postgresql_user, _postgresql_password = secure_key.get_username_password('PSGSQL', use_env_salt=True)


class ConfigPostgreSQL:
    if is_running_in_docker():  # Проверяем, запущен ли код в Docker
        HOST = get_host_ip()  # Получаем IP хоста, если работаем в Docker
        logger.info(f"ConfigPostgreSQL: Dockerized instance detected - HOST IP: {HOST}")
    else:
        # Проверяем наличие переменной окружения PSG_HOST_IP
        potential_host = os.getenv("PSGSQL_HOST_IP", "")
        if potential_host and validate_ip(potential_host):
            HOST = potential_host
            logger.info(f"ConfigPostgreSQL: Using specified HOST IP: {HOST}")
        elif potential_host:
            logger.warning(f"Invalid HOST IP specified: {potential_host}, falling back to localhost.")
            HOST = "localhost"
        else:
            HOST = "localhost"
            logger.info(f"ConfigPostgreSQL: straight run - HOST IP: {HOST}")

    DATABASE = "binance_data"
    USER = _postgresql_user
    PASSWORD = _postgresql_password
