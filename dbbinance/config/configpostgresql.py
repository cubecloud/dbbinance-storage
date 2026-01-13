import os
import logging
from secureapikey import Secure
from dbbinance.config.dockerized import validate_ip

__version__ = 0.013

logger = logging.getLogger()

logger.setLevel(logging.INFO)

secure_key = Secure()
_postgresql_user, _postgresql_password = secure_key.get_username_password('PSGSQL', use_env_salt=True)


# Decorator to initialize the HOST attribute upon class creation
def initialize_host_decorator(cls):
    cls.initialize_host()  # Call the initialization method when decorating the class
    return cls


# Class decorated with the initializer
@initialize_host_decorator
class ConfigPostgreSQL:
    """
    Configuration class for connecting to PostgreSQL.
    """
    DATABASE = "binance_data"  # Database name
    USER = _postgresql_user  # PostgreSQL username
    PASSWORD = _postgresql_password  # PostgreSQL password
    HOST = None  # Initial placeholder value
    PORT = 5432  # Default PostgreSQL port

    # Class method to initialize the HOST attribute
    @classmethod
    def initialize_host(cls):
        # Check for the PSGSQL_HOST_IP environment variable
        potential_host = os.getenv("PSGSQL_HOST_IP", "")
        if potential_host and validate_ip(potential_host):
            cls.HOST = potential_host
            logger.info(f"ConfigPostgreSQL: Using specified HOST IP: {cls.HOST}")
        elif potential_host:

            logger.warning(f"ConfigPostgreSQL: HOST IP specified: {potential_host}, "
                           f"not validated as IP. Please check it.")
            cls.HOST = potential_host
        else:
            cls.HOST = "localhost"
            logger.info(f"ConfigPostgreSQL: Straight run - HOST IP: {cls.HOST}")
