import os
from socket import gethostname, getaddrinfo
import re


def get_host_ip():
    try:
        # Retrieve the hostname of the local machine
        hostname = gethostname()

        # Get a list of IP addresses associated with the hostname
        ip_addresses = getaddrinfo(hostname, None)

        # Extract the first IP address
        return ip_addresses[0][4][0]

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def validate_ip(ip_address):
    """Проверяет, является ли строка корректным IP-адресом."""
    pattern = r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$'
    return bool(re.match(pattern, ip_address))


def is_pythonunbuffered():
    pythonunbuffered = os.getenv("PYTHONUNBUFFERED", "").strip().lower()
    if pythonunbuffered in ('1', 'true', 'yes'):
        return True
    else:
        return False
