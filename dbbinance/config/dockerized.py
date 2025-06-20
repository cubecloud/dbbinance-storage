from socket import gethostname, getaddrinfo


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


def is_running_in_docker():
    try:
        with open("/proc/self/cgroup") as f:
            content = f.read()
            return "/docker/" in content or "/crio/" in content
    except OSError:
        return False
