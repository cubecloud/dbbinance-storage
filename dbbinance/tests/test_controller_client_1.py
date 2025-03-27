from multiprocessing.managers import BaseManager


class ControllerClient(BaseManager):
    pass


if __name__ == '__main__':
    ControllerClient.register('get_id_counter')
    ControllerClient.register('set_id_counter')
    ControllerClient.register('increment_id_counter')

    client = ControllerClient(("127.0.0.1", 5990), authkey=b'password')
    client.connect()

    current_value = client.get_id_counter()
    print(f"Current ID Counter: {current_value}")

    new_value = 10
    client.set_id_counter(new_value)
    updated_value = client.get_id_counter()
    print(f"Updated ID Counter: {updated_value}")

    client.increment_id_counter()
    incremented_value = client.get_id_counter()
    print(f"Incremented ID Counter: {incremented_value}")

    client.env_values.z = 1
    values = client.env_values
    print(f"env_values: {values}")
