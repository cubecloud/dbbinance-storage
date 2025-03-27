from multiprocessing.managers import BaseManager
from multiprocessing import Process
import threading
import time
import unittest
import copy


class CacheSync(BaseManager):
    pass


class ControllerClient(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.manager = CacheSync(*args, **kwargs)
        self.manager.register('get_id_counter')
        self.manager.register('set_id_counter')
        self.manager.register('increment_id_counter')
        self.manager.register('env_values')
        self.manager.connect()


# Глобальная блокировка для защиты критической секции
global_lock = threading.RLock()


def worker_thread(client, process_num, thread_num):
    """Функция, выполняемая каждым потоком."""
    print(f"Thread {thread_num}: Starting work.")

    # with global_lock:
    current_value = copy.deepcopy(client.get_id_counter())
    print(f"Process/Thread: {process_num}/{thread_num} -> Current ID Counter: {current_value}")
    assert current_value >= 0, "ID counter should be non-negative."

    # with global_lock:
    incremented_value = copy.deepcopy(client.increment_id_counter())
    print(f"Process/Thread: {process_num}/{thread_num} -> Incremented ID Counter: {incremented_value}")
    assert incremented_value > current_value, "Increment ID counter failed."

    client.env_values.z
    print(f"Process/Thread: {process_num}/{thread_num} -> env_values.z: {z}")
    z += 1
    z = client.env_values.z
    print(f"Process/Thread: {process_num}/{thread_num} -> env_values.z: {z}")

    # new_value = 10
    # client.set_id_counter(new_value)
    # updated_value = copy.deepcopy(client.get_id_counter())
    # print(f"Thread {thread_num}: Updated ID Counter: {updated_value}")
    # assert updated_value == new_value, "Set ID counter failed."

    # client.increment_id_counter()
    # incremented_value = copy.deepcopy(client.get_id_counter())
    # print(f"Thread {thread_num}: Incremented ID Counter: {incremented_value}")
    # assert incremented_value == new_value + 1, "Increment ID counter failed."


def worker_process(process_num, client):
    """Функция, выполняемая каждым процессом."""
    print(f"Process {process_num}: Starting threads")
    threads = []
    for thread_num in range(3):
        thread = threading.Thread(target=worker_thread, args=(client, process_num, thread_num))
        threads.append(thread)
        thread.start()

    for t in threads:
        t.join()


class TestMultiprocessing(unittest.TestCase):
    def test_multiprocessing(self):
        controller = ControllerClient(("127.0.0.1", 5990), authkey=b'password')
        client = controller.manager

        # Защищаем проверку начального значения
        global_lock.acquire()
        try:
            initial_value = copy.deepcopy(client.get_id_counter())
            print(f"Initial ID Counter: {initial_value}")
            assert initial_value == 0, "Initial ID counter should be zero."
        finally:
            global_lock.release()

        processes = []
        for process_num in range(4):
            p = Process(target=worker_process, args=(process_num, client))
            processes.append(p)
            p.start()
            time.sleep(0.1)

        for p in processes:
            p.join()

        # Защищаем проверку финального значения
        global_lock.acquire()
        try:
            final_value = copy.deepcopy(client.get_id_counter())
            print(f"Final ID Counter: {final_value}")
            expected_final_value = 12
            self.assertEqual(final_value, expected_final_value, "Final ID counter mismatch.")
        finally:
            global_lock.release()


if __name__ == '__main__':
    unittest.main()
