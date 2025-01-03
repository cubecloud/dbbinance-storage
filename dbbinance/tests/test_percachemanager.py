import unittest
from dbbinance.fetcher.percachemanager import PERCacheManager
import time
import random
import string
import os
import multiprocessing
import logging

# Создаем уникальный ключ для тестов
AUTH_KEY = os.urandom(16)
HOST = 'localhost'
PORT = 5103
MAX_MEMORY_GB = 10

logger = multiprocessing.get_logger()


def generate_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


class TestPERCacheManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_manager = PERCacheManager(
            max_memory_gb=MAX_MEMORY_GB,
            start_host=True,
            host=HOST,
            port=PORT,
            authkey=AUTH_KEY,
            unique_name="TestCacheManager"  # Добавили unique_name
        )

    @classmethod
    def tearDownClass(cls):
        cls.cache_manager.shutdown()

    def test_update_and_get_with_score_update(self):
        # Запускаем несколько процессов для записи и чтения данных
        num_processes = 14
        results = []

        def worker(process_id):
            # for i in range(20):
            #     key = f'key_{process_id}_{i}'
            #     value = generate_random_string(20)
            #     self.cache_manager.update({key: value})
            #     # time.sleep(0.01)  # Немного подождем, чтобы имитировать реальную работу
            #
            #     logger.debug(f"{self.__class__.__name__}:  update {key}: {value}")
            #     retrieved_value = self.cache_manager.get(key)
            #     self.assertEqual(retrieved_value, value)
            #     logger.debug(f"{self.__class__.__name__}:  retrieved value{retrieved_value} / value {value}")
            #
            #     # Обновляем оценку для ключа
            #     new_score = random.uniform(0, 1)
            #     self.cache_manager.update_score(key, new_score)
            #     logger.debug(f"{self.__class__.__name__}:  update {key}: {new_score}")
            #
            #     # Проверка того, что оценка была обновлена
            #     with self.cache_manager.lock:
            #         self.assertEqual(self.cache_manager.score.get(key), new_score)
            #
            #     list(self.cache_manager.score_probs().keys())
            #
            # results.append(True)
            for i in range(546):
                key = f'key_{process_id}_{i}'
                value = generate_random_string(15)
                self.cache_manager.update({key: value})
                new_score = random.uniform(0, 1)
                self.cache_manager.update_score(key, new_score)

            probs_dict = self.cache_manager.score_probs()
            self.assertIsInstance(probs_dict, dict)
            self.assertGreater(len(probs_dict), 0)

            results.append(True)

        processes = [
            multiprocessing.Process(target=worker, args=(i,)) for i in range(num_processes)
        ]

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        self.assertTrue(all(results))


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('test_percachemanager.log')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    unittest.main()
