import sys
import objsize
import time
import datetime

from multiprocessing import Process, Manager, Value
from threading import RLock
from dbbinance.fetcher import MpCacheManager
import multiprocessing


class TestEnv:
    def __init__(self, process_id, env_idx, lock):
        self.process_id = process_id
        self.env_idx = env_idx
        self.manager_obj = MpCacheManager(port=5500, start_host=False, unique_name='train', th_rlock=lock)
        self.conn = None
        print(f'#{self.process_id} {self.env_idx} {id(self.manager_obj)} - initialized', flush=True)

    def step(self):
        key = list(self.manager_obj.hits_probs().keys())[0]
        return {key: self.manager_obj.get(key)}

    def probs(self):
        return self.manager_obj.hits_probs()

    def update_cache(self):
        for i in range(2):
            key = f"key_{self.process_id}_{i}"
            value = f"value_{self.process_id}_{i}"
            self.manager_obj.update({key: value})
            print(f"Process {self.process_id} updated cache: {key} = {value}")

    # def run(self, conn):
    #     self.conn = conn
    #     self.update_cache()
    #     self.conn.send(self.step())  # send data from env to parent process
    #     self.conn.close()


def _worker(env_cls, conn, process_id, lock):
    envs = {}
    for env_idx in range(1):
        envs.update({env_idx: env_cls(process_id, env_idx, lock)})
        # envs[env_idx].update_cache()

    done = False
    while not done:
        for env_idx in range(1):
            command = conn.recv()
            if command == 'step':
                data = envs[env_idx].step()
                print(f'#{process_id} - step', data, flush=True)
                conn.send(data)

            if command == 'probs':
                data = envs[env_idx].probs()
                print(f'#{process_id} - probs', data, flush=True)
                conn.send(data)

            elif command == 'done':
                print(f'#{process_id} - done', flush=True)
                done = True

    conn.close()


def run_client(n_envs=2):
    processes = []
    parent_pipes = []
    lock = RLock()
    for process_id in range(n_envs):

        parent_conn, child_conn = multiprocessing.Pipe()
        p = multiprocessing.Process(target=_worker, args=(TestEnv, child_conn, process_id, lock))
        processes.append(p)
        parent_pipes.append(parent_conn)
        p.start()

    # done = False
    for _ in range(3):
        indices = [1]
        command = ('step', indices)
        for ix, pipe in enumerate(parent_pipes):
            if ix in indices:
                pipe.send(command[0])

        for ix, pipe in enumerate(parent_pipes):
            if ix in indices:
                print(pipe.recv())

    indices = [0, 1, 2]
    command = ('probs', indices)
    for ix, pipe in enumerate(parent_pipes):
        if ix in indices:
            pipe.send(command[0])

    for ix, pipe in enumerate(parent_pipes):
        if ix in indices:
            print(pipe.recv())

    indices = [0, 1, 2]
    command = ('done', indices)
    for ix, pipe in enumerate(parent_pipes):
        if ix in indices:
            pipe.send(command[0])

    for ix, pipe in enumerate(parent_pipes):
        if ix in indices:
            pipe.close()

    for p in processes:
        p.join()

    manager = MpCacheManager(port=5500, start_host=False, unique_name='train', th_rlock=lock)
    print("Final cache contents:")

    for key, value in manager.items():
        print(f"{key} = {value}")


if __name__ == '__main__':
    run_client(3)
