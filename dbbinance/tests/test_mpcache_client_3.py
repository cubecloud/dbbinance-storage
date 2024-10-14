from re import M
import sys
import objsize
import time
import datetime

from mlthread_tools.multiprocessing_mutex import mlp_mutex
from multiprocessing import Process, Manager, Value
import pandas as pd

from dbbinance.fetcher import MpCacheManager
import multiprocessing

class TestEnv():
    def __init__(self, process_id):
        self.process_id=process_id 
        self.manager_obj=MpCacheManager(port=5500, start_host=False)
        self.conn=None
    
    def step(self):
        return self.manager_obj.get(list(self.manager_obj.hits_probs().keys())[0])
    def update_cache(self):
        for i in range(3):
            key = f"key_{self.process_id}_{i}"
            value = f"value_{self.process_id}_{i}"
            self.manager_obj.update({key: value})
            print(f"Process {self.process_id} updated cache: {key} = {value}")
            
    def run(self, conn):
        self.conn=conn
        self.update_cache()
        self.conn.send(self.step())  # send data from env to parent process
        self.conn.close()


def run_client():
    processes = []
    pipes = []
    for i in range(multiprocessing.cpu_count() - 2):
        parent_conn, child_conn = multiprocessing.Pipe()
        env = TestEnv(i)
        p = multiprocessing.Process(target=env.run, args=(child_conn,))
        processes.append(p)
        pipes.append(parent_conn)
        p.start()
    for p in processes:
        p.join()
    for pipe in pipes:
        print(pipe.recv())
    
    manager = MpCacheManager(port=5500, start_host=False)
    print("Final cache contents:")
    for key, value in manager.items():
        print(f"{key} = {value}")


if __name__ == '__main__':
    run_client()
