#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Sample8: Sample with three tasks, two dependancies, store read,
# dependancy task parameter read and task parallelization
# run with python -u to disable buffering
#

import luigi
import fbd_tools.etl.io.store
import os.path
import pandas
import time
import os
import logging


def decorate(f):
    def wrapper(*args, **kwargs):
        arglist = list(args)
        instance = args[0]
        instance.logger.info('toto')
        f(*args, **kwargs)
    return wrapper


class StoreTarget(luigi.Target):
    def __init__(self, store, source, state):
        self.store = store
        self.source = source
        self.state = state

    def read(self):
        print("deserialize object...")
        return self.store.get(self.source, self.state)

    def write(self, object_to_serialize):
        print("serialize object...")
        self.store.set(self.source, self.state, object_to_serialize)

    def exists(self):
        return self.store.exists(self.source, self.state)

class Task_001(luigi.Task):
    store = luigi.Parameter()
    logger = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return StoreTarget(self.store, "hello", "003")

    @decorate
    def run(self):
        print("%d: %s" % (os.getpid(), "running task Task_001..."))
        for i in range(0,10):
            print("%d: %s" % (os.getpid(), "Task_001 alive"))
            time.sleep(1)
        print("%d:%s" % (os.getpid(), "end task Task_001"))


class Task_002(luigi.Task):
    store = luigi.Parameter()
    logger = luigi.Parameter()
    source = "hello"
    state = "002"

    def requires(self):
        return None

    def output(self):
        return StoreTarget(self.store, "hello", "004")

    @decorate
    def run(self):
        print("%d:%s" % (os.getpid(), " running task Task_002..."))
        for i in range(0,5):
            print("%d:%s" % (os.getpid(), "Task_002 alive"))
            time.sleep(1)
        print("%d:%s" % (os.getpid(), " end task Task_002"))
        

class Task_003(luigi.Task):
    store = luigi.Parameter()
    logger = luigi.Parameter()
    
    def requires(self):
        return {
            'Task_001': Task_001(store=self.store, logger=self.logger),
            'Task_002': Task_002(store=self.store, logger=self.logger)
        }

    def output(self):
        return StoreTarget(self.store, "output", "004")

    def run(self):
        df_task1 = self.input()['Task_001'].read()
        df_task2 = self.input()['Task_002'].read()

        print("running task Task_003...")

        print("Task:" +
              self.input()['Task_002'].source +
              self.input()['Task_002'].state)
        print("concat dataframes...")

        df_task3 = pandas.concat([df_task1, df_task2])
        self.output().write(df_task3)


if __name__ == '__main__':
    base_path = "/tmp/store4"
    print("creating fbd_tools.etl.io.store.Store object in directory: %s" %
          (base_path))
    storeobj = fbd_tools.etl.io.store.Store(base_path)
    logger=logging.getLogger("Sample8")
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler('Sample8.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.info('ouverture log')

    luigi.build(
        [
            Task_003(store=storeobj, logger=logger)
        ],
        local_scheduler=True,
        workers=2
    )
