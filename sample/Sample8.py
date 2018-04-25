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

    #
    # Better to implement as a Store object method
    #
    def exists(self):
        if os.path.exists(
                os.path.join(
                    self.store.base_path,
                    self.source,
                    self.state)):
            return True
        else:
            return False


class Task_001(luigi.Task):
    store = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return StoreTarget(self.store, "hello1", "001")

    def run(self):
        print("%d: %s" % (os.getpid(), "running task Task_001..."))
        for i in range(0,10):
            print("%d: %s" % (os.getpid(), "Task_001 alive"))
            time.sleep(1)
        print("%d:%s" % (os.getpid(), "end task Task_001"))


class Task_002(luigi.Task):
    store = luigi.Parameter()
    source = "hello"
    state = "002"

    def requires(self):
        return None

    def output(self):
        return StoreTarget(self.store, "hello1", "002")

    def run(self):
        print("%d:%s" % (os.getpid(), " running task Task_002..."))
        for i in range(0,5):
            print("%d:%s" % (os.getpid(), "Task_002 alive"))
            time.sleep(1)
        print("%d:%s" % (os.getpid(), " end task Task_002"))
        

class Task_003(luigi.Task):
    store = luigi.Parameter()

    def requires(self):
        return {
            'Task_001': Task_001(store=self.store),
            'Task_002': Task_002(store=self.store)
        }

    def output(self):
        return StoreTarget(self.store, "output", "002")

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

    luigi.build(
        [
            Task_003(store=storeobj)
        ],
        local_scheduler=True,
        workers=2
    )
