#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Sample4: Sample with two tasks, dependance and csv read
#

import luigi
import fbd_tools.etl.io.store
import pandas
import os.path


class Task_002_Target(luigi.Target):
    def __init__(self, store, source, state):
        self.store = store
        self.source = source
        self.state = state

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

        
class Task_001(luigi.ExternalTask):
    
    def output(self):
        return luigi.LocalTarget('Sample5.csv')

        
class Task_002(luigi.Task):
    store = luigi.Parameter()
    
    def requires(self):
        return Task_001()

    def output(self):
        return Task_002_Target(self.store, "hello", "018")

    def run(self):
        with self.input().open() as fd_input:
            dsobj = pandas.read_csv(fd_input)
        fd_input.close()

        print("running task Task_002...")
        self.output().write(dsobj)


if __name__ == '__main__':
    base_path = "/tmp/store4"
    print("creating fbd_tools.etl.io.store.Store object in directory: %s" %
          (base_path))
    storeobj = fbd_tools.etl.io.store.Store(base_path)
    
    luigi.build(
        [
            Task_002(store=storeobj)
        ],
        local_scheduler=True)
