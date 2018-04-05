#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import fbd_tools.etl.io.store
import os.path


class StoreTarget(luigi.Target):
    def __init__(self, base_path, source, state):
        print("creating fbd_tools.etl.io.store.Store object in directory: %s" %
              (base_path))
        self.store = fbd_tools.etl.io.store.Store(base_path)
        self.base_path = base_path
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
                    self.base_path,
                    self.source,
                    self.state)):
            return True
        else:
            return False


class HelloWorld(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return StoreTarget('/tmp/store1', "hello", "003")

    def run(self):
        print("running task...")
        self.output().write("Hello you")


if __name__ == '__main__':
    luigi.run()
