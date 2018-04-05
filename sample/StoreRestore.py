#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fbd_tools.etl.io.store

MyStore =  fbd_tools.etl.io.store.Store('/tmp/store')

print(MyStore)

i = MyStore.get('integer' , 'raw')

print("i: %d" % (i))
