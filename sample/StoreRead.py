#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fbd_tools.etl.io.store

MyStore =  fbd_tools.etl.io.store.Store('/tmp/store4')

print(MyStore)

object = MyStore.get('hello' , '018')

print(object)
