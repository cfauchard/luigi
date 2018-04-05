#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fbd_tools.etl.io.store

i = 5
MyStore =  fbd_tools.etl.io.store.Store('/tmp/store')

MyStore.set('integer', 'raw', i)
