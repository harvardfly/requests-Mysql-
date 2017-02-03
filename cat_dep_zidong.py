#!/usr/bin/env python
# -*- coding:utf-8 -*-

from catdeppy import *

while True:
    if time.ctime()[11:19] == "00:05:00":
        a = catdeppy1()
        a.get_dependency()
