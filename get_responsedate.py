# -*- coding: utf-8 -*-

from appid_response import * 
while True:
    if time.ctime()[11:19]=="00:45:00":
        a = RqAndRs()
        a.get_daa()