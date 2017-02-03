#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import json
import time
import xmltodict
import numpy
import MySQLdb
import time
import datetime


class RqAndRs(object):
    def __init__(self):
        self.hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08",
                      "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                      "21", "22", "23"]
        self.db2 = MySQLdb.connect(
            host="",
            db="",
            user="",
            passwd="",
            port=,
            charset='utf8'
        )
        self.cur2 = self.db2.cursor()

        self.insertTable = 'cat_respondse_time'

    def get_app1(self):
        sql3 = "select * from all_appids"
        cur3 = self.db2.cursor()
        cur3.execute(sql3)
        alldata = cur3.fetchall()
        app1 = []
        if alldata:
            for app in alldata:
                app1.append(app[0])
        return app1

    def inserttable(self, insertTable, insertId, insertDate, insertTime, insertCount, insertFail):
        insertContentSql = "INSERT INTO " + insertTable + "(appid1,time,respondse,request,failcount)VALUES(%s,%s,%s,%s,%s)"
        DB_insert = self.db2
        cursor_insert = DB_insert.cursor()
        cursor_insert.execute(insertContentSql, (insertId, insertDate, insertTime, insertCount, insertFail))
        DB_insert.commit()

    def get_daa(self):
            aaa = self.get_app1()
            timeStamp_1 = int(time.time())
            dateArray_1 = datetime.datetime.utcfromtimestamp(timeStamp_1)
            dayAgo1 = dateArray_1 + datetime.timedelta(hours =  8) - datetime.timedelta(days = 1)
            timeStamp1 = int(time.mktime(dayAgo1.timetuple()))
            otherStyleTime1 = dayAgo1.strftime("%Y%m%d%H%M%S")
            otherStyleTime = otherStyleTime1[:8]
            for a1 in set(aaa):
                print 'begin load' + str(a1)
                for q in self.hours:
                    url = r'http://xxx=%s&xx=%s%s&type=URL&forceDownload=xml' % (
                        str(a1), otherStyleTime, q)
                    try:
                        r = requests.get(url)
                        data = r.text
                        doc = xmltodict.parse(data)
                        appid = doc[u'transaction'][u'report'][u'@domain']
                        start = doc[u'transaction'][u'report'][u'@startTime']
                        start = start.replace('-', '').replace(' ', '').replace(':', '')
                        stime = start[:12]
                        for i in range(len(doc[u'transaction'][u'report'][u'machine'][u'type'][u'name'])):
                            if type(doc[u'transaction'][u'report'][u'machine'][u'type'][u'name']) == list:
                                adoc = doc[u'transaction'][u'report'][u'machine'][u'type'][u'name'][i][u'@id']
                            else:
                                adoc = doc[u'transaction'][u'report'][u'machine'][u'type'][u'name']
                            if adoc == u'All':
                                a = doc[u'transaction'][u'report'][u'machine'][u'type'][u'name'][i][u'range']
                                for j in range(len(a)):
                                    Appid = appid
                                    dtime = a[j][u'@avg']
                                    dateT = int(str(stime)) + int(a[j][u'@value'])
                                    dateT = str(dateT)
                                    timeArray = time.strptime(dateT, "%Y%m%d%H%M")
                                    timeStamp = int(time.mktime(timeArray))
                                    count = a[j][u'@count']
                                    fail = a[j][u'@fails']
                                    self.inserttable(self.insertTable, Appid, timeStamp, dtime, count, fail)
                    except:
                        pass