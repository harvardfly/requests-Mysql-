#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import xmltodict
import MySQLdb
import datetime


class catdeppy1(object):
    def __init__(self):
        self.db2 = MySQLdb.connect(
            host="",
            db="",
            user="",
            passwd="",
            port=,
            charset='utf8'
        )
        self.hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08",
                      "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                      "21", "22", "23"]

        self.cur2 = self.db2.cursor()

        self.insertTable = 'cat_dependency_EveryMinute'

    def inserttable(self, insertTable, insertId1, insertId2, insertCount1, insertTime):
        insertContentSql = "REPLACE INTO " + insertTable + "(appid1,appid2,count,time)VALUES(%s,%s,%s,%s)"
        DB_insert = self.db2
        cursor_insert = DB_insert.cursor()
        cursor_insert.execute(insertContentSql, (insertId1, insertId2, insertCount1, insertTime))
        DB_insert.commit()

    def now_ti(self):
        now = int(time.time())
        loc_after = time.localtime(now)
        loc1_after = time.strftime('%Y-%m-%d %H:%M', loc_after)
        datetimeObj_After = datetime.datetime.strptime(loc1_after, "%Y-%m-%d %H:%M")
        now2 = datetimeObj_After - datetime.timedelta(days=1)
        now21 = now2.strftime("%Y%m%d%H%M")[:8]
        return now21

    def get_dependency(self):
        sql3 = "select * from all_appids"
        self.cur2.execute(sql3)
        alldata = self.cur2.fetchall()
        a_now = self.now_ti()
        for app in alldata:
            b = []
            print 'begin load dependency' + str(app)
            for hour in self.hours:
                url = 'http://xxxxxxxxxxdoxxx={}&date={}{}&forceDownload=xml'.format(
                    app[0], a_now, hour)
                try:
                    r = requests.get(url)
                    data = r.text
                    doc = xmltodict.parse(data)
                    for i in range(len(doc['dependency']['report']['segment'])):
                        for j in range(len(doc['dependency']['report']['segment'][i]['dependency'])):
                            time = doc['dependency']['report']['@startTime'].split(' ')[0]
                            if doc['dependency']['report']['segment'][i]['dependency'][j]['@type'] == 'Service':
                                appid2 = doc['dependency']['report']['@domain']
                                appid1 = doc['dependency']['report']['segment'][i]['dependency'][j]['@target']
                                count = doc['dependency']['report']['segment'][i]['dependency'][j]['@total-count']
                            elif 'Call' in doc['dependency']['report']['segment'][i]['dependency'][j]['@type']:
                                appid2 = doc['dependency']['report']['segment'][i]['dependency'][j]['@target']
                                appid1 = doc['dependency']['report']['@domain']
                                count = doc['dependency']['report']['segment'][i]['dependency'][j]['@total-count']
                            g = [appid1, appid2, count, time]
                            b.append(g)
                except:
                    pass
            try:
                c = [(0, 0, 0)]
                for n in b:
                    for i in range(0, len(c)):
                        if n[0] == c[i][0] and n[1] == c[i][1]:
                            c[i][2] = int(c[i][2]) + int(n[2])
                            break
                        elif i == len(c) - 1:
                            c.append(n)
                            break

                for i in range(1, len(c)):
                    self.inserttable(self.insertTable, c[i][0], c[i][1], c[i][2], c[i][3])
            except:
                pass
        self.cur2.close()
        self.db2.close()
