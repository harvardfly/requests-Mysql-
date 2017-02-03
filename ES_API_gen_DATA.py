#!/usr/bin/python
# -*- coding: UTF-8 -*-

import json
import requests
import MySQLdb
import time
import datetime


class Get_ES_Data(object):
    def __init__(self):
        self.headers = {'content-type': 'application/json'}
        self.payload2 = {
            "access_token": "xxxxxxxxxxxxxxxxxxxxxxxxxx",
            "request_body": {

            }
        }

        self.url = r"http://xxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxxxxxxxx"
        self.re = requests.post(self.url, data=json.dumps(self.payload2), headers=self.headers)
        self.r = json.loads(self.re.text)

        self.db1 = MySQLdb.connect(
            host="localhost",
            db="",
            user="root",
            passwd="",
            port=3306,
            charset='utf8'
        )

        self.cur2 = self.db1.cursor()
        self.cur2.execute('drop table if exists table_deploy')

        self.sql1 = """create table table_deploy(id INT (11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT ,appid int not null
                                    ,timestamp VARCHAR(50)
                                  ,deploy_id INT
                                  )"""

        self.cur2.execute(self.sql1)
        self.db1.commit()

        self.insertTable = 'table_deploy'

    def inserttable(self, insertTable, insertId, insertTimestamp, insertDeploy_id):
        insertContentSql = "INSERT INTO " + insertTable + "(appid,timestamp,deploy_id)VALUES(%s,%s,%s)"
        DB_insert = self.db1
        cursor_insert = DB_insert.cursor()
        cursor_insert.execute(insertContentSql, (insertId, insertTimestamp, insertDeploy_id))
        DB_insert.commit()
        print 'inert contents to  ' + insertTable + ' successfully'

    def get_es(self):
        now_time = int(time.time())  # 当前时间的时间戳
        hoursAgo = (datetime.datetime.now() - datetime.timedelta(hours=8) - datetime.timedelta(hours=0.1))  # 0.1小时之前的时间
        hoursAgo_timeStamp = int(time.mktime(hoursAgo.timetuple()))  # 0.1小时之前的时间戳
        f = open('xxxxxxxxxx', 'w+')
        for i in range(len(self.r['hits']['hits'])):
            app_id = self.r['hits']['hits'][i]['_source']['app_id']
            timestamp = self.r['hits']['hits'][i]['_source']['@timestamp'].replace('Z', '').replace('T', ' ').replace(
                '.000', '')[:16]
            timeArray = time.strptime(timestamp, "%Y-%m-%d %H:%M")
            t = int(time.mktime(timeArray))  # 转化为时间戳
            if (t > hoursAgo_timeStamp and t < now_time):
                timeArray = time.strptime(timestamp, "%Y-%m-%d %H:%M")
                timestamp1 = \
                    self.r['hits']['hits'][i]['_source']['@timestamp'].replace('Z', '').replace('T', '').replace('.000',
                                                                                                                 '').replace(
                        '-', '').split(':')[0]
                temptime = time.strptime(timestamp1, "%Y%m%d%H")
                timeStamp_1 = int(time.mktime(temptime))
                dateArray_1 = datetime.datetime.utcfromtimestamp(timeStamp_1)
                timestamp1 = dateArray_1 + datetime.timedelta(hours=16)
                timestamp1 = timestamp1.strftime("%Y%m%d%H")
                timestamp2 = \
                    self.r['hits']['hits'][i]['_source']['@timestamp'].replace('Z', '').replace('T', '').replace('.000',
                                                                                                                 '').replace(
                        '-',
                        '').split(
                        ':')[1]
                timestamp3 = timestamp1 + timestamp2
                deploy_id = self.r['hits']['hits'][i]['_source']['deploy_id']
                self.inserttable(self.insertTable, app_id, timestamp1, deploy_id)
                f.write(str(app_id))
                f.write('\t')
                f.write(str(timestamp3))
                f.write('\t')
                f.write(str(deploy_id))
                f.write('\n')


a = Get_ES_Data()
a.get_es()
