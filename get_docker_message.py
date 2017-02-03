# -*- coding: utf-8 -*-
import requests
import json
import traceback
import MySQLdb
import datetime
import time


class GETVERSION(object):
    def __init__(self):
        self.db2 = MySQLdb.connect(
            host="",
            db="",
            user="",
            passwd="",
            port=3306,
            charset='UTF8'
        )
        self.cur2 = self.db2.cursor()
        self.cur2.execute('drop table if exists vm_version_difference')

        self.sql2 = """create table vm_version_difference(id INT (11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,ci_c varchar(20) not null
                                 ,ip varchar(20)
                                ,env VARCHAR(20)
                              ,pd VARCHAR (20)
                              ,appid VARCHAR (20)
                              ,deploy_time VARCHAR (20)
                              ,prod_deploy_time VARCHAR (20)
                              ,version_name VARCHAR (500)
                              ,prod_version_name VARCHAR (500)
                              ,time_difference VARCHAR (20)
                              ,UNIQUE(appid)
                              )"""
        self.cur2.execute(self.sql2)
        self.db2.commit()
        self.insertTable = 'vm_version_difference'

    def inserttable(self, insertTable, insertci_c, insertip, insertenv, insertpd, insertappid, insertdeploy_time,
                    insertprod_deploy_time, insertversion_name, insertprod_version_name, inserttime_difference):
        insertContentSql = "REPLACE INTO " + insertTable + "(ci_c,ip,env,pd,appid,deploy_time,prod_deploy_time,version_name,prod_version_name,time_difference)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        DB_insert = self.db2
        cursor_insert = DB_insert.cursor()
        cursor_insert.execute(insertContentSql, (
        insertci_c, insertip, insertenv, insertpd, insertappid, insertdeploy_time, insertprod_deploy_time,
        insertversion_name, insertprod_version_name, inserttime_difference))
        DB_insert.commit()
        print 'inert contents to  ' + insertTable + ' successfully'

    def get_version_message(self):
        cur6 = self.db2.cursor()
        sql3 = "select * from vm_all_server WHERE role = 'DOCKER'"
        cur6.execute(sql3)
        alldata = cur6.fetchall()
        ci_list = []
        ci_tuple = (0, 0, 0, 0)
        if alldata:
            for ci in alldata:
                ci_tuple = (ci[2], ci[9], ci[1], ci[13])
                ci_list.append(ci_tuple)

        for ci in ci_list:
            url = 'http://xxxxxxx/{}/xxx/?xxx={}&page=1&page_size=8&searchs'.format(
                ci[0], ci[1].lower())
            a_content = requests.get(url)
            aa = a_content.content
            a_json = json.loads(aa)
            application = []
            if a_json.has_key('results'):
                for i in range(len(a_json['results'])):
                    application.append(a_json['results'][i]['id'])

                print 'application:' + str(application)

                for app in set(application):
                    url1 = 'http://xxxxxxxxxx/{}/groups/?env={}'.format(app, ci[1])
                    url2 = 'http://xxxxxxxxxxx/{}/tars_prod_groups/'.format(app)
                    a_content1 = requests.get(url1)
                    aa1 = a_content1.content
                    a_json1 = json.loads(aa1)
                    a_content2 = requests.get(url2)
                    aa2 = a_content2.content
                    a_json2 = json.loads(aa2)
                    appid = app
                    ci_c = ci[0]
                    ip = ci[2]
                    pd = ci[3]
                    if a_json1[0]['latest_release'] != None:
                        version_name = a_json1[0]['latest_release']['package']['name']
                        env = a_json1[0]['latest_release']['package']['env']
                        deploy_time = a_json1[0]['latest_release']['created_at']
                    else:
                        version_name = None
                        env = None
                        deploy_time = None
                    if len(a_json2) != 0:
                        if a_json2[0]['groups'][0]['latest_deployment'].has_key('package'):
                            prod_version_name = a_json2[0]['groups'][0]['latest_deployment']['package']['name']
                            prod_deploy_time = a_json2[0]['groups'][0]['latest_deployment']['created_at']
                        else:
                            prod_version_name = None
                            prod_deploy_time = None
                    if prod_deploy_time and deploy_time != None:
                        time_difference = datetime.datetime.strptime(prod_deploy_time,
                                                                     '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(
                            deploy_time, '%Y-%m-%d %H:%M:%S')
                    else:
                        time_difference = None
                    print prod_version_name
                    self.inserttable(self.insertTable, ci_c, ip, env, pd, appid, deploy_time, prod_deploy_time,
                                     version_name, prod_version_name, time_difference)
        self.db2.close()

a = GETVERSION()
a.get_version_message()
