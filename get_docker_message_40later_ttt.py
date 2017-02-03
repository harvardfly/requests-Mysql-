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


    def get_version_message(self):
        d = {}
        cur6 = self.db2.cursor()
        sql3 = "select * from vm_all_server WHERE role = 'DOCKER' AND env!='FWS'"
        cur6.execute(sql3)
        alldata = cur6.fetchall()
        ci_list = []
        ci_tuple = (0, 0, 0, 0)
        if alldata:
            for ci in alldata:
                ci_tuple = (ci[2], ci[9], ci[1], ci[13])
                ci_list.append(ci_tuple)

        dd = {}
        dd0 = []
        for ci in ci_list:
            d = {}

            url = 'http://xxxxxxxxxxxxxxxxxx/{}/xxx/?xxx={}&page=1&page_size=8&searchs'.format(
                ci[0], ci[1].lower())
            cii = ci[0]
            a_content = requests.get(url)
            aa = a_content.content
            a_json = json.loads(aa)
            application = []
            if a_json.has_key('results'):
                for i in range(len(a_json['results'])):
                    application.append(a_json['results'][i]['id'])

                print 'application:' + str(application)

                d0 = []
                for app in set(application):
                    d1 = {}
                    url1 = 'http://xxxxxxx/{}/groups/?env={}'.format(app, ci[1])
                    url2 = 'http://xxxxxxxx/{}/tars_prod_groups/'.format(app)
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
                        deploy_time = str(deploy_time).split(' ')[0]
                    else:
                        version_name = None
                        env = None
                        deploy_time = None
                    if len(a_json2) != 0:
                        if a_json2[0]['groups'][0]['latest_deployment'].has_key('package'):
                            prod_version_name = a_json2[0]['groups'][0]['latest_deployment']['package']['name']
                            prod_deploy_time = a_json2[0]['groups'][0]['latest_deployment']['created_at']
                            prod_deploy_time = str(prod_deploy_time).split(' ')[0]
                        else:
                            prod_version_name = None
                            prod_deploy_time = None
                    if prod_deploy_time and deploy_time != None:
                        time_difference = datetime.datetime.strptime(prod_deploy_time,
                                                                     '%Y-%m-%d') - datetime.datetime.strptime(
                            deploy_time, '%Y-%m-%d')
                    else:
                        time_difference = None
                    if time_difference!=None and time_difference.total_seconds()<=-3456000:
                        d1['ci_c'] = ci_c
                        d1['ip'] = ip
                        d1['env'] = env
                        d1['pd'] = pd
                        d1['appid'] = appid
                        d1['deploy_time'] = deploy_time
                        d1['prod_deploy_time'] = prod_deploy_time
                        d1['version_name'] = version_name
                        d1['prod_version_name'] = prod_version_name
                        d1['time_difference'] = str(time_difference)
                        d0.append(d1)
                d['content'] = d0
                dd0.append(d)
        dd['ci'] = cii
        dd['content'] = dd0
        print json.dumps(dd)

a = GETVERSION()
a.get_version_message()
