# -*- coding: utf-8 -*-
import redis
import pymongo
import MySQLdb.cursors
import json
from pymongo import MongoClient
"""
图片路径 从mongo 转存到 mysql
"""

if __name__ == '__main__':
    MONGO_CONN_STR = 'mongodb://access_mongo:donewsusemongodbby20170222@59.110.52.111:27017,47.93.84.131:27017,47.93.90.170:27017/admin?readPreference=secondaryPreferred'
    client = MongoClient(MONGO_CONN_STR)
    db = client.toutiao.toutiao_video

    DBKWARGS = {
        'host': '59.110.52.178',
        'user': 'admin001',
        'passwd': 'donews1234',
        'db': 'new_website',
        'charset': 'utf8'
    }
    conn = MySQLdb.connect(**DBKWARGS)
    cur = conn.cursor()

    all_data = []
    scroll_size = 100
    find_query = {'downimg_location':{'$exists':1}}
    find_result = db.find(find_query).sort('_id', -1).limit(scroll_size)
    cc = db.find(find_query).count()
    print cc
    result_count = find_result.count()
    while result_count > 0:
        result_count = result_count - scroll_size
        for item in find_result:
            s = dict()
            s['url'] = item['large_image_list'][0]['url']
            s['location'] = item['downimg_location']
            all_data.append(s)
        skip_size = find_result.count() - result_count
        find_result.close()
        find_result = db.find(find_query).sort('_id', -1).skip(skip_size).limit(scroll_size)
    find_result.close()

    for i in all_data:
        try:
            url =i['url']
            address = i['location'].split("/")
            file =address[2]
            path = "./"+"/".join(address[3:])
            end_path = i['location']
            item = (url,end_path,file,path)
            sql = """UPDATE `new_website`.`toutiao_video_img`
                                            SET `url`=%s ,`end_path`= %s WHERE `file`=%s and `path`=%s;"""
            cur.execute(sql,item)
            conn.commit()
        except Exception as e:
            print e

    conn.close()
