# -*- coding: utf-8 -*-
import redis
import MySQLdb.cursors

"""
图片路径 从redis （哈希） 转存到 mysql(临时简陋版）
"""
__author__ = "kangkang"
__date__ = "2017-04-12"
if __name__ == '__main__':
    # 连接redis和mysql&………………参数均为外网ip
    REDIS_HOST = '112.124.17.26'  # '101.200.174.92' yuqing的redis
    REDIS_PORT = 6379
    REDIS_PWD = 'donews_1234'
    client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=0)
    DBKWARGS = {
        'host': '59.110.52.178',
        'user': 'admin001',
        'passwd': 'donews1234',
        'db': 'new_website',
        'charset': 'utf8'
    }
    conn = MySQLdb.connect(**DBKWARGS)
    cur = conn.cursor()

    image_redis_key = "article_photo_hash:hashes"
    count = client.hlen(image_redis_key)
    print count
    try:
        all_data = client.hscan_iter(image_redis_key)
        for each in all_data:
            try:
                url = each[0]
                address = each[1].split("/")
                file = address[2]
                path = "./" + "/".join(address[3:])
                end_path = each[1]
                item = (url, end_path, file, path)
                sql = """UPDATE `new_website`.`article_img`
                        SET `url`=%s ,`end_path`= %s WHERE `file`=%s and `path`=%s;"""

                cur.execute(sql, item)
                conn.commit()
            except Exception as e:
                print e
    except Exception as e:
        print e
    finally:
        if conn:
            conn.close()
