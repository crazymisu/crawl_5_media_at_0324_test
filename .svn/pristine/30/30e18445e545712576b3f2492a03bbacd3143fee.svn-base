# -*- coding: utf-8 -*-
import logging
import time
import MySQLdb
import pickle
from optparse import OptionParser
import os
from datetime import datetime
import socket
socket_hostname = socket.gethostname()

from crawl_5_media.localsettings import MYSQL_DBKWARGS, environment
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.utils.redis_client import get_redis_client

logging.basicConfig(filename='download_img_{0}_{1}.log'.format(socket_hostname, datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('download_img')
logger.setLevel(logging.INFO)


def get_option():
    parser = OptionParser()
    parser.add_option('-t', '--type', dest='feed_type')

    (options, args) = parser.parse_args()
    return (options, args)


def get_redis_info(sql_type):    
    key_prefix = 'queue:crawl_source:'
    redis_key = None

    redis_client = get_redis_client()

    if sql_type == 'toutiaohao_job_state_5':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_5'
    elif sql_type == 'test_job_state_3':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_3'
    elif sql_type == 'test_job_state_6':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_6'
    elif sql_type == 'test_job_state_4':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_4'
    elif sql_type == 'test_job_state_7':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_7'
    elif sql_type == 'test_job_state_8':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_8'    
    elif sql_type == 'test_job_state_9':
        redis_key = key_prefix + '5_media_url_info_' + environment + '_9'

    return redis_key, redis_client


def get_sql(sql_type):
    sql = None
    if sql_type == 'toutiaohao_job_state_5':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 5
                    AND basic.data_source_id LIKE 'CZZ-TTH%' 
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''    
    elif sql_type == 'test_job_state_6':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 6
                    AND basic.data_source_id LIKE 'CZZ-JH%' 
                    AND data_source_name LIKE "今日头条-%"
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''    
    elif sql_type == 'test_job_state_4':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 4
                    AND basic.data_source_id LIKE 'CZZ-TTH%' 
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''
    elif sql_type == 'test_job_state_7':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 7
                    AND basic.data_source_id LIKE 'CZZ-JH-%' 
                    AND basic.data_source_name LIKE '今日头条手机-%'
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''
    elif sql_type == 'test_job_state_8':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 8
                    AND basic.data_source_id LIKE 'CZZ-TTH%' 
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''
    elif sql_type == 'test_job_state_9':
        sql = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 9
                    AND basic.data_source_id LIKE 'CZZ-TTH%' 
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''
    return sql


def read_mysql(sql):
    DBKWARGS = MYSQL_DBKWARGS
    conn = MySQLdb.connect(**DBKWARGS)
    cur = conn.cursor()
    cur.execute(sql) 
    results = cur.fetchall()

    for row in results:
        data_source_id = row[0]
        data_source_name = row[1]
        basic_url = row[2]
        ishavedetillist = str(row[3])
        basic_category = row[4]
        basic_category_class = row[5]

        detail_data_source_class_id = row[6]
        detail_url = row[7]
        detail_categoryclass = row[8]
        detail_datasource_class = row[9]

        sent_kafka_message = SentKafkaMessage()

        sent_kafka_message['data_source_type'] = basic_category
        sent_kafka_message['data_source_category'] = detail_categoryclass
        sent_kafka_message['data_source_class'] = detail_datasource_class
        sent_kafka_message['data_source_class_id'] = detail_data_source_class_id
        sent_kafka_message['data_source_id'] = data_source_id
        sent_kafka_message['media'] = data_source_name
        sent_kafka_message['url'] = detail_url if ishavedetillist == '1' else basic_url

        if 'http' not in sent_kafka_message['url']:
            sent_kafka_message['url'] = 'http://' + sent_kafka_message['url']

        sent_kafka_message['crawlid'] = '27'
        sent_kafka_message['appid'] = 'crawl_big_media'
        sent_kafka_message['id'] = detail_url if ishavedetillist == '1' else basic_url                    
        sent_kafka_message['parse_function'] = 'parse_list_page'

        logger.info('feed list url data_source_id = {0} ; id = {1} ; crawlid = {2}'.format(data_source_id, sent_kafka_message['id'], sent_kafka_message['crawlid']))
        print('feed list url data_source_id = {0} ; id = {1} ; crawlid = {2}'.format(data_source_id, sent_kafka_message['id'], sent_kafka_message['crawlid']))
        yield sent_kafka_message

    cur.close()
    conn.close()


def feed_list_url_to_redis(sql_type):
    sql = get_sql(sql_type)
    redis_key, redis_client = get_redis_info(sql_type)

    if sql:
        for sent_kafka_message in read_mysql(sql):
            redis_client.lpush(redis_key, pickle.dumps(sent_kafka_message))


def test_feed_list_url(sql_type):
    test_data_source_id_list = ['CZZ-CNET',
                                'CZZ-KJXZ',
                                'CZZ-ZGRJW',
                                'CZZ-MTS',
                                'CZZ-WLWSJ',
                                'CZZ-AKJDW',
                                'CZZ-DMZJ',
                                'CZZ-QDW',
                                'CZZ-WWJDW',
                                'CZZ-ZDW',
                                'CZZ-ZJZX',
                                'CZZ-SZYLW',
                                'CZZ-GPLP',
                                'CZZ-BDSS-000052',
                                'CZZ-BDSS-000036',
                                'CZZ-BDSS-000010',
                                'CZZ-BDSS-000064',
                                'CZZ-BDSS-000078',
                                'CZZ-BDSS-000101',
                                'CZZ-BDSS-000108',
                                'CZZ-BDSS-000116'
                                ]

    sql_template = '''
                SELECT 
                    basic.data_source_id, 
                    basic.data_source_name,
                    basic.source_id,
                    basic.ishavedetillist, 
                    basic.category,
                    basic.category_class,

                    detail.datasource_class_id, 
                    detail.url, 
                    detail.categoryclass, 
                    detail.datasource_class
                    
                FROM basic_list as basic 
                LEFT JOIN basic_detil_list as detail 
                ON basic.data_source_id = detail.data_source_id
                WHERE 
                    basic.job_state = 1
                    AND basic.data_source_id LIKE '{0}%' 
                    AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
        '''

    redis_key, redis_client = get_redis_info(sql_type)

    for data_source_id in test_data_source_id_list:
        sql = sql_template.format(data_source_id)
        for sent_kafka_message in read_mysql(sql):
            redis_client.lpush(redis_key, pickle.dumps(sent_kafka_message))


if __name__ == '__main__':
    options, args = get_option()
    feed_type = options.feed_type

    feed_type_list = ['toutiaohao_job_state_5', 'test_job_state_3', 'test_job_state_4', 'test_job_state_6', 'test_job_state_4',
                        'test_job_state_7', 'test_job_state_8', 'test_job_state_9']

    feed_list_url_to_redis(feed_type)


    if feed_type == 'test_job_state_3':
        test_feed_list_url(feed_type)