# -*- coding: utf-8 -*-
import sys
reload(sys)

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re
import pickle
import MySQLdb
from datetime import datetime
import logging
import time
import pdb
from collections import OrderedDict
import traceback
import imp

import socket
socket_hostname = socket.gethostname()

from crawl_5_media.page_parser.fenghuangkeji import FengHuangParser
from crawl_5_media.page_parser.guizhou_parser import GuiZhou_parser
from crawl_5_media.page_parser.haowai_parser import Haowaicaijing_Parser
from crawl_5_media.page_parser.kufeng_parser import Kufeng_parser
from crawl_5_media.page_parser.yibangdongli_parser import YiBangDongLiParser
from crawl_5_media.page_parser.yiou_parser import YiouParser
from crawl_5_media.page_parser.qianlong_parser import QianlongParser
from crawl_5_media.page_parser.aiken_parser import Aiken_Parser
from crawl_5_media.page_parser.dongman_parser import Dongman_parser
from crawl_5_media.page_parser.qidi_parser import Qidi_parser
from crawl_5_media.page_parser.wanwei_parser import Wanwei_parser
from crawl_5_media.page_parser.zhidian_parser import Zhidian_parser
from crawl_5_media.page_parser.zhiding_parser import ZhiDing_Parser
from crawl_5_media.page_parser.jifang360_parser import Jifang360_Parser
from crawl_5_media.page_parser.bjtvnews_parser import Bjtvnews_Parser
from crawl_5_media.page_parser.ctiform_parser import Ctiform_Parser
from crawl_5_media.page_parser.d1net_parser import D1Net_Parser
from crawl_5_media.page_parser.toutiao_shouji_parser import ToutiaoShouji_Parser

from crawl_5_media.utils.redis_client import get_redis_client, check_hash_md5_url_at_redis
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.localsettings import MYSQL_DBKWARGS, environment

logging.basicConfig(filename='crawled_log_{0}_{1}.log'.format(socket_hostname, datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('ctoutiao')
logger.setLevel(logging.INFO)


class CtoutiaoSpider(CrawlSpider):
    name = 'ctoutiao'
    allowed_domains = ['ctoutiao']

    def __init__(self, crawler_type=None, test=None, source=None, save_mongo=None, database=None, collcetion=None, is_filter=None, job_state=1):
        key_prefix = 'queue:crawl_source:'
        self.job_state = str(job_state)

        key_suffix = '_' + self.job_state if self.job_state != '1' else ''
        self.key = key_prefix + '5_media_url_info_' + environment + key_suffix
        self.fail_url_redis_key = key_prefix + '5_media_fail_url_info_' + environment + key_suffix

        self.redis_proxy_key = key_prefix + 'proxies'
        self.redis_hash_set_key = 'crawl_5_media_url_hash:hash_set_' + environment        
        self.redis_client = get_redis_client()

        self.db_3_redis_client = get_redis_client(None, 3)

        self.crawler_type = crawler_type
        self.test = True if test and test.lower() == 'true' else False
        self.source = source
        self.save_mongo = save_mongo if save_mongo else True
        self.database = database
        self.collcetion = collcetion
        self.is_filter = False if is_filter and is_filter.lower() == 'false' else True        

        self.new_logger = logger


    def start_requests(self):

        self.new_logger.info('*' * 20)
        self.new_logger.info('redis key = {0}'.format(self.key))
        self.new_logger.info('redis fail_url_redis_key = {0}'.format(self.fail_url_redis_key))

        self.new_logger.info('redis key length = {0}'.format(self.redis_client.llen(self.key)))
        self.new_logger.info('redis fail_url_redis_key length = {0}'.format(self.redis_client.llen(self.fail_url_redis_key)))
        self.new_logger.info('*' * 20)

        parser_config_dic = self.read_parser_config_from_mysql()

        feed_url_flag = False
        crawl_detail_flag = False

        if self.crawler_type and self.crawler_type == 'feed_url':
            feed_url_flag = True
            crawl_detail_flag = False
        elif self.crawler_type and self.crawler_type == 'crawl_detail':
            feed_url_flag = False
            crawl_detail_flag = True

        if feed_url_flag:
            DBKWARGS = MYSQL_DBKWARGS
            conn = MySQLdb.connect(**DBKWARGS)
            cur = conn.cursor()

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
                    WHERE basic.job_state = {2} AND basic.data_source_id LIKE "{0}" {1} AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
            '''

            parse_list_page_function = 'parse_list_page'

            for parser_data_source_id_key in parser_config_dic.keys():
                like_keyword = parser_data_source_id_key + '%'
                try:
                    parser_info = parser_config_dic[parser_data_source_id_key]

                    real_sql = sql.format(like_keyword, 'AND ' + parser_info['remark'] if parser_info['remark'] else '', self.job_state)
                    cur.execute(real_sql) 
                    results = cur.fetchall()
                    
                    parser_obj = None
                    if 'parser_obj' in parser_info.keys() and parser_info['parser_obj']:
                        parser_obj = parser_info['parser_obj']
                    else:
                        parser_class = self.dynamic_load_parser_module(parser_info['module_path'], parser_info['module_name'])
                        parser_obj = parser_class()
                        parser_config_dic['parser_obj'] = parser_obj

                    crawl_url_index = 0

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

                        if self.test:
                            if self.source and self.source not in data_source_id:
                                continue

                        sent_kafka_message = SentKafkaMessage()

                        sent_kafka_message['data_source_type'] = basic_category
                        sent_kafka_message['data_source_category'] = detail_categoryclass
                        sent_kafka_message['data_source_class'] = detail_datasource_class
                        sent_kafka_message['data_source_class_id'] = detail_data_source_class_id
                        sent_kafka_message['data_source_id'] = data_source_id
                        sent_kafka_message['media'] = data_source_name
                        sent_kafka_message['url'] = detail_url if ishavedetillist == '1' else basic_url

                        sent_kafka_message['crawlid'] = '27'
                        sent_kafka_message['appid'] = 'crawl_big_media'
                        sent_kafka_message['id'] = detail_url if ishavedetillist == '1' else basic_url                    
                        sent_kafka_message['parse_function'] = 'parse_list_page'

                        request_meta = {}
                        request_meta['queue_value'] = sent_kafka_message

                        self.new_logger.info('feed list url data_source_id = {0} : id = {1} ; crawlid = {2}'.format(data_source_id, sent_kafka_message['id'], sent_kafka_message['crawlid']))
                        yield scrapy.Request(sent_kafka_message['url'], callback=getattr(parser_obj, parse_list_page_function), meta=request_meta, errback=self.request_err_back)
                except Exception as e:
                    self.new_logger.error(traceback.format_exc())
                    print(traceback.format_exc())

            cur.close()
            conn.close()

        while crawl_detail_flag:
            queue_value = None
            if self.redis_client.llen(self.key) == 0:
                if self.redis_client.llen(self.fail_url_redis_key) == 0:
                    time.sleep(10)
                    crawl_detail_flag = self.redis_client.llen(self.key) != 0
                    continue
                else:
                    queue_value = pickle.loads(self.redis_client.rpop(self.fail_url_redis_key))
            else:
                queue_value = pickle.loads(self.redis_client.rpop(self.key))

            if type(queue_value) == SentKafkaMessage:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                media = queue_value['media']
                data_source_id = queue_value['data_source_id']
                parse_function_name = queue_value['parse_function'] if 'parse_function' in queue_value.keys() else 'parse_detail_page'
                parse_function = None

                if self.test:
                    if not self.source in data_source_id:
                        continue

                if not parse_function_name or parse_function_name == 'parse_detail_page':
                    if self.is_filter and not check_hash_md5_url_at_redis(url, self.redis_client, self.redis_hash_set_key):
                        self.new_logger.info('check_hash_md5_url_at_redis data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3}; crawlid = {4}'.format(data_source_id, url, queue_value['id'], parse_function_name, queue_value['crawlid']))
                        continue

                for parser_data_source_id_key in parser_config_dic.keys():
                    if parser_data_source_id_key in data_source_id:                        
                        parser_info = parser_config_dic[parser_data_source_id_key]
                        parser_obj = None
                        if 'parser_obj' in parser_info.keys() and parser_info['parser_obj']:
                            parser_obj = parser_info['parser_obj']
                        else:
                            parser_class = self.dynamic_load_parser_module(parser_info['module_path'], parser_info['module_name'])
                            parser_obj = parser_class()
                            parser_config_dic['parser_obj'] = parser_obj
                        parse_function = getattr(parser_config_dic['parser_obj'], parse_function_name, getattr(parser_config_dic['parser_obj'], 'parse_detail_page'))
                        break

                self.new_logger.info('rpop redis queue data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3}; crawlid = {4}'.format(data_source_id, url, queue_value['id'], parse_function_name, queue_value['crawlid']))
                if parse_function and url:
                    yield scrapy.Request(url, callback=parse_function, meta=request_meta, errback=self.request_err_back)


    def read_parser_config_from_mysql(self):
        DBKWARGS = MYSQL_DBKWARGS
        conn = MySQLdb.connect(**DBKWARGS)
        cur = conn.cursor()

        parser_config_dic = OrderedDict()

        sql = '''
                SELECT 
                        parser_data_source_id_key, 
                        module_path, 
                        module_name, 
                        parser_media_name,
                        remark
                FROM big_media_dynamic_parser 
                WHERE job_state = {0}
                ORDER BY order_id DESC
        '''

        cur.execute(sql.format(self.job_state))
        results = cur.fetchall()

        for row in results:
            parser_data_source_id_key = row[0]
            module_path = row[1]
            module_name = row[2]
            parser_media_name = row[3]
            remark = row[4]

            parser_config_dic[parser_data_source_id_key] = {'module_path': module_path, 'module_name': module_name, 'parser_media_name': parser_media_name, 'remark': remark}

        cur.close()
        conn.close()

        return parser_config_dic


    def dynamic_load_parser_module(self, module_path, module_name):
        module_path_list = module_path.split('.')
        first_path = '.'.join(module_path_list[0:-1])
        page_parser_path = module_path_list[-2]
        last_path = module_path_list[-1]

        error_module_dic = {
                                'FengHuangParser': FengHuangParser,
                                'GuiZhou_parser': GuiZhou_parser,
                                'Haowaicaijing_Parser': Haowaicaijing_Parser,
                                'Kufeng_parser': Kufeng_parser,
                                'YiBangDongLiParser': YiBangDongLiParser,
                                'YiouParser': YiouParser,
                                'QianlongParser': QianlongParser,
                                'Aiken_Parser': Aiken_Parser,
                                'Dongman_parser': Dongman_parser,
                                'Qidi_parser': Qidi_parser,
                                'Wanwei_parser': Wanwei_parser,
                                'Zhidian_parser': Zhidian_parser,
                                'ZhiDing_Parser': ZhiDing_Parser,
                                'Jifang360_Parser': Jifang360_Parser,
                                'Bjtvnews_Parser': Bjtvnews_Parser,
                                'Ctiform_Parser': Ctiform_Parser,
                                'D1Net_Parser': D1Net_Parser,
                                'ToutiaoShouji_Parser': ToutiaoShouji_Parser
        }

        try:
            crawl_module = __import__(first_path)
            parser_module = getattr(crawl_module, page_parser_path)
            parser_class = getattr(parser_module, last_path)
            parser_class = getattr(parser_class, module_name)
            return parser_class
        except Exception as e:
            if module_name in error_module_dic.keys():
                parser_class = error_module_dic[module_name]
                return parser_class
            print(traceback.format_exc())
            raise e


    def request_err_back(self, failure):
        fail_request = failure.request
        sent_kafka_message = fail_request.meta['queue_value']
        if sent_kafka_message and isinstance(sent_kafka_message, SentKafkaMessage):
            self.redis_client.lpush(self.fail_url_redis_key, pickle.dumps(sent_kafka_message))
        self.new_logger.info('crawl fail url data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3} ; error_message = {4} ; crawlid = {5} ; response_url = {6}'.format(sent_kafka_message['data_source_id'], 
                                                                                                                                                                                          sent_kafka_message['url'], 
                                                                                                                                                                                          sent_kafka_message['id'], 
                                                                                                                                                                                          sent_kafka_message['parse_function'] if 'parse_function' in sent_kafka_message.keys() else 'parse_detail_page', 
                                                                                                                                                                                          repr(failure),
                                                                                                                                                                                          sent_kafka_message['crawlid'], 
                                                                                                                                                                                          fail_request.url))