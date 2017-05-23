# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
import logging
import time
import pickle
import MySQLdb

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.shenmezhidemai_parser import ShenMeZhiDeMaiParser  # debug
from crawl_5_media.localsettings import MYSQL_DBKWARGS, environment
from crawl_5_media.utils.redis_client import get_redis_client, check_hash_md5_url_at_redis

logging.basicConfig(filename='crawled_log_{0}.log'.format(str(int(time.time()))), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('ctoutiao')
logger.setLevel(logging.INFO)


class LcgTestSpider(CrawlSpider):
    name = 'crawler_test'

    def __init__(self, flag="list"):
        key_prefix = 'queue:crawl_source:'  # 需要特殊化队列名称，比如 'queue:crawl_source:wst:', 加上自己名字的开头拼音 debug
        self.key = key_prefix + 'shuyong_shenmezhide' + environment
        self.fail_url_redis_key = key_prefix + '5_media_fail_url_info_' + environment
        self.redis_client = get_redis_client()
        self.db_3_redis_client = get_redis_client(None, 3)
        self.database = 'dmt'
        self.collcetion = 'shuyong_shenmezhidemai_media_result'  # 特殊化为自己的表名，比如'wst_huxiu_media_result' ,开头前缀为自己的名字开头字母，debug
        self.flag = flag

        self.new_logger = logger

    def __read_mysql(self, keywords, condition='1', **kwargs):
        """从mysql查出需要抓取的源
        
        :param keywords: 过滤data_source_id的查询条件，如："CZZ-HX%".
        :param condition: 自定义的查询条件，如："data_source_name like 虎嗅-%".
        :param kwargs: mysql数据库连接参数,字典类型".
        """
        conn = MySQLdb.connect(**kwargs)
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
            WHERE basic.data_source_id LIKE "{0}" AND {1}
            AND (basic.source_id IS NOT NULL OR detail.url IS NOT NULL);
                                '''
        real_sql = sql.format(keywords, condition)
        cur.execute(real_sql)
        results = cur.fetchall()
        return results

    def start_requests(self):
        feed_url_flag = False
        crawl_detail_flag = False
        if self.flag == "list":
            feed_url_flag = True
        elif self.flag == "detail":
            crawl_detail_flag = True

        parser = ShenMeZhiDeMaiParser()  # 设置为自己的 parser 文件中的类，需要头部 import 自己的parser文件 debug
        # 抓取列表页
        if feed_url_flag:
            keywords = "CZZ-SMZDM%"  # 过滤出自己数据源的条件 debug
            # condition = "data_source_name like '虎嗅-%'"  # 如果不需要设置为 "1"即可 debug
            condition = "1"
            results = self.__read_mysql(keywords, condition, **MYSQL_DBKWARGS)
            crawl_url_index = 0

            for row in results:
                print "row[0],row[1]:", row[0], row[1]
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

                sent_kafka_message['crawlid'] = '27'
                sent_kafka_message['appid'] = 'crawl_big_media'
                sent_kafka_message['id'] = detail_url if ishavedetillist == '1' else basic_url
                sent_kafka_message['parse_function'] = 'parse_list_page'

                if '://' not in sent_kafka_message['url']:
                    sent_kafka_message['url'] = 'http://' + sent_kafka_message['url']

                request_meta = {}
                request_meta['queue_value'] = sent_kafka_message
                # import pdb
                # pdb.set_trace()
                print(request_meta)
                yield scrapy.Request(sent_kafka_message['url'], callback=parser.parse_list_page, meta=request_meta)

        if crawl_detail_flag:
            tmp_pop_count = self.redis_client.llen(self.key)
            # import pdb
            # pdb.set_trace()
            print "redis count:", tmp_pop_count  # test
            for i in range(int(tmp_pop_count)):
                tmp_pop = self.redis_client.rpop(self.key)
                print "tmp_pop:", i  # test
                # if i ==2:break # test
                queue_value = pickle.loads(tmp_pop)
                if type(queue_value) == SentKafkaMessage:
                    request_meta = {}
                    request_meta['queue_value'] = queue_value
                    print('+' * 30)
                    # import pdb
                    # pdb.set_trace()
                    print(request_meta)
                    print(queue_value['data_source_id'])
                    print('+' * 30)

                    url = queue_value['url']
                    yield scrapy.Request(url,
                         callback=getattr(parser, queue_value['parse_function']),
                         meta=request_meta)

    def parse(self, response):
        print(response.url)
        pass

