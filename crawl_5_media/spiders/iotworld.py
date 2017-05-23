# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.iotworld_parser import *
from crawl_5_media.utils.redis_client import get_redis_client


class IotworldSpider(CrawlSpider):
    # 编码utf-8
    name = 'iotworld'
    allowed_domains = ['iotworld.com']
    start_url_list = [
		'http://www.iotworld.com.cn/IOTNews/List-20-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-1-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-3-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-4-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-5-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-6-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-7-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-8-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-9-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-10-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-13-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-14-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-30-0-0-1.aspx',
        'http://www.iotworld.com.cn/IOTNews/List-31-0-0-1.aspx',

    ]

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()

    def start_requests(self):
        iotworld_parser = IotworldParser()

        while self.redis_client.llen(self.key) > 0:
            queue_value = eval(self.redis_client.rpop(self.key))
            if type(queue_value) == dict:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                yield scrapy.Request(url, callback=iotworld_parser.parse_detail_page, meta=request_meta)
                break
            else:
                url = queue_value
                yield scrapy.Request(url, callback=iotworld_parser.parse_list_page)
