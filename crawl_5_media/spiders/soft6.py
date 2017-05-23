# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.soft6_parser import *
from crawl_5_media.utils.redis_client import get_redis_client


class Soft6Spider(CrawlSpider):
    # 编码utf-8
    name = 'soft6'
    allowed_domains = ['soft6.com']
    start_url_list = [
		'http://www.soft6.com/news/',
        'http://cloud.soft6.com/',
        'http://safe.soft6.com/',
        'http://news.soft6.com/',
        'http://tech.soft6.com/',
    ]

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()

    def start_requests(self):
        soft6_parser = Soft6Parser()

        while self.redis_client.llen(self.key) > 0:
            queue_value = eval(self.redis_client.rpop(self.key))
            if type(queue_value) == dict:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                yield scrapy.Request(url, callback=soft6_parser.parse_detail_page, meta=request_meta)
                break
            else:
                url = queue_value
                yield scrapy.Request(url, callback=soft6_parser.parse_list_page)
