# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.prnasia_parser import *
from crawl_5_media.utils.redis_client import get_redis_client


class PrnasiaSpider(CrawlSpider):
    # 编码utf-8
    name = 'prnasia'
    allowed_domains = ['prnasia.com']
    start_url_list = [
		'http://www.prnasia.com/p/group-all-1.shtml',
        'http://www.prnasia.com/p/public-theme-1-0.shtml',
        'http://www.prnasia.com/p/lightnews-all-1.shtml',
    ]

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()

    def start_requests(self):
        prnasia_parser = PrnasiaParser()

        while self.redis_client.llen(self.key) > 0:
            queue_value = eval(self.redis_client.rpop(self.key))
            if type(queue_value) == dict:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                yield scrapy.Request(url, callback=prnasia_parser.parse_detail_page, meta=request_meta)
                break
            else:
                url = queue_value
                yield scrapy.Request(url, callback=prnasia_parser.parse_list_page)
