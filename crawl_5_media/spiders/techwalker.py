# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.techwalker_parser import *
from crawl_5_media.utils.redis_client import get_redis_client


class TechwalkerSpider(CrawlSpider):
    name = 'techwalker'
    allowed_domains = ['techwalker.com']
    start_url_list = [
		'http://www.techwalker.com/?is_choice=1&page=1',
        'http://www.techwalker.com/?is_choice=1&page=2',
        'http://www.techwalker.com/?is_choice=1&page=3',
        'http://www.techwalker.com/?is_choice=1&page=4',
        'http://www.techwalker.com/?is_choice=1&page=5',
    ]

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()

    def start_requests(self):
        techwalker_parser = TechwalkerParser()

        while self.redis_client.llen(self.key) > 0:
            queue_value = eval(self.redis_client.rpop(self.key))
            if type(queue_value) == dict:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                yield scrapy.Request(url, callback=techwalker_parser.parse_detail_page, meta=request_meta)
                break
            else:
                url = queue_value
                yield scrapy.Request(url, callback=techwalker_parser.parse_list_page)
