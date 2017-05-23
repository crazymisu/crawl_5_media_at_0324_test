# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.huxiu_parser import HuxiuParser
from crawl_5_media.utils.redis_client import get_redis_client


class HuxiuSpider(CrawlSpider):
    name = 'huxiu'
    allowed_domains = ['huxiu.com']
    start_url_list = [
        'https://www.huxiu.com/channel/103.html',
        'https://www.huxiu.com/channel/22.html',
        'https://www.huxiu.com/channel/104.html',
        'https://www.huxiu.com/channel/21.html',
        'https://www.huxiu.com/channel/105.html',
        'https://www.huxiu.com/channel/111.html',
        'https://www.huxiu.com/channel/102.html',
        'https://www.huxiu.com/channel/4.html',
        'https://www.huxiu.com/channel/106.html',
        'https://www.huxiu.com/channel/107.html',
        'https://www.huxiu.com/channel/112.html',
        'https://www.huxiu.com/channel/110.html',
        'https://www.huxiu.com/channel/2.html'
    ]

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()

    
    def start_requests(self):
        huxiu_parser = HuxiuParser()

        while self.redis_client.llen(self.key) > 0:
            queue_value = eval(self.redis_client.rpop(self.key))
            if type(queue_value) == dict:
                request_meta = {}
                request_meta['queue_value'] = queue_value
                url = queue_value['url']
                yield scrapy.Request(url, callback=huxiu_parser.parse_detail_page, meta=request_meta)
                break
            else:
                url = queue_value
                yield scrapy.Request(url, callback=huxiu_parser.parse_list_page)
