# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.ctoutiao_parser import CtoutiaoParser
from crawl_5_media.utils.redis_client import get_redis_client


class CtoutiaoSpider(CrawlSpider):
    name = 'test_ctoutiao'
    # allowed_domains = ['ctoutiao']

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()


    def start_requests(self):
        ctoutiao_parser = CtoutiaoParser()

        # while self.redis_client.llen(self.key) > 0:
        #     queue_value = eval(self.redis_client.rpop(self.key))
        #     if type(queue_value) == dict:
        #         request_meta = {}
        #         request_meta['queue_value'] = queue_value
        #         url = queue_value['url']
        #         yield scrapy.Request(url, callback=ctoutiao_parser.parse_detail_page, meta=request_meta)
        #         break
        #     else:
        #         url = queue_value
        #         yield scrapy.Request(url, callback=ctoutiao_parser.parse_list_page)
        start_url_list=[
            "http://www.ebrun.com/djk/",
            # "http://www.ebrun.com/qcds/",
            # "http://www.ebrun.com/fcds/",
            # "http://www.ebrun.com/whds/",
            # "http://www.ebrun.com/sxds/",
            # "http://www.ebrun.com/myds/",
            # "http://www.ebrun.com/jjds/",
            # "http://www.ebrun.com/hljr/",
            # "http://www.ebrun.com/ssds/",
            # "http://www.ebrun.com/dswl/",
            # "http://www.ebrun.com/retail/",
            # "http://www.ebrun.com/fto/",
            # "http://www.ebrun.com/mec/",
            # "http://www.ebrun.com/service/",
            # "http://www.ebrun.com/brands/",
            # "http://www.ebrun.com/intl/",
            # "http://www.ebrun.com/o2o/",
            # "http://www.ebrun.com/b2b/",
            # "http://www.ebrun.com/conference/",
            # "http://www.ebrun.com/policy/",
            # "http://www.ebrun.com/data/retail/",
            # "http://www.ebrun.com/data/brands/",
            # "http://www.ebrun.com/data/b2b/",
            # "http://www.ebrun.com/data/service/",
            # "http://www.ebrun.com/data/mec/",
            # "http://www.ebrun.com/data/o2o/",
            # "http://www.ebrun.com/data/fto/",
            # "http://www.ebrun.com/data/intl/",
            # "http://www.ebrun.com/data/more/",
            # "http://www.ebrun.com/data/earnings/",
        ]
        for url_ in start_url_list:
            # http://www.ebrun.com/djk/more.php?page=4#m
            for num in range(4,5):
                start_url=url_+"more.php?page="+str(num)
                print start_url
                yield scrapy.Request(start_url, callback=ctoutiao_parser.parse_list_page)

