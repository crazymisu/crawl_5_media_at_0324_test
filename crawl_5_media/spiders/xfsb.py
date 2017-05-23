# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import re

from crawl_5_media.page_parser.xiaofeishibao_parser import XiaofeishibaoParser
from crawl_5_media.utils.redis_client import get_redis_client


class XiaofeishibaoSpider(CrawlSpider):
    name = 'xiaofeishibao'
    # allowed_domains = ['ctoutiao']

    def __init__(self):
        key_prefix = 'queue:crawl_source:'
        self.key = key_prefix + '5_media_url_info'
        self.redis_client = get_redis_client()


    def start_requests(self):
        xiaofeishibao_parser = XiaofeishibaoParser()

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
            "http://www.xdxfdb.cn/index.php/News/index/cid/8.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/9.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/10.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/11.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/12.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/13.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/14.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/15.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/16.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/17.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/18.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/19.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/20.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/21.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/22.html",
            "http://www.xdxfdb.cn/index.php/News/index/cid/23.html"
        ]

        for url_ in start_url_list:
            # http://www.xdxfdb.cn/index.php/News/index/cid/8.html
            # http://www.xdxfdb.cn/index.php/News/index/cid/8.html?cate_id=8&status=1&pageNum=2

            channel_id = re.search(r'\d+', url_, re.S).group()

            for num in range(1, 11):
                start_url = url_ + "?cate_id=" + channel_id + "&status=1&pageNum=" + str(num)
                print start_url
                yield scrapy.Request(start_url, callback=xiaofeishibao_parser.parse_list_page)

