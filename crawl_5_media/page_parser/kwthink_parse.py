# -*- coding: utf-8 -*-
import scrapy
import re
from urlparse import urlparse, urlunparse
from datetime import datetime
import copy
import traceback
import json
import time
import requests
import urllib
import hashlib
import uuid
import os
from scrapy.selector import Selector
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import SERVER_VIDEO_PATH, environment
import sys
reload(sys)
sys.setdefaultencoding('utf8')

class Kwthink_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            newslsit = response.xpath('//*[@class="excerpt ias_excerpt"]').extract()
            for info in newslsit:
                new = Selector(text=info)
                original_sent_kafka_message = response.meta['queue_value']
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                url = new.xpath('//*[@class="desc"]/a/@href').extract_first()
                sent_kafka_message['url'] = url if url else None
                title = new.xpath('//div[@class="desc"]/a/text()').extract_first()
                sent_kafka_message['title'] = title if title else None
                author = new.xpath('//span[@class="name"]/text()').extract_first()
                sent_kafka_message['author'] = author if author else None
                publishtime = new.xpath('//span[@class="time"]/time/@datetime').extract_first()
                sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publishtime) if publishtime else None
                small_img = new.xpath('//*[@class="excerpt ias_excerpt"]/a[1]/@style').extract_first()
                small_img = small_img.split('src=')[1].split('&')[0] if small_img else None
                check_flag, img_file_info = save_img_file_to_server(small_img, self.mongo_client, self.redis_client,self.redis_key, sent_kafka_message['publish_time'])
                if not check_flag:
                    sent_kafka_message['small_img_location'] = {'img_src': small_img, 'img_path': None, 'img_index': 1, 'img_desc': None,'img_width': None, 'img_height': None}
                else:
                    sent_kafka_message['small_img_location'] = {'img_src': small_img, 'img_path': img_file_info['img_file_name'], 'img_index': 1,'img_desc': None, 'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height']}
                sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            sent_kafka_message['status_code'] = response.status
            tag = response.xpath("//*[@class='single-post-tags']/a/text()").extract()
            sent_kafka_message['tags'] = tag if tag else None
            click_count = response.xpath("//*[@class='author single-post-meta']/span[2]/text()").extract_first()
            sent_kafka_message['click_count'] = int(click_count.split(u'浏览')[0]) if click_count else None
            sent_kafka_message['body'] = response.body_as_unicode()
            content = response.xpath("//*[@class='article']").extract_first()
            article_content = self.extract_article_parsed_content(content, sent_kafka_message['publish_time'])
            sent_kafka_message['parsed_content_main_body'] = article_content[4].split(u'延伸阅读')[0].split(u'声明：')[0].split(u'持续不断为餐饮业贡献深度硬干货')[0]
            sent_kafka_message['parsed_content'] = article_content[0].split(u'<p>延伸阅读')[0].split(u'<p>声明：')[0].split(u'<p>持续不断为餐饮业贡献深度硬干货')[0]
            sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content'])
            sent_kafka_message['authorized'] = response.xpath("//*[@class='article']/p/strong/text()").extract()[-1]
            sent_kafka_message['img_location'] = article_content[2]
            sent_kafka_message['img_location_count'] = article_content[3]
            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())



