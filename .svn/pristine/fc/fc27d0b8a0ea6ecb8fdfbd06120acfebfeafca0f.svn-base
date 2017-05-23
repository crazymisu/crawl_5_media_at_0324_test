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
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import SERVER_VIDEO_PATH, environment
from selenium import webdriver
from scrapy.http import HtmlResponse
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import random
from random import choice
import sys
reload(sys)
sys.setdefaultencoding('utf8')

"""
需要用phantomjs去加载列表页和详细页
翻页未做，比较困难，每次url都一样，需要用phantom去click实现，其实更新不多，一天抓一次就ok
详细页正文包含推荐等，剔除的方法比较恶心
"""
class Xinbang_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    ua_list = [
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/48.0.2564.82 Chrome/48.0.2564.82 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",
        "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36"
    ]
    dcap = dict(DesiredCapabilities.PHANTOMJS)
    dcap["phantomjs.page.settings.resourceTimeout"] = 5
    # dcap["phantomjs.page.settings.loadImages"] = False
    dcap["phantomjs.page.settings.userAgent"] = choice(ua_list)
    dcap["phantomjs.page.settings.localToRemoteUrlAccessEnabled"] = True

    # 在本地跑的时候，没有将exe放到path中
    driver = webdriver.PhantomJS(executable_path=r'C:\Users\pc\Desktop\phantomjs.exe', desired_capabilities=dcap)
    # 服务器上跑不需要设置exe的路径，默认存在
    # driver = webdriver.PhantomJS(desired_capabilities=dcap)

    def process_request(self, request):
        try:
            self.driver.get(request.url)
            self.driver.implicitly_wait(5)
            time.sleep(random.uniform(3, 6))
            page = self.driver.page_source  # .decode('utf-8','ignore')
            # close_driver()
            return HtmlResponse(request.url, body=page, encoding='utf-8', request=request, )

        except Exception:
            print "use middleware failed"

    def __del__(self):
        self.driver.close()


    def parse_list_page(self, response):
        try:
            response = self.process_request(response)
            newslsit = response.xpath('//*[@id="media-info-item"]/li').extract()
            for info in newslsit:
                new = Selector(text=info)
                original_sent_kafka_message = response.meta['queue_value']
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                url = new.xpath('//*[@class="media-main-left-news-info"]/h3/a/@href').extract_first()
                sent_kafka_message['url'] = url if url else None
                title = new.xpath('//*[@class="media-main-left-news-info"]/h3/a/text()').extract_first()
                sent_kafka_message['title'] = title if title else None
                author = new.xpath('//p/b[1]/text()').extract_first()
                sent_kafka_message['author'] = author if author else None
                publishtime = new.xpath('//p/b[2]/text()').extract_first()
                sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(
                    publishtime) if publishtime else None
                tags = new.xpath('//*[@class="media-main-left-news-info"]/p/span/text()').extract()
                sent_kafka_message['tags'] = tags if tags else None
                desc = new.xpath('//*[@class="media-main-left-news-info-p-text"]/a/text()').extract_first()
                sent_kafka_message['desc'] = desc if desc else None
                small_img = new.xpath('//*[@class="media-main-left-news-pic"]/a/@href').extract_first()
                check_flag, img_file_info = save_img_file_to_server(small_img, self.mongo_client, self.redis_client,self.redis_key,sent_kafka_message['publish_time'])
                if not check_flag:
                    sent_kafka_message['small_img_location'] = {'img_src': small_img, 'img_path': None,'img_index': 1, 'img_desc': None, 'img_width': None,'img_height': None}
                else:
                    sent_kafka_message['small_img_location'] = {'img_src': small_img,
                                                                'img_path': img_file_info['img_file_name'],
                                                                'img_index': 1, 'img_desc': None,
                                                                'img_width': img_file_info['img_width'],
                                                                'img_height': img_file_info['img_height']}
                sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            response = self.process_request(response)
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body_as_unicode()
            # 默认格式是固定的，the end图片是变化的，但是下一段紧接着就是固定的文字
            info = response.xpath(u'//*[contains(text(),"以上内容使用新榜编辑器")]/parent ::*/preceding-sibling ::*').extract()
            content = '<div class=\"detail-main-content\">' + ''.join(info[:-1]) + '</div>'
            article_content = self.extract_article_parsed_content(content,sent_kafka_message['publish_time'])
            sent_kafka_message['img_location'] = article_content[2]
            sent_kafka_message['img_location_count'] = article_content[3]
            sent_kafka_message['parsed_content_main_body'] = article_content[4]
            sent_kafka_message['parsed_content'] = article_content[0]
            sent_kafka_message['parsed_content_char_count'] = article_content[1]
            sent_kafka_message['authorized'] = None
            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())



