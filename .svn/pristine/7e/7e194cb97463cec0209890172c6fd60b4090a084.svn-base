# -*- coding: utf-8 -*-
from urlparse import urlparse, urlunparse
from datetime import datetime
import copy
import traceback
import re
from datetime import datetime, timedelta
import logging
import pymongo
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class YiouParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']

            """分页机制，太多，130左右"""
            if IS_CRAWL_NEXT_PAGE and 'html' not in response.url:
                pages = response.xpath("//*[@id='page-nav']/li/a/text()").extract()
                page = pages[-1]
                # for i in range(2, int(page)):
                for i in range(2, 11):
                    url = response.url + '/page/' + str(i) + '.html'
                    parse_function = 'parse_list_page'
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    sent_kafka_message['url'] = url
                    sent_kafka_message['parse_function'] = parse_function if parse_function else None
                    yield sent_kafka_message
                    
            img_list = response.xpath("//div[@class='img fl']/a/img/@src").extract()
            url_list = response.xpath("//div[@class='text fl']/a/@href").extract()
            n = -1
            for url in url_list:
                n += 1
                item = dict()
                item['url'] = response.url
                """ 缩略图先传递到详情页，等解析时间后在处理"""
                small_img_location = response.xpath("//div[@class='img fl']/a/img/@src").extract()[n]
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] = url
                sent_kafka_message['small_img_location'] = small_img_location
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
            sent_kafka_message['title'] = response.xpath("//*[@id='post_title']/text()").extract_first()
            sent_kafka_message['body'] = response.body_as_unicode()
            """时间特别乱，需要调用函数处理"""
            publish_time = response.xpath(
                "//div[@id='post_date']/text()").extract_first()
            sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(str(self.get_webname_time(publish_time.strip().split(u'：')[-1])))
            """作者有多个的，用/分割"""
            author = response.xpath("//div[@id='post_author']/a/text()").extract_first()
            if author == None:
                author = response.xpath(
                    "//div[@id='post_author']/text()").extract_first()
            sent_kafka_message['author'] = author if author else None
            info_source = response.xpath(
                "//div[@id='post_source']/a/text()").extract_first()
            if info_source == None:
                info_source = response.xpath(
                    "//div[@id='post_source']/text()").extract_first()
            sent_kafka_message['info_source'] = info_source if info_source else None
            tags_list = response.xpath("//div[@class='article_info_box tags']//a/text()").extract()
            sent_kafka_message['tags'] = tags_list if tags_list else None
            num = response.xpath("//div[@class='functionicon-item praise ']/p/text()").extract_first()
            sent_kafka_message['like_count'] = num if num else None
            desc = response.xpath("//div[@id='post_brief']/text()").extract_first()
            sent_kafka_message['desc'] = desc if desc else None
            img_src = response.meta['queue_value']['small_img_location']

            check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,self.redis_key,sent_kafka_message['publish_time'])

            if not check_flag:
                # sent_kafka_message['parse_function'] = 'parse_detail_page'
                # sent_kafka_message['body'] = None
                # return sent_kafka_message
                small_img_location = [
                    {'img_src': img_src, 'img_path': None, 'img_index': 1,'img_desc': None, 'img_width': None, 'img_height': None}]
                pass
            else:
                small_img_location = [
                    {'img_src': img_src, 'img_path': img_file_info['img_file_name'], 'img_index': 1,'img_desc': None, 'img_width': img_file_info['img_width'], 'img_height': img_file_info['img_height']}]
            if small_img_location:
                sent_kafka_message['small_img_location'] = small_img_location
                sent_kafka_message['small_img_location_count'] = 1

            # content = response.xpath("//div[@id='post_description']").extract_first()
            content_body = response.xpath("//div[@id='post_description']").extract_first()
            content_img = response.xpath("//div[@id='post_thumbnail']").extract_first()
            content = content_img + content_body
            article_content = self.extract_article_parsed_content(content, sent_kafka_message['publish_time'])
            download_img_flag = article_content[6]

            # if not download_img_flag:
            #     sent_kafka_message['parse_function'] = 'parse_detail_page'
            #     sent_kafka_message['body'] = None
            #     return sent_kafka_message

            parsed_content = article_content[0].decode('utf-8')
            sent_kafka_message['parsed_content'] = parsed_content.split(u'<p>推荐阅读:')[0]
            parsed_content_main_body = article_content[4]
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body.split(u'推荐阅读:')[0]
            sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
            sent_kafka_message['authorized'] = article_content[5]
            sent_kafka_message['img_location'] = article_content[2]
            sent_kafka_message['img_location_count'] = article_content[3]
            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())
