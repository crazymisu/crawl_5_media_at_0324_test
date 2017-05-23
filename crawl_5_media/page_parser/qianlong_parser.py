# -*- coding: utf-8 -*-
import scrapy
import re
from urlparse import urlparse, urlunparse
from datetime import datetime
import copy
import traceback

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class QianlongParser(Parser):    
    def __init__(self):
        super(Parser, self).__init__()
        self.init()


    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']        

            """ 首页的url是不带shtml，第二页以后带,一般有20页，少量的不够，也就4页,"""
            """ 有20页的就只爬10页，少量的不够，也就4页,就爬原来的页数"""

            if IS_CRAWL_NEXT_PAGE and 'shtml' not in response.url:
                page_all = response.xpath('//*[@class="pagination pagination-centered"]/ul/li/a/text()').extract()
                page = page_all[-2] if page_all else None
                if page != None and '...' in page:
                    page = int(page.split('...')[-1].strip(''))
                else:
                    page = 0
                if page > 10:
                    for num in range(2, 11):
                        link = response.url + str(num) + '.shtml'
                        parse_function = 'parse_list_page'
                        sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                        sent_kafka_message['url'] = link
                        sent_kafka_message['parse_function'] = parse_function if parse_function else None
                        yield sent_kafka_message
                else:
                    for num in range(2, page):
                        link = response.url + str(num) + '.shtml'
                        parse_function = 'parse_list_page'
                        sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                        sent_kafka_message['url'] = link
                        sent_kafka_message['parse_function'] = parse_function if parse_function else None

            """列表页抓取详细页的url"""
            urls = response.xpath('//ul[@class="list6"]/li/a/@href').extract()
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] = url
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())


    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['response_url'] = response.url
            if 'http://qlapp.qianlong.com/?app=article&controller=article&action=fullt' in response.url:
                info = eval(response.body[1:-2])
                body_info = info['content'].replace('\/', '/').decode('unicode-escape')
                parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
body_info, sent_kafka_message['publish_time'])

                # if not download_img_flag:
                #     sent_kafka_message['parse_function'] = 'parse_detail_page'
                #     sent_kafka_message['body'] = None
                #     return sent_kafka_message
                sent_kafka_message['body'] = response.body
                sent_kafka_message['img_location'] = img_location
                sent_kafka_message['img_location_count'] = img_location_count
                sent_kafka_message['parsed_content'] = parsed_content
                sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                sent_kafka_message['authorized'] = authorized

                return sent_kafka_message
            else:
                old_publish_time = response.xpath('//*[@class="pubDate"]/text()').extract_first()
                if old_publish_time != None:
                    publish_time = old_publish_time + ':00'
                else:
                    publish_time = None
                sent_kafka_message['url_domain'] = urlparse(response.url).netloc
                sent_kafka_message['status_code'] = response.status
                title = response.xpath('//*[@class="span12"]/h1/text()').extract_first()
                sent_kafka_message['title'] = title if title else None
                sent_kafka_message['body'] = response.body
                author = response.xpath('//*[@class="editor"]/span/text()').extract_first()
                sent_kafka_message['author'] = author.split(u'（')[0] if author else None
                info_source = response.xpath('//*[@class="source"]/a/@title').extract_first()
                sent_kafka_message['info_source'] = info_source if info_source else None
                sent_kafka_message['publish_time'] = publish_time
                if response.xpath('//*[@class="page"]').extract() != []:
                    url = 'http://qlapp.qianlong.com/?app=article&controller=article&action=fulltext&contentid=' + response.url.split('/')[-1].split('.')[0]
                    sent_kafka_message['url'] = url
                    parse_function = 'parse_detail_page'
                    sent_kafka_message['parse_function'] = parse_function if parse_function else None
                else:
                    news_content_document = response.xpath('//*[@class="article-content"]').extract_first()
                    parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                    news_content_document, publish_time)

                    if not download_img_flag:
                        sent_kafka_message['parse_function'] = 'parse_detail_page'
                        sent_kafka_message['body'] = None
                        return sent_kafka_message

                    sent_kafka_message['img_location'] = img_location
                    sent_kafka_message['img_location_count'] = img_location_count
                    sent_kafka_message['parsed_content'] = parsed_content
                    sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                    sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                    sent_kafka_message['authorized'] = authorized

                    return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

        