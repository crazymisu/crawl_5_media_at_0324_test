# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider
from scrapy.selector import Selector
from scrapy import Request
from urlparse import urlparse, urlunparse
import bs4
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import copy
import traceback
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

__author__ = "kangkang"
__date__ = "2017-4-17"
"""
功能：
    抓取艾肯网相关新闻
"""


class Aiken_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']
            list_page_url = response.url
            # 暂定10页列表测试:
            if "page" in list_page_url:
                pass
            else:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    if "?" in list_page_url:
                        next_page_url = list_page_url + "&page={}".format(page_index)
                    else:
                        next_page_url = list_page_url + "?page={}".format(page_index)

                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

            if "index" in response.url:
                document_list = response.xpath('//*[@class="art_list"]/li')
            else:
                document_list = response.xpath('//*[@id="artlist"]/div/ul/li')

            for li_document in document_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                try:
                    url = li_document.xpath('h5/a/@href').extract()[0]
                    title = li_document.xpath("h5/a/@title").extract_first()
                    publish_time = self.parse_toutiao_publish_time(self.get_webname_time(
                        li_document.xpath('h5/span/text()').extract()[0]))
                    info_source = None
                    desc = None
                except:
                    try:
                        url = li_document.xpath('div/h5/a/@href').extract()[0]
                        title = li_document.xpath("div/h5/a/@title").extract_first()
                        info_source, publish_time = li_document.xpath(
                            "div/div/p[@class='time']/text()").extract_first().split()
                        publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
                        desc = li_document.xpath("div/div/p[@class='info']/text()").extract_first()
                        small_img_location = li_document.xpath('div/div/img/@src').extract_first()
                    except:
                        url = li_document.xpath('div/h5/a/@href').extract_first()
                        title = li_document.xpath("div/h5/a/@title").extract_first()
                        info_time = li_document.xpath(
                            "div/div/div/span[@class='time']/text()").extract_first()
                        info_source, publish_time = info_time.split() if info_time else None, None
                        if publish_time:
                            publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
                        desc = li_document.xpath("div/div/p/text()").extract_first()
                        small_img_location = li_document.xpath('div/div/img/@src').extract_first()

                    if small_img_location:
                        small_img_src = response.urljoin(small_img_location)

                        check_flag, img_file_info = save_img_file_to_server(small_img_src, self.mongo_client,
                                                                            self.redis_client, self.redis_key,
                                                                            publish_time if publish_time else self.now_date)
                        if not check_flag:
                            small_img_location = [{'img_src': small_img_src,
                                                   'img_path': None,
                                                   'img_index': 1,
                                                   'img_desc': None,
                                                   'img_width': None,
                                                   'img_height': None}]
                        else:
                            small_img_location = [{'img_src': small_img_src,
                                                   'img_path': img_file_info['img_file_name'],
                                                   'img_index': 1,
                                                   'img_desc': None,
                                                   'img_width': img_file_info['img_width'],
                                                   'img_height': img_file_info['img_height']}]
                        sent_kafka_message['small_img_location'] = small_img_location

                sent_kafka_message['url'] = url if url else None
                sent_kafka_message['title'] = title if title else None
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                sent_kafka_message['info_source'] = info_source if info_source else None
                sent_kafka_message['desc'] = desc if desc else None
                sent_kafka_message['publish_time'] = publish_time if publish_time else None
                yield sent_kafka_message
        except Exception as e:
            pass
            # self.error_logger.error(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            # from scrapy.shell import inspect_response
            # inspect_response(response, self)

            try:
                news_content_document = response.xpath("//section[@class='textblock']").extract_first()
                if news_content_document:
                    sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
                    info_source, author = response.xpath("//span[@class='time am-fl'][2]").xpath(
                        'string(.)').extract_first().split('/')
                    publish_time = sent_kafka_message['publish_time']
                    tags = response.xpath("//meta[@name='keywords']/@content").extract_first()
                    if tags:
                        tags = tags.split(",")
                    parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                        news_content_document, publish_time)
                    sent_kafka_message['response_url'] = response.url
                    sent_kafka_message['status_code'] = response.status
                    sent_kafka_message['body'] = response.body_as_unicode()
                    sent_kafka_message['publish_time'] = publish_time
                    sent_kafka_message['img_location'] = img_location
                    sent_kafka_message['img_location_count'] = img_location_count
                    sent_kafka_message['video_location'] = None
                    sent_kafka_message['parsed_content'] = parsed_content
                    sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                    sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                    sent_kafka_message['authorized'] = authorized
                    sent_kafka_message['publish_time'] = publish_time
                    sent_kafka_message['info_source'] = info_source
                    sent_kafka_message['author'] = author
                    sent_kafka_message['tags'] = tags
                    yield sent_kafka_message
            except:
                news_content_document = response.xpath(
                    "//div[@id='content']").extract_first()  # .encode('utf-8')
                if news_content_document:
                    sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
                    info_source = response.xpath("//div[@class='bz1']/span[2]/text()").extract_first()
                    publish_time = sent_kafka_message['publish_time']
                    tags = response.xpath("//meta[@name='Keywords']/@content").extract_first()
                    if tags:
                        tags = tags.split(",")
                    parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                        news_content_document, publish_time)
                    sent_kafka_message['img_location'] = sent_kafka_message.get('img_location', [])
                    sent_kafka_message['parsed_content'] = sent_kafka_message.get('parsed_content', '')
                    sent_kafka_message['parsed_content_main_body'] = sent_kafka_message.get('parsed_content_main_body', u'')
                    sent_kafka_message['parsed_content_char_count'] = sent_kafka_message.get('parsed_content_char_count', 0)
                    sent_kafka_message['img_location_count'] = sent_kafka_message.get('img_location_count', 0)
                    sent_kafka_message['response_url'] = response.url
                    sent_kafka_message['status_code'] = response.status
                    sent_kafka_message['body'] = response.body_as_unicode()
                    sent_kafka_message['publish_time'] = publish_time
                    sent_kafka_message['img_location'].extend(img_location)
                    sent_kafka_message['img_location_count'] += img_location_count
                    sent_kafka_message['video_location'] = None
                    sent_kafka_message['parsed_content'] += parsed_content
                    sent_kafka_message['parsed_content_main_body'] += parsed_content_main_body
                    sent_kafka_message['parsed_content_char_count'] += parsed_content_char_count
                    sent_kafka_message['authorized'] = authorized
                    sent_kafka_message['publish_time'] = publish_time
                    sent_kafka_message['info_source'] = info_source
                    sent_kafka_message['tags'] = tags
                    next_page = response.xpath('//ul[@class="pageNav"]/li/a/@href').extract()  # [2:-1]
                    if next_page:
                        now = int(response.xpath('//ul[@class="pageNav"]/li/a[@class="now"]/text()').extract_first())
                        page_length = int(len(response.xpath('//ul[@class="pageNav"]/li/a/@href').extract()) - 1)
                        if now == page_length:
                            yield sent_kafka_message
                        else:
                            yield sent_kafka_message
                    else:
                        yield sent_kafka_message
                else:
                    pass
                    # self.logger.warning("img types,not news.")
        except Exception as e:
            pass

    def parse_toutiao_publish_time(self, article_publish_time):
        publish_time = None
        if article_publish_time:
            time_value_list = re.findall(r'\d+', article_publish_time)
            time_value_length = len(time_value_list)
            try:
                if time_value_length >= 3:
                    for i in range(6 - time_value_length):
                        time_value_list.append(0)
                    publish_time = datetime(int(time_value_list[0]), int(time_value_list[1]), int(time_value_list[2]),
                                            int(time_value_list[3]), int(time_value_list[4]), int(time_value_list[5]))
                    publish_time = publish_time.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                print e
        return publish_time

