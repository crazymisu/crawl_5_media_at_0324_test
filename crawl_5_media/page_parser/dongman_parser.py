# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider
from scrapy.selector import Selector
from scrapy import Request
from urlparse import urlparse, urlunparse
import traceback
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

__author__ = 'kangkang'
__date__ = '2017/4/20'
"""
抓取动漫之家的文章只需要宅新闻
"""


class Dongman_parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            # a = response.request.url
            original_sent_kafka_message = response.meta['queue_value']
            list_page_url = response.url
            # 暂定10页列表测试:
            if "html" in list_page_url:
                pass
            else:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    next_page_url = list_page_url + "p{}.html".format(page_index)

                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

            document_list = response.xpath("//div[@class='briefnews_con']/div")
            for div in document_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                url = div.xpath("div[@class='li_img_de']/h3/a/@href").extract_first()
                title = div.xpath("div[@class='li_img_de']/h3/a/text()").extract_first()
                desc = div.xpath("div[@class='li_img_de']/p[@class='com_about']/text()").extract_first()
                try:
                    publish_time, info_source, author = div.xpath("div[@class='li_img_de']/p/span/text()").extract()
                    publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
                    tags = div.xpath("div[@class='li_img_de']/div[@class='u_comfoot']/a/span/text()").extract()

                    small_img_location = div.xpath("div[@class='li_content_img']/a/img/@src").extract_first()

                    if small_img_location:
                        small_img_src = small_img_location

                        temp_headers = {
                            'Referer': response.url,
                            'Connection': 'keep-alive',
                            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36'
                        }

                        small_img_location, check_flag = self.get_small_img_location(small_img_src, None,
                                                                                     headers=temp_headers)
                        if check_flag:
                            sent_kafka_message['small_img_location'] = small_img_location
                            sent_kafka_message['small_img_location_count'] = len(small_img_location)
                    sent_kafka_message['url'] = url if url else None
                    sent_kafka_message['title'] = title.strip() if title else None
                    sent_kafka_message['desc'] = desc.strip() if desc else None
                    sent_kafka_message['publish_time'] = publish_time if publish_time else None
                    sent_kafka_message['info_source'] = info_source[3:] if info_source else None
                    sent_kafka_message['author'] = author[3:] if author else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'
                    sent_kafka_message['tags'] = tags if tags else None
                    yield sent_kafka_message
                except Exception as e:
                    pass

        except Exception as e:
            # self.logger.error(e)
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            news_content_document = response.xpath(
                "//div[@class='news_content_con']").extract_first()  # .encode('utf-8')
            sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
            publish_time = sent_kafka_message['publish_time']
            parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                news_content_document, publish_time,response.url)
            comment_count = response.xpath('//*[@id="Comment"]/div/h2/span[1]/em/text()').extract_first()
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body_as_unicode()
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count
            sent_kafka_message['parsed_content'] = parsed_content
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
            sent_kafka_message['authorized'] = authorized if authorized else None
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['comment_count'] = int(comment_count) if comment_count else 0

            yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())
