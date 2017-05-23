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
__date__ = "2017-4-11"
"""
功能：
    抓取贵州网相关文章信息
"""


class GuiZhou_parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']
            list_page_url = response.url
            # 暂定10页列表测试:
            if "shtml" in list_page_url:
                pass
            else:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    next_page_url = "http://travel.gzw.net/{}.shtml".format(page_index)

                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

            document_list = response.xpath("//li[not(@*)]")  # response.xpath("//a")
            for li_document in document_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                url = response.urljoin(li_document.xpath('a/@href').extract_first())
                title = li_document.xpath("a/text()").extract()[0]

                sent_kafka_message['url'] = url if url else None
                sent_kafka_message['title'] = title if title else None
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                # request_meta = {'queue_value': sent_kafka_message}
                yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            info_docu = response.xpath("//div[@class='from']/span/text()").extract()[0]

            news_content_document = response.xpath(
                "//div[@class='content']").extract_first()  # .encode('utf-8')
            sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
            info_source = info_docu.split()[2][3:]  # 消息来源
            publish_time = info_docu.split()[0][5:] + " " + info_docu.split()[1]
            tags = response.xpath("//meta[@name='keywords']/@content").extract()[0].split(",")

            parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                news_content_document, publish_time)

            authorized = response.xpath('/html/body/div[3]/div[1]/div[8]/div/text()').extract_first()
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['title'] = response.xpath("//h2/text()").extract_first()
            sent_kafka_message['body'] = response.body_as_unicode()
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count
            sent_kafka_message['parsed_content'] = parsed_content
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
            sent_kafka_message['authorized'] = authorized
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['info_source'] = info_source
            sent_kafka_message['tags'] = tags

            yield sent_kafka_message
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

    def get_webname_time(self, web_and_time_t):
        web_and_time = web_and_time_t.split()
        now = datetime.now()
        # 今天 10:00   或者 11-13 11:04 或者2015-11-13 11:00
        if len(web_and_time) == 2 and u'前' not in web_and_time[1]:
            if u"今天" in web_and_time[0]:
                date_str = "{}{}{} {}".format(
                    now.year, now.month, now.day, web_and_time[1])
                dt = datetime.strptime(
                    date_str, "%Y%m%d %H:%M").isoformat()
                return dt
            elif len(web_and_time[0].split("-")) == 4:
                return datetime.strptime(web_and_time_t[5:], "%Y-%m-%d %H:%M").isoformat()
            elif len(web_and_time[0].split("-")) == 3:
                return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M").isoformat()
            else:  # 11-13 11:04
                dt = datetime.strptime("{}-{}".format(now.year, web_and_time_t),
                                       "%Y-%m-%d %H:%M").isoformat()
                return dt
        elif u'前' in web_and_time_t:
            if u'天前' in web_and_time_t:
                time_back = web_and_time_t.split(u'天前')[0]
                dt = (now - timedelta(days=int(time_back))).isoformat()
                return dt
            elif u'小时' in web_and_time_t:
                time_back = web_and_time_t.split(u'小时')[0]
                dt = (now - timedelta(hours=int(time_back))).isoformat()
                return dt
            else:
                time_back = web_and_time_t.split(u'分钟')[0]
                dt = (now - timedelta(minutes=int(time_back))).isoformat()
                return dt
        else:
            return web_and_time_t
