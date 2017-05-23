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
__date__ = '2017/4/10'
"""
酷锋网抓取逻辑？
"""


class Kufeng_parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            if "kuaibao" in response.url:
                pass
            else:
                original_sent_kafka_message = response.meta['queue_value']
                list_page_url = response.url
                # 暂定10页列表测试:
                # if False:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    next_page_url = list_page_url.split('&')[0] + "&page={}".format(page_index)

                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    request_meta = {'queue_value': sent_kafka_message}
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

                document_list = response.xpath("//li")  # response.xpath("//a")
                for li_document in document_list:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    url = response.urljoin(li_document.xpath('div/a/@href').extract()[0])
                    title = li_document.xpath("h3/a/text()").extract()[0]
                    desc = li_document.xpath("p/text()").extract()[0]
                    publish_time = self.parse_toutiao_publish_time(self.get_webname_time(
                        li_document.xpath('span[@class="timeago"]/text()').extract()[0]))
                    author = li_document.xpath('span/text()').extract()[0][3:]

                    small_img_location = li_document.xpath('div/a/img/@src').extract()[0]

                    if small_img_location:
                        small_img_src = small_img_location

                        check_flag, img_file_info = save_img_file_to_server(small_img_src, self.mongo_client,
                                                                            self.redis_client, self.redis_key,
                                                                            publish_time if publish_time else self.now_date)
                        if not check_flag:
                            small_img_location = [{'img_src': small_img_src, 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None}]
                        else:
                            small_img_location = [{'img_src': small_img_src, 'img_path': img_file_info['img_file_name'], 'img_index': 1, 'img_desc': None, 'img_width': img_file_info['img_width'], 'img_height': img_file_info['img_height']}]
                        sent_kafka_message['small_img_location'] = small_img_location
                        # check_flag, img_file_name = \
                        #     save_img_file_to_server(small_img_src, self.mongo_client,
                        #     self.redis_client, self.redis_key,publish_time if publish_time else self.now_date)
                        # small_img_location = [
                        #     {'img_src': small_img_src, 'img_path': img_file_name, 'img_index': 1, 'img_desc': None}]
                        # sent_kafka_message['small_img_location'] = small_img_location

                    sent_kafka_message['url'] = url if url else None
                    sent_kafka_message['title'] = title if title else None
                    sent_kafka_message['author'] = author if author else None
                    sent_kafka_message['desc'] = desc if desc else None
                    sent_kafka_message['publish_time'] = publish_time if publish_time else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'
                    request_meta = {'queue_value': sent_kafka_message}
                    yield sent_kafka_message
        except Exception as e:
            # self.logger.error(e)
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            news_content_document = response.xpath('.//div[contains(@class, "article-content")]').extract_first()

            info_source = ""  # 消息来源
            # tag_list = news_info_document.xpath('.//div[contains(@class, "A_linebn")]/a')
            # tag = [tag_item.xpath('.//text()').extract_first() for tag_item in tag_list]
            publish_time = sent_kafka_message['publish_time']
            # parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized = self.extract_article_parsed_content(
            #     news_content_document, publish_time)
            parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(news_content_document, publish_time)

            # if not download_img_flag:
            #     sent_kafka_message['parse_function'] = 'parse_detail_page'
            #     sent_kafka_message['body'] = None
            #     yield sent_kafka_message

            sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body_as_unicode()
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['info_source'] = info_source.split(u'：')[-1].strip() if info_source else None
            sent_kafka_message['video_location'] = None
            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count
            # sent_kafka_message['small_img_location'] = None
            sent_kafka_message['small_img_location_count'] = 1
            sent_kafka_message['parsed_content'] = parsed_content
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
            authorized = response.xpath("//div[@class='avow']/p/text()").extract_first()
            sent_kafka_message['authorized'] = authorized

            yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    # def parse_article_content(self, content_html, pub_date):
    #     parsed_content = ''
    #     parsed_content_char_count = 0
    #     img_location = []
    #     img_location_count = 0
    #     img_index = 1
    #
    #     if content_html:
    #         soup = BeautifulSoup(content_html)
    #         parsed_content_main_body = soup.body.div.text.replace(u'\n', u'').replace(u'\r', u'').replace(u' ',
    #                                                                                                       u'').strip()
    #         parsed_content_char_count = len(parsed_content_main_body)
    #         p_content_document_list = soup.body.div.children
    #         for p in p_content_document_list:
    #             if isinstance(p, bs4.Tag):
    #                 if p.findChild('img'):
    #                     p_path = save_img_file_to_server(p.img['src'], mongo_client=self.mongo_client,
    #                                                      redis_client=self.redis_client, redis_key=self.redis_key,
    #                                                      save_date=pub_date)[1]
    #
    #                     img_location.append(
    #                         {'img_src': p.img['src'], 'img_path': p_path, 'img_desc': None, 'img_dex': img_index})
    #                     img_location_count += 1
    #                     img_index += 1
    #                     parsed_content += u'%s"%s"%s' % (u'<p><img src=', p_path, u'/></p>')
    #                     if p.text != u' ':
    #                         p_content = u'%s%s%s' % (u'<p>', p.text.replace(u' ', u'').strip(), u'</p>')
    #                         parsed_content += p_content
    #
    #                 else:
    #                     if p.text != u' ':
    #                         p_content = u'%s%s%s' % (u'<p>', p.text.replace(u' ', u'').strip(), u'</p>')
    #                         parsed_content += p_content
    #
    #         parsed_content = parsed_content.replace(u'\n', u'').replace(u'\r', u'').replace(u'<p></p>', u'')
    #     return parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count

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
