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
__date__ = '2017/4/18'
"""
万维家电文章抓取
"""


class Wanwei_parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            # a = response.request.url
            original_sent_kafka_message = response.meta['queue_value']
            list_page_url = response.url
            # 暂定10页列表测试:
            if len(list_page_url.split("_")) >= 3:
                pass
            else:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    next_page_url = list_page_url[:-5] + "_{}.html".format(page_index)

                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

            document_list = response.xpath("//ul[@class='list']/li")
            for div in document_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                url = response.urljoin(div.xpath('h3/a/@href').extract_first())
                title = div.xpath('h3/a/text()').extract_first()

                desc = div.xpath('div/p/text()').extract_first()
                publish_time = div.xpath('div/div/div[@class="tag"]/span[2]/text()').extract_first()
                publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))

                small_img_location = div.xpath('div/a/img/@src').extract_first()

                if small_img_location:
                    small_img_src = small_img_location

                    check_flag, img_file_info = save_img_file_to_server(small_img_src, self.mongo_client,
                                                                        self.redis_client, self.redis_key,
                                                                        publish_time if publish_time else self.now_date)
                    if not check_flag:
                        small_img_location = [
                            {'img_src': small_img_src,
                             'img_path': None,
                             'img_index': 1,
                             'img_desc': None,
                             'img_width': None,
                             'img_height': None}]
                    else:
                        small_img_location = [
                            {'img_src': small_img_src,
                             'img_path': img_file_info['img_file_name'],
                             'img_index': 1,
                             'img_desc': None,
                             'img_width': img_file_info['img_width'],
                             'img_height': img_file_info['img_height']}]
                    sent_kafka_message['small_img_location'] = small_img_location
                sent_kafka_message['url'] = url if url else None
                sent_kafka_message['title'] = title.strip() if title else None
                sent_kafka_message['desc'] = desc.strip() if desc else None
                sent_kafka_message['publish_time'] = publish_time if publish_time else None
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message
        except Exception as e:
            # self.logger.error(e)
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            news_content_document = response.xpath("//div[@class='at-text']").extract_first()  # .encode('utf-8')
            sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
            info_source = response.xpath("//*[@id='source_baidu']/text()").extract_first()
            author = response.xpath("//*[@id='author_baidu']/text()").extract_first()
            publish_time = sent_kafka_message['publish_time']
            tags = response.xpath('//*[@id="commentmode"]/div[1]/a/text()').extract()
            parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(
                news_content_document, publish_time)
            authorized = response.xpath('/html/body/div[5]/div[1]/div[4]/text()').extract_first()

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
            sent_kafka_message['tags'] = tags if tags else []
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['author'] = author.strip().split(u"：")[1] if author else None
            sent_kafka_message['info_source'] = info_source.split(u"：")[1] if info_source else None

            yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

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
        """
              时间统一格式
              :param self:
              :param web_and_time_t:可能出现的格式有：
                  今天 10:50
                  昨天 10:50
                  1分钟前
                  1小时前
                  11-13 11:44
                  2016-2015-01-31 22:00
                  2015-01-31 22:00
                  2015-11-13 11:00:30
                  2015年03月24日 
                  时间：2016-10-25 15:05:34
              :return:类似 2015-11-13 11:00:30的时间格式
              """
        try:
            web_and_time = web_and_time_t.split()
            now = datetime.now()
            if u"时间" in web_and_time_t:
                web_and_time_t = web_and_time_t.split(u'：')[-1]
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
                    try:
                        return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M").isoformat()
                    except:
                        return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M:%S").isoformat()
                elif u"月" in web_and_time[0]:
                    month, day = re.findall(r'\d+', web_and_time[0])
                    minute, second = re.findall(r'\d+', web_and_time[1])
                    dt = "{}-{}-{} {}:{}".format(now.year, month, day, minute, second)
                    return dt
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
        except Exception :
            return web_and_time_t
