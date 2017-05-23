# -*- coding: utf-8 -*-
from urlparse import urlparse, urlunparse
from datetime import datetime
import copy
import traceback
import re
from datetime import datetime, timedelta
import logging
import pymongo
import json
import time
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

"""
处理新浪中可以从network中得到的json请求。未翻页，每页可以调整url为50个
"""
class XinLangKeJiParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']
            if 'http://feed.mix.sina.com.cn' in response.url:
                info = json.loads(response.body_as_unicode())
                newslist = info['result']['data']
                for news in newslist:
                    """列表页信息"""
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    sent_kafka_message['url'] = news['url'] if 'url' in news else None
                    sent_kafka_message['title'] = news['title'] if 'title' in news else None
                    sent_kafka_message['info_source'] = news['media_name'] if 'media_name' in news else None
                    sent_kafka_message['author'] = news['author'] if 'author' in news and news['author'] else None
                    sent_kafka_message['desc'] = news['summary'] if 'summary' in news else None
                    sent_kafka_message['publish_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(
                        int(news['ctime']))) if 'ctime' in news else None
                    img_src = news['img']['u'] if 'img' in news and 'u' in news['img'] else None
                    img_desc = news['img']['t'] if 'img' in news and 't' in news['img'] else None
                    check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,self.redis_key, sent_kafka_message['publish_time'])
                    if not check_flag:
                        sent_kafka_message['small_img_location'] = [{'img_src': img_src, 'img_path': None, 'img_index': 1, 'img_desc': img_desc,'img_width': None, 'img_height': None}]
                    else:
                        sent_kafka_message['small_img_location'] = [{'img_src': img_src, 'img_path': img_file_info['img_file_name'], 'img_index': 1,'img_desc': img_desc, 'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height']}]
                    sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
                    sent_kafka_message['parse_function'] = 'parse_detail_page'
                    yield sent_kafka_message
            elif 'http://tech.sina.com.cn' in response.url:
                info = response.xpath('//*[@class="list01"]/li').extract()
                for news in info:
                    news = Selector(text=news)
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    url = news.xpath('//h3/a/@href').extract_first()
                    sent_kafka_message['url'] = url if url else None
                    title = news.xpath('//h3/a/text()').extract_first()
                    sent_kafka_message['title'] = title if title else None
                    desc = news.xpath('//*[@class="list_info"]/text()').extract_first()
                    sent_kafka_message['desc'] = desc if desc else None
                    publish_time = news.xpath('//h5/text()').extract_first()
                    sent_kafka_message['publish_time'] = publish_time if publish_time else None
                    commentcount = news.xpath('//*[@class="list_meta"]/a/text()').extract_first()
                    sent_kafka_message['comment_count'] = commentcount.split('(')[1].split(')')[0] if commentcount else None
                    img_src = news.xpath('//*[@class="clearfix"]/a/img/@src').extract_first()
                    check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,self.redis_key,sent_kafka_message['publish_time'])
                    if not check_flag:
                        sent_kafka_message['small_img_location'] = [
                            {'img_src': img_src, 'img_path': None, 'img_index': 1, 'img_desc': None,
                             'img_width': None, 'img_height': None}]
                    else:
                        sent_kafka_message['small_img_location'] = [
                            {'img_src': img_src, 'img_path': img_file_info['img_file_name'], 'img_index': 1,'img_desc': None, 'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height']}]
                    sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
                    sent_kafka_message['parse_function'] = 'parse_detail_page'
                    yield sent_kafka_message
            elif 'http://roll.tech.sina.com.cn/elec/index.shtml' in response.url:
                urls = response.xpath('//*[@class="contList"]/ul/li/a/@href').extract()
                titles = response.xpath('//*[@class="contList"]/ul/li/a/text()').extract()
                for i in range(len(urls)):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    sent_kafka_message['url'] = urls[i]
                    sent_kafka_message['title'] = titles[i]
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
            sent_kafka_message['body'] = response.body_as_unicode()
            if not sent_kafka_message['info_source']:
                media = response.xpath('//*[@class="ent1 fred"]/text()').extract_first()
                if not media:
                    media = response.xpath('//*[@class="source"]/text()').extract_first()
                sent_kafka_message['info_source'] = media if media else  None
            if not sent_kafka_message['publish_time']:
                publish_time = response.xpath('//*[@class="titer"]/text()').extract_first()
                if not publish_time:
                    publish_time = response.xpath('//*[@id="pub_date"]/text()').extract_first()
                sent_kafka_message['publish_time'] = publish_time if publish_time else None
            if not sent_kafka_message['comment_count']:
                comment_count = response.xpath('//*[@id="commentCount1"]/text()').extract_first()
                sent_kafka_message['comment_count'] = comment_count if comment_count else 0
            tags = response.xpath('//*[@class="art_keywords"]/a/text()').extract()
            sent_kafka_message['tags'] = tags if tags else None
            content = response.xpath('//div[@id="artibody"]').extract_first()
            article_content = self.extract_article_parsed_content(content, sent_kafka_message['publish_time'])
            sent_kafka_message['img_location'] = article_content[2]
            sent_kafka_message['img_location_count'] = article_content[3]
            sent_kafka_message['parsed_content_main_body'] = article_content[4]
            sent_kafka_message['parsed_content'] = article_content[0]
            sent_kafka_message['parsed_content_char_count'] = article_content[1]
            sent_kafka_message['authorized'] = ''
            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())
