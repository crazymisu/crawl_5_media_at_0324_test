# -*- coding: utf-8 -*-
import re
import urlparse
import scrapy
import bs4
from bs4 import BeautifulSoup
import sys
from scrapy.selector import Selector
import json
import codecs
reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
from datetime import datetime
import copy
import traceback
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE


class GPLPParser(Parser):

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']

        next_page_url_document = response.xpath("//span[@class='next']//a/@href").extract()
        url_page = next_page_url_document[0] if next_page_url_document else None

        if IS_CRAWL_NEXT_PAGE and url_page:
            sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
            sent_kafka_message['url'] = url_page
            sent_kafka_message['parse_function'] = 'parse_list_page'
            yield sent_kafka_message

        url_list = response.xpath("//main[@id='main']//article").extract()
        n = -1
        for each in url_list:
            small_img_location = []
            item = copy.deepcopy(original_sent_kafka_message)
            mess = Selector(text=each)
            # 缩略图
            n += 1
            time_= mess.xpath("//article//span[@class='date']").extract()
            time_ = time_[0] if time_ else None
            publish_time = self.parse_toutiao_publish_time(time_)
            small_img = {}
            img_src=mess.xpath("//img/@src").extract()
            img_src=img_src[0] if img_src else None
            small_img['img_src'] = img_src if img_src else None
            if small_img['img_src']:
                check_flag, img_file_info = save_img_file_to_server(
                    small_img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                if not check_flag:
                    small_img_location.append({'img_src': small_img['img_src'], 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                    item['small_img_location']=small_img_location
                    item['small_img_location_count'] = len(small_img_location)
                else:
                    small_img['img_path'] = img_file_info['img_file_name']
                    small_img['img_index'] = 1
                    small_img['img_desc'] = None
                    small_img['img_width'] = img_file_info['img_width']
                    small_img['img_height'] = img_file_info['img_height']
                    small_img_location.append(small_img)
                    item['small_img_location'] = small_img_location
                    item['small_img_location_count'] = len(small_img_location)
            else:
                item['small_img_location']=None
                item['small_img_location_count'] = 0

            each_url=mess.xpath("//h2[@class='entry-title']/a/@href").extract()
            each_url = each_url[0] if each_url else None
            item['url'] = each_url
            item['parse_function'] = 'parse_detail_page'
            yield item

            # yield scrapy.Request(each_url, callback=self.parse_detail_page, meta={"queue_value": item})
        # yield scrapy.Request(url_page, callback=self.parse_list_page)

    def parse_detail_page(self, response):
        title = response.xpath("//div[@class='left']//h1/text() | //h1[@class='entry-title']/text()").extract()
        if title:
            sent_kafka_message = response.meta['queue_value']
            try:
                small_img_location = sent_kafka_message['small_img_location']
                sent_kafka_message[
                    'small_img_location'] = small_img_location
            except Exception as e:
                print e
                print(traceback.format_exc())
            # crawl url domain
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            # 跳转后的真实url
            sent_kafka_message['response_url'] = response.url
            # HTTP status code
            sent_kafka_message['status_code'] = response.status
            # title
            title = response.xpath("//header[@class='entry-header']/h1/text()").extract()
            sent_kafka_message['title'] = title[0] if title else None
            # 文章授权说明
            authorized=response.xpath("//div[@class='authorbio']//li[1]/text()").extract()
            sent_kafka_message['authorized'] = authorized[0] if authorized else None
            # 网页源代码 不需要base64加密
            sent_kafka_message['body'] = response.body_as_unicode()
            # 发布时间
            try:
                publish_time = response.xpath("//div[@class='single-cat']/text()").extract()
                publish_time = publish_time[0].strip() if publish_time else None
                publish_time=publish_time.split("：")[1].split(" ")[0]
                sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
            except Exception as e:
                print e
                print(traceback.format_exc())
            # 作者
            sent_kafka_message['author'] = None
            # 文章的信息来源
            sent_kafka_message['info_source'] = None
            # 文章大图相关信息
            img_list = response.xpath("//div[@class='entry-content']//h1/a/img/@src").extract()
            img_location = []
            if img_list:
                for each in img_list:
                    img = {'img_path': None, 'img_desc': None, 'img_width': None, 'img_height': None}
                    img['img_src'] = each
                    check_flag, img_file_info = save_img_file_to_server(
                        img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                    img['img_index'] = img_list.index(each) + 1
                    if check_flag:
                        img['img_path'] = img_file_info['img_file_name']
                        img['img_desc'] = None
                        img['img_width'] = img_file_info['img_width']
                        img['img_height'] = img_file_info['img_height']

                    img_location.append(img)
                    sent_kafka_message['img_location_count'] = len(img_list)
                    if sent_kafka_message['img_location_count'] == 0:
                        sent_kafka_message['img_location'] = None
                    else:
                        # 文章大图个数
                        sent_kafka_message['img_location'] = img_location
            else:
                sent_kafka_message['img_location'] = None
                sent_kafka_message['img_location_count']= None
            # 按照规定格式解析出的文章纯文本
            content = response.xpath("//div[@class='single-content']//text()").extract()
            content = content if content else None
            # print(content)
            if content:
                sent_kafka_message['parsed_content_main_body'] = ''.join(
                    [i for i in content])
                # print(sent_kafka_message['parsed_content_main_body'])
                # 按照规定格式解析出的文章纯文本个数
                # sent_kafka_message['parsed_content_char_count'] = len(response.xpath(
                #     "//div[@class='clearfix contdiv']//p[@class='newtext']/text()").extract()[:-1])
                sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
                # print(sent_kafka_message['parsed_content_char_count'])
                # 文章关键词标签
                tags=response.xpath("//div[@class='single-tag']//text()").extract()
                try:
                    if u'，' in tags[1]:
                        sent_kafka_message['tags'] = tags[1].split("，") if tags[1] else None
                    else:
                        sent_kafka_message['tags'] = tags[1].split(" ") if tags[1] else None
                except Exception as e:
                    print e
                    print(traceback.format_exc())
                    sent_kafka_message['tags']= None
                # 点赞数
                num = response.xpath(
                    "//i[@class='count']/text()").extract()
                try:
                    sent_kafka_message['like_count'] = num[0].strip() if num else 0
                except Exception as e:
                    print e
                    print(traceback.format_exc())
                    sent_kafka_message['like_count'] = None
                # 回复数
                sent_kafka_message['comment_count'] = None
                # 按照规定格式解析出的文章正文 <p>一段落</p>
                try:
                    sent_kafka_message[
                        'parsed_content'] = self.get_parsed_content(response, publish_time)
                except Exception as e:
                    print e
                    print(traceback.format_exc())

                yield sent_kafka_message
            else:
                pass
        else:
            pass


    def get_parsed_content(self, response,publish_time):
        # 文章段落原生版
        publish_time=publish_time
        base_body = response.xpath("//div[@class='single-content']")[0].extract()
        parsed_content = self.filter_tags(base_body,publish_time,response) # a标签替换还没有写
        return parsed_content

    def filter_tags(self, htmlstr,publish_time,response):
        publish_time=publish_time
        parsed_content = ''
        if htmlstr:
            soup = BeautifulSoup(htmlstr,'lxml')
            # parsed_content_main_body = soup.body.div.text
            p_content_document_list = soup.body.div.children
            for p in p_content_document_list:
                if isinstance(p, bs4.Tag):
                    if p.findChild('img'):
                        try:
                            img_src = p.img['src']
                        except Exception as e:
                            print e
                        try:
                            check_flag, img_file_info = save_img_file_to_server(
                                img_src,
                                self.mongo_client,
                                self.redis_client,
                                self.redis_key,
                                save_date=publish_time)
                            if check_flag:
                                img_path = img_file_info['img_file_name']
                            else:
                                img_path = img_src
                            img_src = '<img src="%s"' % img_path
                        except Exception as e:
                            print e.message
                        p_img = '%s%s%s' % ('<p>', img_src, '</p>')
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content+=p_content
                        parsed_content+=p_img
                    else:
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content+=p_content
        content_text = parsed_content.replace('<p></p>', '').replace('<p> </p>','')
        return content_text
