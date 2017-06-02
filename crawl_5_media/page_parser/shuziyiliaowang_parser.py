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


class ShuZiYiLiaoWangparser(Parser):

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']

        next_page_url_document = response.xpath("//li[@id='next']/a/@href").extract()
        url_page = next_page_url_document[0] if next_page_url_document else None

        if IS_CRAWL_NEXT_PAGE and url_page:
            sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
            sent_kafka_message['url'] = url_page
            sent_kafka_message['parse_function'] = 'parse_list_page'
            yield sent_kafka_message

        url_list = response.xpath("//li[@class='element']").extract()
        for each in url_list:
            item = copy.deepcopy(original_sent_kafka_message)
            mess = Selector(text=each)
            # 缩略图
            item['small_img_location'] = None
            each_url=mess.xpath("//div[@class='name center']/a/@href").extract()
            each_url = each_url[0] if each_url else None
            item['url'] = each_url
            item['parse_function'] = 'parse_detail_page'
            yield item

            # yield scrapy.Request(each_url, callback=self.parse_detail_page, meta={"queue_value": item})
        # yield scrapy.Request(url_page, callback=self.parse_list_page)

    def parse_detail_page(self, response):
        sent_kafka_message = response.meta['queue_value']
        # crawl url domain
        sent_kafka_message['url_domain'] = response.url.split('/')[2]
        # 跳转后的真实url
        sent_kafka_message['response_url'] = response.url
        # HTTP status code
        sent_kafka_message['status_code'] = response.status
        # title
        title = response.xpath("//div[@class='title']/text()").extract()
        sent_kafka_message['title'] = title[0].strip() if title else None
        # 文章授权说明
        sent_kafka_message['authorized'] = u"转载请注明出处：HC3i中国数字医疗网"
        # 网页源代码 不需要base64加密
        sent_kafka_message['body'] = response.body_as_unicode()
        # 发布时间
        publish_time = response.xpath("//ul[@class='data left']/li[1]/text()").extract()
        publish_time = publish_time[0].strip() if publish_time else None
        sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
        # 作者
        author = response.xpath("//ul[@class='data left']/li[2]/text()").extract()
        sent_kafka_message['author'] = author[0].split(":")[1] if author else None
        # 文章的信息来源
        info_source = response.xpath("//ul[@class='data left']/li[3]/text()").extract()
        sent_kafka_message['info_source'] = info_source[0].split("：")[1] if info_source else None
        # 文章大图相关信息
        img_list = response.xpath("//p[@style='text-align: center']/a/img/@src").extract()
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
            sent_kafka_message['img_location_count']=None
        # 按照规定格式解析出的文章纯文本
        content = response.xpath("//div[@class='answer']//p//text()").extract()
        content = content if content else None
        if content:
            sent_kafka_message['parsed_content_main_body'] = ''.join(
                [i for i in content])
            sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
            # 文章关键词标签
            sent_kafka_message['tags'] = None
            # 点赞数
            num = response.xpath("//span[@class='total']/text()").extract()
            try:
                sent_kafka_message['like_count'] = num[1].srtip() if num else 0
            except Exception as e:
                print e
                print(traceback.format_exc())
            # 回复数
            comment_count_list = response.xpath("//span[@class='total']/text()").extract()
            sent_kafka_message['comment_count'] = comment_count_list[0] if comment_count_list else None
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

    def get_parsed_content(self, response,publish_time):
        # 文章段落原生版
        publish_time=publish_time
        base_body = response.xpath("//div[@class='answer']")[0].extract()
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
                        if u'转载请注明出处' in p_content:
                            pass
                        elif u'责任编辑' in p_content:
                            pass
                        elif u'收藏本页' in p_content:
                            pass
                        else:
                            parsed_content += p_content
                            parsed_content += p_img
                    else:
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        if u'转载请注明出处' in p_content:
                            pass
                        elif u'责任编辑' in p_content:
                            pass
                        elif u'收藏本页' in p_content:
                            pass
                        else:
                            parsed_content+=p_content
        content_text = parsed_content.replace('<p></p>', '').replace('<p> </p>','')
        return content_text
