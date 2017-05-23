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
from openpyxl import load_workbook
import os

__author__ = 'kangkang'
__date__ = '2017/5/8'
"""
58汽车的文章（通过excel模板）
"""


def read_execel_element_config(name):
    excel_file_path = os.getcwd() + os.sep + 'excels' + os.sep
    excel_file_name = name  # 'kwsw.xlsx'
    wb = load_workbook(excel_file_path + excel_file_name)
    ws = wb.get_sheet_by_name('article')  # 列表页：list   ,  article:文章页
    article_xpath_result = {}
    list_xpath_result = {}
    for row in ws.iter_rows('A2:C18'):
        if row[2].value:
            article_xpath_result[row[0].value] = row[2].value
    ws = wb.get_sheet_by_name('list')
    for row in ws.iter_rows('A2:C18'):
        if row[2].value:
            list_xpath_result[row[0].value] = row[2].value
    list_xpath_result['excel'] = excel_file_name
    return list_xpath_result, article_xpath_result


class Qiche58_Parser(Parser):
    list_xpath_dic, article_xpath_dic = read_execel_element_config('58qiche.xlsx')

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        list_area = len(self.list_xpath_dic['article_list'])
        original_sent_kafka_message = response.meta['queue_value']
        try:
            # 下一页处理
            if self.list_xpath_dic.get('next_page'):
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                pagelist = response.xpath(self.list_xpath_dic['next_page'])
                next_pages = pagelist.xpath(
                    './/following-sibling::*/descendant-or-self::a[@href and not(re:match(@href,"javascript"))]/@href').extract()
                if not next_pages:
                    next_pages = pagelist.xpath(
                        '../following-sibling::*/descendant-or-self::a[@href and not(re:match(@href,"javascript"))]/@href').extract()
                next_pages = next_pages + pagelist.xpath('./@href').extract()
                for next_page_url in next_pages[1:]:
                    if next_page_url and '10' not in next_page_url:
                        sent_kafka_message['url'] = response.urljoin(next_page_url)
                        sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        yield sent_kafka_message

            list_list = self.list_xpath_dic['article_list'].split('/')
            num = [i for i, x in enumerate(list_list) if '@class' in x]
            if num:
                list_xpath = "//" + "/".join(list_list[num[0]:])
                document_list = response.xpath(list_xpath) + \
                                response.xpath(list_xpath + "//following-sibling::*")
            else:
                document_list = response.xpath(self.list_xpath_dic['article_list']) + \
                                response.xpath(self.list_xpath_dic['article_list'] + "/following-sibling::*")

            # 列表页处理
            for li_document in document_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                for key, xpath_str in self.list_xpath_dic.iteritems():
                    if key in ('next_page', 'id', 'data_source_name', 'data_source_id', 'excel'):
                        sent_kafka_message[key] = xpath_str
                    elif key == 'publish_time':
                        publish_time = li_document.xpath(xpath_str[list_area + 1:])
                        sent_kafka_message['publish_time'] = self.extract_time_text(publish_time)
                    elif key == 'url':
                        if self.list_xpath_dic[key][list_area + 1:]:
                            sent_kafka_message[key] = response.urljoin(li_document.xpath(
                                self.list_xpath_dic[key][list_area + 1:] + '/@href').extract_first())
                        else:
                            sent_kafka_message[key] = response.urljoin(li_document.xpath(
                                self.list_xpath_dic[key][list_area + 1:] + './@href').extract_first())
                    elif key == 'tags':
                        sent_kafka_message[key] = []
                        value = li_document.xpath(xpath_str[list_area + 1:] + '/descendant::text()').extract()
                        for item in value:
                            if len(item.strip()) > 1:
                                sent_kafka_message[key].append(item.strip().replace(' ', ''))
                    elif key == 'small_img_location':
                        if li_document.xpath(xpath_str[list_area + 1:]).extract_first():
                            small_img_location = li_document.xpath('./descendant::img/@src').extract_first()
                            small_img_src = small_img_location

                            check_flag, img_file_info = save_img_file_to_server(small_img_src, self.mongo_client,
                                                                                self.redis_client, self.redis_key,
                                                                                publish_time if publish_time else self.now_date)
                            if not check_flag:
                                small_img_location = [
                                    {'img_src': small_img_src, 'img_path': None, 'img_index': 1, 'img_desc': None,
                                     'img_width': None, 'img_height': None}]
                            else:
                                small_img_location = [
                                    {'img_src': small_img_src, 'img_path': img_file_info['img_file_name'],
                                     'img_index': 1,
                                     'img_desc': None, 'img_width': img_file_info['img_width'],
                                     'img_height': img_file_info['img_height']}]
                            sent_kafka_message['small_img_location'] = small_img_location
                    elif 'count' in key:
                        sent_kafka_message[key] = self.extract_number_count(
                            li_document.xpath(xpath_str[list_area + 1:] + '/text()').extract_first())
                    elif key == 'author':
                        sent_kafka_message[key] = self.extract_author_text(li_document.xpath(
                            self.list_xpath_dic[key][list_area + 1:] + '/text()').extract_first())
                    elif key == 'info_source':
                        sent_kafka_message[key] = self.extract_info_source_text(li_document.xpath(
                            self.list_xpath_dic[key][list_area + 1:] + '/text()').extract_first())
                    else:
                        sent_kafka_message[key] = li_document.xpath(
                            self.list_xpath_dic[key][list_area + 1:] + '/text()').extract_first()
                        if sent_kafka_message[key]:
                            sent_kafka_message[key].replace(" ", "")
                yield sent_kafka_message

        except Exception as e:
            # self.logger.error(e)
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            body = response.text
            body = body.replace("&nbsp;", "")  # 有一些带有扩展字符集字符（\xa0而不是正常的\x20)需要替换一下
            response = response.replace(body=body)

            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message = copy.deepcopy(sent_kafka_message)
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body_as_unicode()
            for key, xpath_str in self.article_xpath_dic.iteritems():
                if key == 'parsed_content':
                    content_document = response.xpath(xpath_str).extract_first()
                    parsed_result = self.extract_article_parsed_content(content_document)
                    parsed_content = parsed_result[0]
                    parsed_content_char_count = parsed_result[1]
                    img_location = parsed_result[2]
                    img_location_count = parsed_result[3]
                    parsed_content_main_body = parsed_result[4]
                    authorized = parsed_result[5]

                    sent_kafka_message['img_location'] = img_location
                    sent_kafka_message['img_location_count'] = img_location_count
                    sent_kafka_message['parsed_content'] = parsed_content
                    sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                    sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                    sent_kafka_message['authorized'] = authorized
                elif key in ('id', 'data_source_id'):
                    sent_kafka_message[key] = xpath_str
                elif key == 'publish_time':
                    publish_time = response.xpath(xpath_str + '/text()')
                    sent_kafka_message['publish_time'] = sent_kafka_message[key] if sent_kafka_message.get(
                        key) else self.extract_time_text(publish_time)
                elif key == 'tags':
                    # tags = response.xpath(xpath_str + '/text()').extract()
                    # sent_kafka_message['tags'] = tags if tags else None
                    sent_kafka_message[key] = []
                    value = response.xpath(xpath_str + '/descendant::text()').extract()
                    for item in value:
                        if len(item.strip()) > 1 and u'标签' not in item:
                            sent_kafka_message[key].append(item.strip().replace(' ', ''))
                elif 'count' in key:
                    sent_kafka_message[key] = self.extract_number_count(
                        response.xpath(xpath_str + '/text()').extract_first())
                elif key == 'info_source':
                    info_source = self.extract_info_source_text(response.xpath(xpath_str + '/text()').extract_first())
                    sent_kafka_message[key] = sent_kafka_message[key] if sent_kafka_message.get(key) else info_source
                elif key == 'author':
                    try:
                        author = self.extract_author_text(response.xpath(xpath_str + '/text()').extract_first())
                        sent_kafka_message[key] = sent_kafka_message[key] if sent_kafka_message.get(key)  else author
                    except:
                        pass
                elif 'count' in key:
                    sent_kafka_message[key] = self.extract_number_count(
                        response.xpath(xpath_str + '/text()').extract_first())
                elif key =='url':
                    pass
                else:
                    sent_kafka_message[key] = sent_kafka_message[key] if sent_kafka_message.get(
                        key)  else response.xpath(
                        xpath_str + '/text()').extract_first()
                    if sent_kafka_message[key]:
                        sent_kafka_message[key].replace(" ", "")
            if sent_kafka_message['title'] and sent_kafka_message['url']:
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
                # print(traceback.format_exc())
                pass
        return publish_time

    def extract_number_count(self, content_html):
        if content_html:
            count_list = re.findall(r'\d+', content_html)
            if count_list:
                return int(count_list[0])
        return None

    def extract_info_source_text(self, info_source):
        # 开源可能中间含有空格
        if info_source:
            try:
                info_source = re.findall(ur'来源：[\s]*([\u4e00-\u9fa50-9a-z]+)[\s]*', info_source.replace(' ', ''))[0]
            except:
                info_source = info_source
            return info_source
        return None

    def extract_author_text(self, author):
        if author:
            try:
                author = re.findall(ur'(?:作者：|发布人：|作者:|编辑：)[\s]*([\u4e00-\u9fa50-9a-z]+)[\s]*', author)[0]
            except:
                author = author
            return author
        return None

    def extract_time_text(self, publish_time):
        publish_time = publish_time.xpath('string(.)').extract_first()
        if publish_time:
            try:
                publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
            except:
                publish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return publish_time
        return None
