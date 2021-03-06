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
import copy
reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
from datetime import datetime
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE


class ShenMeZhiDeMaiParser(Parser):

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']
        next_page_url_document = response.xpath("//li[@class='pagedown']/a/@href").extract()
        url_page = next_page_url_document[0] if next_page_url_document else None
        if IS_CRAWL_NEXT_PAGE and url_page:
            sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
            sent_kafka_message['url'] = url_page
            sent_kafka_message['parse_function'] = 'parse_list_page'
            yield sent_kafka_message

        url_list = response.xpath("//div[@class='list']").extract()

        for each in url_list:
            item = copy.deepcopy(original_sent_kafka_message)
            mess = Selector(text=each)
            # 缩略图
            small_img_src=response.xpath("//a[@class='picLeft']/img/@src").extract()
            item['small_img_location']=small_img_src[0] if small_img_src else None
            each_url=mess.xpath("//a[@class='picLeft']/@href").extract()
            each_url = each_url[0] if each_url else None
            # print(each_url)
            item['url'] = each_url
            # import pdb
            # pdb.set_trace()
            print(item['url'])
            item['parse_function'] = 'parse_detail_page'
            yield item
        # yield scrapy.Request(url_page, callback=self.parse_list_page)

    def parse_detail_page(self, response):
        title = response.xpath("//h1/text()").extract()
        # import pdb
        # pdb.set_trace()
        print(title)
        if title:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['small_img_location']=sent_kafka_message['small_img_location']
            # 发布时间
            publish_time = response.xpath("//span[@class='lrTime']/text()").extract()
            publish_time = publish_time[0].strip() if publish_time else None
            sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
            # sent_kafka_message['publish_time'] = publish_time
            # print(sent_kafka_message['publish_time'])
            if sent_kafka_message['small_img_location']:
                small_img = {}
                small_img_location=[]
                check_flag, img_file_info = save_img_file_to_server(
                    sent_kafka_message['small_img_location'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                if not check_flag:
                    small_img_location.append({'img_src': sent_kafka_message['small_img_location'], 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                else:
                    small_img['img_path'] = img_file_info['img_file_name']
                    small_img['img_index'] = 1
                    small_img['img_desc'] = None
                    small_img['img_width'] = img_file_info['img_width']
                    small_img['img_height'] = img_file_info['img_height']
                    small_img_location.append(small_img)
                sent_kafka_message['small_img_location'] = small_img_location
                sent_kafka_message['small_img_location_count'] = len(small_img_location)
            else:
                sent_kafka_message['small_img_location'] = None
                sent_kafka_message['small_img_location_count']

            print(sent_kafka_message['small_img_location'])
            # crawl url domain
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            print(sent_kafka_message['url_domain'])
            # 跳转后的真实url
            sent_kafka_message['response_url'] = response.url
            print(sent_kafka_message['response_url'])
            # HTTP status code
            sent_kafka_message['status_code'] = response.status
            print(sent_kafka_message['status_code'])
            # title
            sent_kafka_message['title'] = title[0] if title else None
            print(sent_kafka_message['title'])
            # 文章授权说明
            sent_kafka_message['authorized'] = None
            print(sent_kafka_message['authorized'])
            # 网页源代码 不需要base64加密
            sent_kafka_message['body'] = response.body_as_unicode()
            # print(type(sent_kafka_message['body']))
            # print(sent_kafka_message['body'])
            # 作者
            sent_kafka_message['author'] = None
            print(sent_kafka_message['author'])
            # 文章的信息来源
            sent_kafka_message['info_source'] = None
            print(sent_kafka_message['info_source'])
            # 文章大图相关信息
            img_list = response.xpath("//span[@class='img_desc']/img/@src").extract()
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

            # 按照规定格式解析出的文章正文 <p>一段落</p>
            # content_and_img=response.xpath("//div[@class='clearfix contdiv']//p").extract()[:-1]
            # content_str =''.join([i for i in content_and_img])
            # #去掉class="newtext"
            # a = content_str.replace(' class="newtext"', '')
            # #去掉a标签的样式
            # b = re.subn(u' class="ebkw" title="[\u4e00-\u9fa5]+"', "", a)[0]
            # #去掉img标签
            # c= re.subn(u' style="text-align:center"',"",b)[0].encode("utf-8")
            # c=b.replace(' style="text-align:center"','')

            # 按照规定格式解析出的文章纯文本
            content = response.xpath("//div[@class='news_content']//p//text()").extract()
            content = content if content else None
            # print(content)
            sent_kafka_message['parsed_content_main_body'] = ''.join(
                [i for i in content])
            # print(sent_kafka_message['parsed_content_main_body'])
            # 按照规定格式解析出的文章纯文本个数
            # sent_kafka_message['parsed_content_char_count'] = len(response.xpath(
            #     "//div[@class='clearfix contdiv']//p[@class='newtext']/text()").extract()[:-1])
            sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
            # print(sent_kafka_message['parsed_content_char_count'])
            # 文章关键词标签
            tags=response.xpath("//span[@class='lFloat']/a/text()").extract()
            sent_kafka_message['tags'] = tags[0] if tags else None
            # print(sent_kafka_message['tags'])
            # 点赞数
            num = response.xpath("//span[@class='grey']/em/text()").extract()
            try:
                sent_kafka_message['like_count'] = num[0].strip() if num else None
                # print("****" * 30)
                print(sent_kafka_message['like_count'])
            except Exception as e:
                print e
            # 回复数
            comment_count_list = response.xpath("//span[@class='greyNum']/em/text()").extract()
            sent_kafka_message['comment_count'] = comment_count_list[0] if comment_count_list else None
            print(sent_kafka_message['comment_count'])
            # 按照规定格式解析出的文章正文 <p>一段落</p>
            try:
                sent_kafka_message[
                    'parsed_content'] = self.get_parsed_content(response, publish_time)
                # response.xpath("//div[@class='clearfix contdiv']//p").extract()[:-1]
                print(sent_kafka_message['parsed_content'])
            except Exception as e:
                print e

            yield sent_kafka_message
        else:
            pass


    def get_parsed_content(self, response,publish_time):
        # 文章段落原生版
        publish_time=publish_time
        base_body = response.xpath("//div[@class='news_content']")[0].extract()
        parsed_content = self.filter_tags(base_body,publish_time,response) # a标签替换还没有写
        # print(parsed_content)
        return parsed_content

    def filter_tags(self, htmlstr,publish_time,response):
        publish_time=publish_time
        parsed_content = ''
        if htmlstr:
            htmlstr=htmlstr.replace(u'<dl class="list_catalogue">',"<p>").replace(u'<dd></dd>','').replace(u'</dl>','</p>').replace('\n','')
            soup = BeautifulSoup(htmlstr,'lxml')
            print soup
            # parsed_content_main_body = soup.body.div.text
            p_content_document_list = soup.body.div.children
            for p in p_content_document_list:
                if isinstance(p, bs4.Tag):
                    if p.findChild('img'):
                        try:
                            img_src = img_src = p.img['src']
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
                        # print(type(p_img))
                        # print(p_img)
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content+=p_content
                        parsed_content+=p_img
                    else:
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content+=p_content
                        # print(parsed_content)
        content_text = parsed_content.replace(u'<p></p>', '').replace(u'<p>     直达链接>  </p>','').replace(u'<p> </p>','')
        # content_text= content_text.split(' ')
        # content_text =''.join(content_text)
        # print(content_text)
        # print("1212"*30)
        return content_text
