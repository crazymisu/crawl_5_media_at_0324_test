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


class ZhongJinZaiXianParser(Parser):

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']

        url_list = response.xpath("//ul[@class='TList']/li | //ul[@id='articlelist']/li | //div[@class='Fl W630']/ul/li | //ul[@id='articl']/li").extract()
        for each in url_list:
            item = copy.deepcopy(original_sent_kafka_message)
            mess = Selector(text=each)
            img_src = mess.xpath("//a/img/@src | //p[@class='des']/a/img/@src").extract()
            if u"cnfol.hk/news" in response.url:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//div[@class='textconR textconAll']/h3/a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            elif u"tags/%B9%C9%CC%ED%C0%D6%C6%C0" in response.url:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            elif u"forex.cnfol" in response.url:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//h3/a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            elif u"gold.cnfol" in response.url:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//h3/a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            elif u"auto.cnfol" in response.url:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            else:
                img_src = img_src[0] if img_src else None
                each_url = mess.xpath("//a/@href").extract()[0]
                item['small_img_location']= img_src
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
            yield item

            # yield scrapy.Request(each_url, callback=self.parse_detail_page, meta={"queue_value": item})
        # yield scrapy.Request(url_page, callback=self.parse_list_page)

    def parse_detail_page(self, response):
        page_num=response.xpath("//a[@id='pagenav_next']").extract()

        if not page_num:
            sent_kafka_message = response.meta['queue_value']
            # crawl url domain
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            # 跳转后的真实url
            sent_kafka_message['response_url'] = response.url
            # HTTP status code
            sent_kafka_message['status_code'] = response.status
            # 发布时间
            publish_time = response.xpath("//span[@id='pubtime_baidu']/text() | //div[@class='GSTitsL Cf']/span/text() | //p[@class='Fl']/span/text() | //div[@class='artDes']/span/text()").extract()
            publish_time = publish_time[0].strip() if publish_time else None
            sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
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
                sent_kafka_message['small_img_location_count']=None
            # title
            title = response.xpath("//h1[@id='Title']/text() | //h2[@id='Title']/text() | //h3[@class='artTitle']/text()").extract()
            sent_kafka_message['title'] = title[0] if title else None
            # 文章授权说明
            try:
                authorized=response.xpath("//em/text() | //div[@class='Stmt']/text()").extract()
                sent_kafka_message['authorized'] = authorized[0].split("：")[1] if authorized else None
            except Exception as e:
                sent_kafka_message['authorized'] = None
            # 网页源代码 不需要base64加密
            sent_kafka_message['body'] = response.body_as_unicode()
            # 作者
            author = response.xpath("//span[@id='author_baidu']/text() | //P[@class='Fl']//span/text() | //div[@class='GSTitsL Cf']/span/text() | //div[@class='artDes']//span/text() | //div[@class='article-information']//span/text()").extract()
            print(author)
            try:
                if u"auto.cnfol" in response.url:
                    sent_kafka_message['author'] = author[3].strip().split("：")[1] if author else None
                else:
                    sent_kafka_message['author'] = author[-1].strip().split("：")[1] if author else None
            except Exception as e :
                sent_kafka_message['author'] = None
            # 文章的信息来源
            info_source = response.xpath("//P[@class='Fl']//span[@class='Mr10']/text() | //span[@class='Mr10']/text() | //div[@class='artDes']//span[@class='Mr10']/text() | //span[@id='source_baidu']/span/text() | //div[@class='GSTitsL Cf']/span/a/text()").extract()
            sent_kafka_message['info_source'] = info_source[0] if info_source else None
            # 文章大图相关信息
            img_list = response.xpath("//div[@class='ArtM']//center//img/@src | //div[@class='pageBd']//center/a/img/@src | //div[@class='pageBd']//center//img/@src | //div[@class='Article']//center/a/img/@src").extract()
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
            content = response.xpath("//div[@class='ArtM']//text() | //div[@class='newDetailText']/text() | //div[@class='pageBd']/text() | //div[@class='Article']/text()").extract()
            content = content if content else None
            # print(content)
            if content:
                sent_kafka_message['parsed_content_main_body'] = ''.join(
                    [i for i in content])
                sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
                # 文章关键词标签
                tags=response.xpath("//div[@id='tags']//a/text()").extract()
                try:
                    sent_kafka_message['tags'] = tags[-3:] if tags else None
                except Exception as e:
                    sent_kafka_message['tags'] = None
                # 点赞数
                sent_kafka_message['like_count'] = None
                # 回复数
                sent_kafka_message['comment_count'] = None
                # 按照规定格式解析出的文章正文 <p>一段落</p>
                try:
                    sent_kafka_message['parsed_content'] = self.get_parsed_content(response, publish_time)
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
        base_body = response.xpath("//div[@class='ArtM'] | //div[@class='newDetailText'] | //div[@class='pageBd'] | //div[@class='Article']")[0].extract().replace(' ', '')
        base_body_str=''.join(base_body.split()).replace(u'<divclass="ArtM"id="Content">',u'<p>').replace(u'<br><br>',u'</p><p>').replace(u'<!--<spanid="editor_baidu"class="FrMt10">责任编辑：</span>--></div>','').replace(u'imgrel="nofollow"src',u'img rel="nofollow" src').replace(u'divclass',u'div class').replace(u'<tablecellspacing>','').replace(u'</tablecellspacing>','').replace(u'<div class="Article">',u'<p>').replace(u'</div>',u'</p>').replace(u'<div class="pageBd"id="__content">',u'<p>').replace(u'<div class="newDetailText"id="Content">',u'<p>')
        parsed_content = self.filter_tags(base_body_str,publish_time,response)
        return parsed_content

    def filter_tags(self, htmlstr,publish_time,response):
        publish_time=publish_time
        parsed_content = ''
        if htmlstr:
            try:
                soup = BeautifulSoup(htmlstr,'lxml')
                # print("3535"*30)
                # print(soup)
            except Exception as e:
                print e
            # parsed_content_main_body = soup.body.div.text
            try:
                if u"div" in soup:
                    p_content_document_list = soup.body.div.children
                else:
                    p_content_document_list = soup.body.children
            except Exception as e:
                print e
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
