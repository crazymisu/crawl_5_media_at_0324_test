# -*- coding: utf-8 -*-
import re
import requests
import urlparse
import random
import scrapy
import datetime
import bs4
from parsel import Selector
from bs4 import BeautifulSoup
from datetime import datetime
import copy

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class CaishimvParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        '''
            parse article url
            example:
                sent_kafka_message = SentKafkaMessage()
                sent_kafka_message['url'] = url
                sent_kafka_message['title'] = title
                ...
                yield sent_kafka_message
        '''
        try:
            original_sent_kafka_message = response.meta['queue_value']

            urls = response.xpath('//div[@class="innewsbox"]/ul/li')
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] =  url.xpath('.//h2/a/@href').extract()[0]
                sent_kafka_message['title'] = url.xpath('.//h2/a/text()').extract()[0]  # 标题
                sent_kafka_message['desc'] = url.xpath('.//p/text()').extract()[0]  # 简介
                sent_kafka_message['publish_time'] = url.xpath('.//p[@class="innews_txt3"]/span[2]/text()').extract()[0].strip() + ':00'

                sent_kafka_message['tags'] = url.xpath('.//span[@class="innews_txt3_3"]/a/text()').extract()
                small_img_url = url.xpath('.//img/@src').extract()[0]
                small_img_location, check_flag = self.get_small_img_location(small_img_url, sent_kafka_message['publish_time'])
                if small_img_location:
                    sent_kafka_message['small_img_location'] = small_img_location  # 小图
                    sent_kafka_message['small_img_location_count'] = len(small_img_location)  # 小图数量

                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['url_domain'] = 'caishimv.com'
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message

                # yield scrapy.Resquest(sent_kafka_message['url'], callback=self.parse_detail_page, meta={'queue_value': "sent_kafka_message"})

        except Exception as e:
            print e

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['status_code'] = response.status  # HTTP status code ; int type
            sent_kafka_message['body'] = response.body


            content_html = response.xpath('//div[@class="main_content"]').extract()[0]
            # 去除广告
            content_html = content_html.split(response.xpath('//div[@class="main_content"]/p').extract()[-1])[0] + '</div>'
            parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count = self.parse_article(
                content_html, sent_kafka_message['publish_time']
            )

            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count

            sent_kafka_message['parsed_content'] = parsed_content  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body  # 按照规定格式解析出的文章纯文本 ; string type
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count  # 按照规定格式解析出的文章纯文本个数 ; int type

            sent_kafka_message['authorized'] = response.xpath('//div[@class="main_content"]//strong[1]//text()').extract()[0]

            yield sent_kafka_message

        except Exception as e:
            print e


    def get_small_img_location(self, small_img_url, publish_time):
        check_flag, img_file_info = save_img_file_to_server(small_img_url, self.mongo_client, self.redis_client,
                                                            self.redis_key, publish_time)
        if check_flag:
            img = [{
                'img_src': small_img_url,
                'img_path': img_file_info['img_file_name'],
                'img_width': img_file_info['img_width'],
                'img_height': img_file_info['img_height'],
                'img_index': 1,
                'img_desc': None
            }]
            return img, True
        else:
            img = [{
                'img_src': small_img_url,
                'img_path': None,
                'img_width': None,
                'img_height': None,
                'img_index': 1,
                'img_desc': None
            }]
            return img, False

    def parse_article(self, content_html, pub_date):
        parsed_content = ''
        parsed_content_main_body = ''
        parsed_content_char_count = 0
        img_location = []
        img_location_count = 0
        img_index = 1

        if content_html:
            soup = BeautifulSoup(content_html,'lxml')
            parsed_content_main_body = soup.body.div.text.replace(u"\n", "").replace(u'\r',u'').replace(u' ', u'').strip()  # 纯文本
            parsed_content_char_count = len(parsed_content_main_body)  # 字符个数
            p_content_document_list = soup.body.div.children  # 段落
            for p in p_content_document_list:
                if isinstance(p,bs4.Tag):
                    if p.findChild('img'):
                        check_flag, img_file_info = save_img_file_to_server('http://www.caishimv.com' + p.img['src'],mongo_client=self.mongo_client,redis_client=self.redis_client , redis_key=self.redis_key,save_date=pub_date)
                        if check_flag:
                            img_location.append({'img_src':'http://www.caishimv.com' + p.img['src'],'img_path':img_file_info['img_file_name'],'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height'],'img_desc':None,'img_index':img_index})
                        else:
                            img_location.append({'img_src':'http://www.caishimv.com' + p.img['src'],'img_path':None,'img_desc':None,'img_index':img_index})
                        img_location_count += 1
                        img_index +=1
                        parsed_content += u'%s"%s"%s'% (u'<p><img src=',img_file_info['img_file_name'],u'/></p>')
                        if p.text != u' ':
                            p_content = u'%s%s%s' % (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

                    else:
                        if p.text!= u' ':
                            p_content = u'%s%s%s'% (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

            parsed_content = parsed_content.replace(u'\n',u'').replace(u'\r',u'').replace(u'<p></p>',u'')

        return parsed_content ,parsed_content_main_body,parsed_content_char_count,img_location,img_location_count

