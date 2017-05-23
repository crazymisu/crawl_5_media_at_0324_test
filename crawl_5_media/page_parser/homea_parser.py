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
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class HomeaParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):

        try:
            original_sent_kafka_message = response.meta['queue_value']

            details = response.xpath('//table//tr')
            for detail in details:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] =  'http://info.homea.hc360.com' + detail.xpath('.//a/@href').extract_first()
                sent_kafka_message['title'] = detail.xpath('.//a//text()').extract_first()  # 标题

                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['url_domain'] = 'homea.com'
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message

                # yield scrapy.Resquest(sent_kafka_message['url'], callback=self.parse_detail_page, meta={'queue_value': "sent_kafka_message"})

        except Exception as e:
            pass

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['status_code'] = response.status  # HTTP status code ; int type
            sent_kafka_message['body'] = response.body

            publish_time = response.xpath('//span[@id="endData"]/text()').extract_first().strip()
            sent_kafka_message['publish_time'] = self.get_time(publish_time)

            sent_kafka_message['info_source'] = response.xpath('//span[@id="endSource"]/text()').extract_first().strip()[3:]
            # 来源：北京商报

            if sent_kafka_message['parsed_content']:
                sent_kafka_message['img_location'] = []
                sent_kafka_message['img_location_count'] = 0
                sent_kafka_message['parsed_content'] = ''
                sent_kafka_message['parsed_content_main_body'] = ''
                sent_kafka_message['parsed_content_char_count'] = 0

            content_html = response.xpath('//div[@id="artical"]').extract()[0]
            # 去除正文中的责任编辑
            bianji = response.xpath('//p[@class="editorN"]').extract_first()
            if bianji:
                content_html = content_html.replace(bianji, '')
            parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count = self.parse_article(
                content_html, sent_kafka_message['publish_time']
            )

            sent_kafka_message['img_location'] += img_location
            sent_kafka_message['img_location_count'] += img_location_count

            sent_kafka_message['parsed_content'] += parsed_content  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] += parsed_content_main_body  # 按照规定格式解析出的文章纯文本 ; string type
            sent_kafka_message['parsed_content_char_count'] += parsed_content_char_count  # 按照规定格式解析出的文章纯文本个数 ; int type

            if '下一页' not in response.xpath('//div[@id="pageno"]/span[@class="pageno_on"]/text()').extract():
                # 1018111192025-2.shtml
                u = response.xpath('//div[@id="pageno"]/a/@href').extract()[-1]
                # 1018111192025.shtml
                old_u = sent_kafka_message['url'].split('/')[-1]

                sent_kafka_message['url'] = sent_kafka_message['url'].replcae(old_u, u)
                sent_kafka_message['parse_function'] = 'parse_detail_page'


        except Exception as e:
            pass

    def get_time(self,source_time):
        if u'小时前' in source_time:
            time_back = source_time.split(u'小时前')[0]
            dt = str((datetime.datetime.now() - datetime.timedelta(hours=int(time_back)))).split(r'.')[0]
        elif u'分钟前' in source_time:
            time_back = source_time.split(u'分钟前')[0]
            dt = str((datetime.datetime.now() - datetime.timedelta(minutes=int(time_back)))).split(r'.')[0]
        else:
            temp = str(source_time.replace(u'年', '-').replace(u'月', '-').replace(u'日', ' ')) + ':00'
            dt = datetime.datetime.strptime(temp, '%Y-%m-%d %H:%M:%S')
        return str(dt)


    def get_small_img_location(self, url, publish_time):
        try:
            small_img_url = url.xpath('.//img/@src').extract()[0]
        except:
            return None, True  # 没有小图返回None
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
                        check_flag, img_file_info = save_img_file_to_server(p.img['src'],mongo_client=self.mongo_client,redis_client=self.redis_client , redis_key=self.redis_key,save_date=pub_date)
                        if check_flag:
                            img_location.append({'img_src':p.img['src'],'img_path':img_file_info['img_file_name'],'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height'],'img_desc':None,'img_index':img_index})
                        else:
                            img_location.append({'img_src':p.img['src'],'img_path':None,'img_desc':None,'img_index':img_index})
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