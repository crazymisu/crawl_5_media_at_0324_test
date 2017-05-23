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


class Soft6Parser(Parser):
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
            # 每页8条
            urls = response.xpath('//div[@class="main_left"]//ul/li')
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                try:  # 中间穿插了广告
                    # 详情页url
                    sent_kafka_message['url'] = url.xpath('./h1[@class="title"]/a/@href').extract()[0]
                except:
                    continue
                # 标题
                sent_kafka_message['title'] = url.xpath('./h1[@class="title"]/a/text()').extract()[0]
                # 简介
                sent_kafka_message['desc'] = self.get_desc(url)
                # 发布时间
                sent_kafka_message['publish_time'] = url.xpath('.//div[@class="time"]/span/text()').extract()[0].strip()
                # tag
                sent_kafka_message['tags'] = self.get_tags(url)

                small_img_location, check_flag = self.get_small_img_location(url, sent_kafka_message['publish_time'])
                if small_img_location:
                    sent_kafka_message['small_img_location'] = small_img_location
                    sent_kafka_message['small_img_location_count'] = len(small_img_location)

                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['url_domain'] = 'cnetnews.com'
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

            sent_kafka_message['info_source'] = self.get_info_source(response)
            sent_kafka_message['author'] = self.get_author(response)

            sent_kafka_message['authorized'] = self.get_authorized(response)

            content_html, content_html_head = self.get_content_html(response)

            parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count = self.parse_article(
                content_html, sent_kafka_message['publish_time'], content_html_head
            )

            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count

            sent_kafka_message['parsed_content'] = parsed_content  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body  # 按照规定格式解析出的文章纯文本 ; string type
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count  # 按照规定格式解析出的文章纯文本个数 ; int type

        except Exception as e:
            pass


    def get_content_html(self, response):
        try:  # 如果文章正文中包含div
            content_html = response.xpath('//div[@class="main_left"]//div[@class="content"]//div').extract()[0]
        except Exception as e:  # 不包含div直接返回
            content_html = response.xpath('//div[@class="main_left"]//div[@class="content"]').extract()[0]
            return content_html, None

        content_html_head = response.xpath('//div[@class="main_left"]//div[@class="content"]/text()').extract()[0].strip()
        # content_html_head = u'%s' % content_html_head

        return content_html, content_html_head


    def get_desc(self, url):
        desc = url.xpath('.//div[@class="desbox"]/text()').extract()[0].strip()
        return desc

    def get_tags(self, url):
        tags = url.xpath('.//div[@class="kwbox"]//a/text()').extract()
        return tags

    def get_info_source(self, response):
        x = response.xpath('//div[@class="main_left"]//div[@class="des_box"]/span/text()').extract()
        info_source = x[1][3:]
        return info_source

    def get_author(self, response):
        x = response.xpath('//div[@class="main_left"]//div[@class="des_box"]/span/text()').extract()
        author = x[2][3:]
        if author == '':
            return None
        else:
            return author

    def get_authorized(self, response):
        authorized = u'免责声明：本站系本网编辑转载，本站会尽可能注明出处，但不排除无法注明来源的情况，转载目的在于传递更多信息，并不代表本网赞同其观点和对其真实性负责。如涉及作品内容、版权和其它问题，请在30日内与本网联系， 来信: liujun@soft6.com 我们将在收到邮件后第一时间删除内容！[声明]本站文章版权归原作者所有，内容为作者个人观点，不代表本网站的观点和对其真实性负责，本站拥有对此声明的最终解释权。'

        return authorized


    def get_small_img_location(self, url, publish_time):
        try:
            small_img_url = url.xpath('.//img/@src').extract()[0]
        except:
            return None, True
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

    def parse_article(self, content_html, pub_date, content_html_head):
        parsed_content = ''
        parsed_content_main_body = ''
        parsed_content_char_count = 0
        img_location = []
        img_location_count = 0
        img_index = 1

        if content_html:
            soup = BeautifulSoup(content_html,'lxml')
            parsed_content_main_body = soup.body.div.text.replace(u"\n", "").replace(u'\r',u'').replace(u' ', u'').strip()  # 纯文本
            if content_html_head:
                parsed_content_main_body = content_html_head + parsed_content_main_body

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

            if content_html_head:
                parsed_content = '<p>' + content_html_head + '</p>' + parsed_content

        return parsed_content ,parsed_content_main_body,parsed_content_char_count,img_location,img_location_count

