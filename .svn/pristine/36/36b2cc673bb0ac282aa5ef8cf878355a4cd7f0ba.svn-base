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

class CnetParser(Parser):
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

            urls = response.xpath('//div[@class="qu_loop"]')
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] =  url.xpath('.//div[@class="qu_tix"]/b/a/@href').extract()[0]
                sent_kafka_message['title'] = url.xpath('.//div[@class="qu_tix"]/b/a/text()').extract()[0]  # 标题
                sent_kafka_message['desc'] = url.xpath('.//div[@class="qu_tix"]/p/text()').extract()[0]  # 简介
                sent_kafka_message['publish_time'] = url.xpath('./div[@class="qu_times"]/text()').extract()[0].strip()

                sent_kafka_message['tags'] = self.get_tags(url)  # 标签
                small_img_location, check_flag = self.get_small_img_location(url, sent_kafka_message['publish_time'])
                if small_img_location:
                    sent_kafka_message['small_img_location'] = small_img_location  # 小图
                    sent_kafka_message['small_img_location_count'] = len(small_img_location)  # 小图数量

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
            sent_kafka_message['author'] = self.get_author(response)  # 文章作者
            sent_kafka_message['info_source'] = self.get_info_source(response)  # 文章的信息来源 ; string type

            content_html = response.xpath('//div[@class="qu_ocn"]').extract()[0]
            xiaoxi = re.search(r'<span.*?</span>', content_html, re.S).group(0)  # 文章中的消息来源
            try:
                tb = re.search(r'<table.*?</table>', content_html, re.S).group(0)  # 如果有table布局
                content_html = content_html.replace(tb, '')
            except:
                pass
            content_html = content_html.replace(xiaoxi, '').strip()  # 去除正文中消息报道来源

            parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count = self.parse_article(
                content_html, sent_kafka_message['publish_time']
            )

            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count

            sent_kafka_message['parsed_content'] = parsed_content  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body  # 按照规定格式解析出的文章纯文本 ; string type
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count  # 按照规定格式解析出的文章纯文本个数 ; int type


            # img_location = self.get_img_locaion(response, sent_kafka_message['publish_time']) # 文章大图相关信息 ; list type ; [{img_src: '', img_path: '', 'img_index':'', 'img_desc':''}]
            # sent_kafka_message['img_location'] = img_location
            # if img_location:
            #     sent_kafka_message['img_location_count'] = len(sent_kafka_message['img_location'])  # 图片数量
            #
            # sent_kafka_message['parsed_content'] = self.get_parsed_content(response, img_location)  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            # sent_kafka_message['parsed_content_main_body'] = self.get_parsed_content_main_body(sent_kafka_message['parsed_content'])  # 按照规定格式解析出的文章纯文本 ; string type
            # sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])  # 按照规定格式解析出的文章纯文本个数 ; int type

        except Exception as e:
            pass

    def get_tags(self, url):
        tags = url.xpath('.//div[@class="qu_tix"]//p[@class="meta"]/a/@title').extract()
        return tags

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

    def get_author(self, response):
        text = response.xpath('//div[@class="qu_lix"]//div[@class="qu_zuo"]//text()').extract()[0]
        info_list = text.split(u'来源')
        for info in info_list:
            if u'作者' in info:  # 如果有作者
                author = info[3:]
                return author  # 返回作者名称
        return None  # 返回空

    def get_info_source(self, response):
        text = response.xpath('//div[@class="qu_lix"]//div[@class="qu_zuo"]/p/text()').extract()[0]
        info_list = text.split(' ')
        for info in info_list:
            if u'来源' in info:  # 如果有来源
                author = info[3:]
                return author
        return None

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