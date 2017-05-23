# -*- coding: utf-8 -*-
import re
import requests
import urlparse
import random
import scrapy
import datetime
from parsel import Selector
from bs4 import BeautifulSoup
from datetime import datetime
import copy

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class XiaofeishibaoParser(Parser):
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

            urls = response.xpath('//div[@class="page_left"]//dt/a/@href').extract()
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] = 'http://www.xdxfdb.cn' + url
                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['url_domain'] = 'xdxfdb.cn'
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message

                # yield scrapy.Resquest(sent_kafka_message['url'], callback=self.parse_detail_page, meta={'queue_value': "sent_kafka_message"})

        except Exception as e:
            pass

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['status_code'] = response.status  # HTTP status code ; int type
            sent_kafka_message['title'] = response.xpath('//h1/text()').extract()[0].strip() # title
            sent_kafka_message['desc'] = self.get_desc(response)  # 文章摘要 ; string type
            sent_kafka_message['body'] = response.body  # 网页源代码 不需要base64加密 ; string type
            sent_kafka_message['publish_time'] = self.get_publish_time(response)  # 发布时间 ; string type
            sent_kafka_message['author'] = self.get_author(response)

            sent_kafka_message['img_location'], check_flag = self.get_img_location(response,sent_kafka_message['publish_time']) # 文章大图相关信息 ; list type ; [{img_src: '', img_path: '', 'img_index':'', 'img_desc':''}]
            # if not check_flag:  # 如果图片下载失败
            #     sent_kafka_message['parse_function'] = 'parse_detail_page'
            #     sent_kafka_message['body'] = None
            #     return sent_kafka_message  # 返回
            if sent_kafka_message['img_location'] is not None:
                sent_kafka_message['img_location_count'] = len(sent_kafka_message['img_location']) # 文章大图个数 ; int type

            sent_kafka_message['parsed_content'] = self.get_parsed_content(response, sent_kafka_message['img_location'])  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] = self.get_parsed_content_main_body(response)  # 按照规定格式解析出的文章纯文本 ; string type
            sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])  # 按照规定格式解析出的文章纯文本个数 ; int type


            return sent_kafka_message
        except Exception as e:
            pass

    def get_desc(self, response):
        # 文章摘要 ; string type
        try:
            desc = response.xpath('//div[@class="desc"]/text()').extract()[0].strip()
        except Exception as e:
            desc = None
        return desc

    def get_publish_time(self, response):
        # 发布时间 ; string type
        page_con_info = response.xpath('//span[@class="page_con_info"]/text()').extract()[0].strip()
        l = page_con_info.partition(u'作者')
        publish_time = l[0].strip()
        return publish_time

    def get_author(self, response):
        # 文章作者
        page_con_info = response.xpath('//span[@class="page_con_info"]/text()').extract()[0].strip()
        l = page_con_info.partition(u'作者：')
        author = l[2]
        author = author.replace(u'摄', '').strip()
        return author

    def get_img_location(self, response, publish_time):
        # 文章大图相关信息 ; list type ; [{img_src: '', img_path: '', 'img_index':'', 'img_desc':''}]
        try:
            urls = response.xpath('//div[@class="con"]//img/@src').extract()
            # img_srcs = 'http://www.xdxfdb.cn' + url
        except:
            urls = None

        if urls is not None:  # 如果有图
            img_location = []
            for url in urls:
                img_src = 'http://www.xdxfdb.cn' + url

                check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,self.redis_key, publish_time)
                if check_flag:  # 如果图片下载成功
                    img = {
                        'img_src': img_src,
                        'img_path': img_file_info['img_file_name'],
                        'img_width': img_file_info['img_width'],
                        'img_height': img_file_info['img_height'],
                        'img_index': str(urls.index(url) + 1),
                        'img_desc': None
                    }
                else:
                    img = {
                        'img_src': url,
                        'img_path': None,
                        'img_width': None,
                        'img_height': None,
                        'img_index': str(img_urls.index(url) + 1),
                        'img_desc': None
                    }
                img_location.append(img)
                # else:  # 如果图片下载失败
                #     return None, False
            return img_location, True  # 如果全部下载成功
        else:
            return None, True  # 没有图片

    def get_parsed_content(self, response, img_location):
        # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
        con = response.xpath('//div[@class="con"]').extract()
        c = ''.join(con)
        parsed_content = self.filter_tags(c, img_location)
        return parsed_content

    def get_parsed_content_main_body(self, response):
        # 按照规定格式解析出的文章纯文本 ; string type
        con = response.xpath('//div[@class="con"]//text()').extract()
        base_body = ''.join(con)
        re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
        re_br = re.compile('<br\s*?/?>')  # 处理换行
        re_h = re.compile('</?\w+[^>]*>')  # HTML标签
        re_comment = re.compile('<!--[^>]*-->')  # HTML注释
        s = re_style.sub('', base_body)  # 去掉style
        s = re_br.sub('\n', s)  # 将br转换为换行
        s = re_h.sub('', s)  # 去掉HTML 标签
        s = re_comment.sub('', s)  # 去掉HTML注释
        blank_line = re.compile('\n+')
        parsed_content_main_body = blank_line.sub('\n', s)
        return parsed_content_main_body.strip()


    def filter_tags(self, htmlstr, img_location):
        # 解析标签，替换img标签src为本地地址，去除p标签和img标签以外的标签
        div = re.findall(r'<div.*?>', htmlstr, re.S)
        div2 = re.findall(r'</div.*?>', htmlstr, re.S)
        annotations = re.findall(r'<!--[^>]*-->', htmlstr, re.S)
        strong = re.findall(r'<strong>', htmlstr, re.S)
        strong2 = re.findall(r'</strong>', htmlstr, re.S)
        br = re.findall(r'<br>', htmlstr,re.S)
        br2 = re.findall(r'</br>', htmlstr, re.S)
        span = re.findall(r'<span.*?>', htmlstr)
        span2 = re.findall(r'</span.*?>', htmlstr, re.S)
        blockquote = re.findall(r'<blockquote.*?>', htmlstr, re.S)
        blockquote2 = re.findall(r'</blockquote.*?>', htmlstr, re.S)
        ul = re.findall(r'<ul.*?>', htmlstr, re.S)
        ul2 = re.findall(r'</ul.*?>', htmlstr, re.S)
        li = re.findall(r'<li.*?>', htmlstr, re.S)
        li2 = re.findall(r'</li.*?>', htmlstr, re.S)
        a = re.findall(r'<a.*?>', htmlstr, re.S)

        p = re.findall(r'<p.*?>', htmlstr, re.S)
        for i in p:
            htmlstr = htmlstr.replace(i, '<p>')

        img = re.findall(r'<img.*?>', htmlstr, re.S)
        for i in img:
            new_img = '<' + re.findall(r'img src=".*?"', i, re.S)[0] + '>'
            htmlstr = htmlstr.replace(i, new_img)

        t = div + div2 + annotations + strong + strong2 + br + br2 + span + span2 + blockquote + blockquote2 + ul + ul2 + li + li2 + a
        for i in t:
            htmlstr = htmlstr.replace(i, '')
        body = htmlstr.replace('<p></p>', '').replace('\n', '')
        body = htmlstr.replace('<p> </p>', '').replace('\n', '')
        body = body.strip()

        if img_location is not None:
            for i in img_location:
                src = i['img_src'].replace('http://www.xdxfdb.cn', '')
                body = body.replace(src, i['img_path']['img_file_name'])
        return body