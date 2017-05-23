# -*- coding: utf-8 -*-

import scrapy
import re
from urlparse import urlparse, urlunparse
from datetime import datetime, timedelta
import json
import copy
import traceback

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class CtoutiaoParser(Parser):    
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
        sent_kafka_message_list = []
        try:
            original_sent_kafka_message = response.meta['queue_value']

            list_page_url = original_sent_kafka_message['url']   
            if IS_CRAWL_NEXT_PAGE and '?p=' not in list_page_url and 'p=' not in list_page_url:
                for page_index in range(2, 11):
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)                
                    has_params = '?' in list_page_url
                    if has_params:
                        page_param = '&p={0}'.format(page_index)
                    else:
                        page_param = '?p={0}'.format(page_index)
                    next_page_url = list_page_url + page_param
                    sent_kafka_message['url'] = next_page_url
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    # print('next_page_url = {0}'.format(next_page_url))
                    # yield sent_kafka_message
                    sent_kafka_message_list.append(sent_kafka_message)
                    
            li_document_list = response.xpath('//ul[contains(@class, "recommend-list")]/li')
            for li_document in li_document_list:
                find_div_document = li_document.xpath('.//div[contains(@class, "A_list Nclea")]')
                if find_div_document:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)

                    small_img_location = find_div_document[0].xpath('.//div[1]/a/img/@src').extract()
                    url = find_div_document[0].xpath('.//div[2]/h1/a/@href').extract()
                    title = find_div_document[0].xpath('.//div[2]/h1/a/@title').extract()                
                    desc = find_div_document[0].xpath('.//div[2]/h6/text()').extract()
                    publish_time = find_div_document[0].xpath('.//div[2]').xpath('.//span[contains(@class, "A_m")]/text()').extract()

                    if small_img_location and small_img_location[0]:
                        small_img_src = small_img_location[0]
                        check_flag, img_file_info = save_img_file_to_server(small_img_src, self.mongo_client, self.redis_client, self.redis_key, publish_time[0] if publish_time else self.now_date)
                        if check_flag:
                            small_img_location = [{'img_src': small_img_src, 'img_path': img_file_info['img_file_name'], 'img_index': 1, 'img_desc': None, 'img_width': img_file_info['img_width'], 'img_height': img_file_info['img_height']}]
                        else:
                            small_img_location = [{'img_src': small_img_src, 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None}]
                        sent_kafka_message['small_img_location'] = small_img_location
                        sent_kafka_message['small_img_location_count'] = len(small_img_location)

                    sent_kafka_message['url'] = url[0] if url else None
                    sent_kafka_message['title'] = title[0] if title else None
                    sent_kafka_message['desc'] = desc[0] if desc else None
                    sent_kafka_message['publish_time'] = publish_time[0] if publish_time else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'

                    if 'http' not in sent_kafka_message['url']:
                        scheme, netloc, path, params, query, fragment = urlparse(response.url)
                        sent_kafka_message['url'] = urlunparse((scheme, netloc, sent_kafka_message['url'], '', '', ''))
                    # print(str(sent_kafka_message))
                    # yield sent_kafka_message
                    sent_kafka_message_list.append(sent_kafka_message)
                    continue

                find_div_document = li_document.xpath('.//div[contains(@class, "A_list A_lists2 Nclea")]')
                if find_div_document:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)

                    url = find_div_document[0].xpath('.//div[1]/h1/a/@href').extract()
                    title = find_div_document[0].xpath('.//div[1]/h1/a/text()').extract()                
                    desc = find_div_document[0].xpath('.//div[1]/h6/text()').extract()
                    publish_time = find_div_document[0].xpath('.//div[2]').xpath('.//span[contains(@class, "A_m")]/text()').extract()

                    sent_kafka_message['url'] = url[0] if url else None
                    sent_kafka_message['title'] = title[0] if title else None
                    sent_kafka_message['desc'] = desc[0] if desc else None
                    sent_kafka_message['publish_time'] = publish_time[0] if publish_time else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'

                    if 'http' not in sent_kafka_message['url']:
                        scheme, netloc, path, params, query, fragment = urlparse(response.url)
                        sent_kafka_message['url'] = urlunparse((scheme, netloc, sent_kafka_message['url'], '', '', ''))
                    # print(str(sent_kafka_message))
                    # yield sent_kafka_message
                    sent_kafka_message_list.append(sent_kafka_message)
                    continue

                find_div_document = li_document.xpath('.//div[contains(@class, "recommend-con recommend-con2")]')
                if find_div_document:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)

                    url = find_div_document[0].xpath('.//a/@href').extract()
                    parse_function = 'parse_list_page'
                    sent_kafka_message['url'] = url[0] if url else None
                    sent_kafka_message['parse_function'] = parse_function if parse_function else None

                    if 'http' not in sent_kafka_message['url']:
                        scheme, netloc, path, params, query, fragment = urlparse(response.url)
                        sent_kafka_message['url'] = urlunparse((scheme, netloc, sent_kafka_message['url'], '', '', ''))
                    # print(str(sent_kafka_message))
                    # yield sent_kafka_message
                    sent_kafka_message_list.append(sent_kafka_message)
                    continue

                find_div_document = li_document.xpath('.//div[contains(@class, "Newlelibox")]')
                if find_div_document:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    
                    url = find_div_document[0].xpath('.//h2/a/@href').extract()
                    title = find_div_document[0].xpath('.//h2/a/text()').extract()                
                    desc = find_div_document[0].xpath('.//h6/text()').extract()
                    publish_time = find_div_document[0].xpath('.//h5/text()').extract()

                    sent_kafka_message['url'] = url[0] if url else None
                    sent_kafka_message['title'] = title[0] if title else None
                    sent_kafka_message['desc'] = desc[0] if desc else None
                    sent_kafka_message['publish_time'] = publish_time[0] if publish_time else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'

                    if 'http' not in sent_kafka_message['url']:
                        scheme, netloc, path, params, query, fragment = urlparse(response.url)
                        sent_kafka_message['url'] = urlunparse((scheme, netloc, sent_kafka_message['url'], '', '', ''))
                    # print(str(sent_kafka_message))
                    # yield sent_kafka_message 
                    sent_kafka_message_list.append(sent_kafka_message)
                    continue
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

        for sent_kafka_message in sent_kafka_message_list:
            yield sent_kafka_message


    def parse_detail_page(self, response):
        try:
            news_info_document = response.xpath('//div[contains(@class, "A_ctes")]')
            news_head_document = news_info_document.xpath('.//p[contains(@class, "A_pon1")]')
            news_content_document = news_info_document.xpath('.//div[contains(@class, "A_contxt")]')

            publish_time = news_head_document.xpath('.//em[1]/text()').extract_first()
            click_count = news_head_document.xpath('.//em[2]/text()').extract_first()
            info_source = news_head_document.xpath('.//em[3]/text()').extract_first()
            tag_list = news_info_document.xpath('.//div[contains(@class, "A_linebn")]/a')
            tag = [tag_item.xpath('.//text()').extract_first() for tag_item in tag_list]
            publish_time = self.parse_toutiao_publish_time(str(self.get_webname_time(publish_time.strip().split(u'：')[-1])))
            parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag = self.extract_article_parsed_content(news_content_document.extract_first(), publish_time)              

            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['url_domain'] = urlparse(sent_kafka_message['url']).netloc
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body
            sent_kafka_message['publish_time'] = publish_time
            sent_kafka_message['author'] = None
            sent_kafka_message['info_source'] = info_source.split(u'：')[-1].strip() if info_source else None
            sent_kafka_message['video_location'] = None
            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count
            sent_kafka_message['parsed_content'] = parsed_content
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
            sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
            sent_kafka_message['tags'] = tag
            sent_kafka_message['like_count'] = 0
            sent_kafka_message['click_count'] = int(re.findall(r'\d+', click_count)[0])
            sent_kafka_message['comment_count'] = 0
            sent_kafka_message['repost_count'] = 0     
            sent_kafka_message['authorized'] = authorized

            '''
                parse detail page
                example:
                    page_html = response.body
                    parse_result =  self.parse_article_content(page_html)
            '''

            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())
        

    def get_webname_time(self, web_and_time_t):
        web_and_time = web_and_time_t.split()
        now = datetime.now()
        # 今天 10:00   或者 11-13 11:04 或者2015-11-13 11:00
        if len(web_and_time) == 2 and u'前' not in web_and_time[1]:
            if u"今天" in web_and_time[0]:
                date_str = "{}{}{} {}".format(
                    now.year, now.month, now.day, web_and_time[1])
                dt = datetime.strptime(
                    date_str, "%Y%m%d %H:%M").isoformat()
                return dt
            elif len(web_and_time[0].split("-")) == 4:
                return datetime.strptime(web_and_time_t[5:], "%Y-%m-%d %H:%M").isoformat()
            elif len(web_and_time[0].split("-")) == 3:
                return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M").isoformat()
            else:  # 11-13 11:04
                dt = datetime.strptime("{}-{}".format(now.year, web_and_time_t),
                                       "%Y-%m-%d %H:%M").isoformat()
                return dt
        elif u'前' in web_and_time_t:
            if u'天前' in web_and_time_t:
                time_back = web_and_time_t.split(u'天前')[0]
                dt = (now - timedelta(days=int(time_back))).isoformat()
                return dt
            elif u'小时' in web_and_time_t:
                time_back = web_and_time_t.split(u'小时')[0]
                dt = (now - timedelta(hours=int(time_back))).isoformat()
                return dt
            else:
                time_back = web_and_time_t.split(u'分钟')[0]
                dt = (now - timedelta(minutes=int(time_back))).isoformat()
                return dt
        else:
            return web_and_time_t