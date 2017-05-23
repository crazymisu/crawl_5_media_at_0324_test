# -*- coding: utf-8 -*-
__author__ = 'liangchu'
__date__ = '2017/5/16'

from scrapy.spiders import CrawlSpider
from scrapy.selector import Selector
import urlparse
from scrapy import Request
import bs4
import re
from datetime import datetime
from bs4 import BeautifulSoup
import copy
import traceback

from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE


class Jiadianmofang_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()


    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']        

            sel = Selector(response)
            article_list = sel.xpath('//div[@class="listpic"]')
            article = {}
            next_page = response.xpath('//a[@class="nxt"]/@href').extract_first()

            for a in article_list:
                small_img_location = []
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)

                article['url'] = a.xpath('./a/@href').extract_first()
                article['domain'] = 'jiadianmofang.com'
                article['title'] = a.xpath('./a/img/@alt').extract_first()
                small_img_src = a.xpath('./a/img/@src').extract_first()
                small_img_url = urlparse.urljoin(u'http://www.jiadianmofang.com',small_img_src)
                small_check_flag, small_img_file_info = save_img_file_to_server(small_img_url, mongo_client=self.mongo_client, redis_client=self.redis_client, redis_key=self.redis_key, save_date=None)
                if small_check_flag :
                    small_img_location.append({'img_src':small_img_url,'img_path':small_img_file_info['img_file_name'],'img_desc':None,'img_index':1, 'img_height': small_img_file_info['img_height'], 'img_width': small_img_file_info['img_width']})

                else:
                    small_img_location = [{'img_src': small_img_url, 'img_path': None, 'img_index': 1, 'img_desc': article['title'], 'img_width': None, 'img_height': None}]

                sent_kafka_message['url'] = article['url'] 
                sent_kafka_message['url_domain'] = article['domain'] 
                sent_kafka_message['title'] = article['title']
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                sent_kafka_message['small_img_location'] = small_img_location
                sent_kafka_message['small_img_location_count'] = 1
                yield sent_kafka_message



        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())
        


    def parse_detail_page(self, response):
        try:
            sel = Selector(response)
            if response.status == 200:
                sent_kafka_message = response.meta['queue_value']


                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['status_code'] = response.status

                sent_kafka_message['body'] = response.body
                info_source = sel.xpath('//div[@class="h hm"]/p')
                if info_source:
                    sent_kafka_message['info_source'] = info_source.xpath('./a/text()').extract_first()
                    publish_time = info_source.xpath('./text()').extract_first().replace(u'\r\n','')
                    sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
                    sent_kafka_message['click_count'] = int(info_source.xpath('./em/text()').extract_first())
                    sent_kafka_message['comment_count'] = int(info_source.xpath('./text()')[3].extract().split(u':')[1])

                content_html = sel.xpath('//td[@id="article_content"]').extract_first()
                content = self.parse_article_content(content_html,sent_kafka_message['publish_time'])
                download_img_flag = content[5]
                if download_img_flag:
                    sent_kafka_message['parsed_content'] = content[0]
                    sent_kafka_message['parsed_content_main_body'] = content[1]
                    sent_kafka_message['parsed_content_char_count'] = content[2]
                    sent_kafka_message['img_location'] = content[3]
                    sent_kafka_message['img_location_count'] = content[4]

                return sent_kafka_message
          
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())




    def parse_article_content(self, content_html,pub_date):
        parsed_content = ''
        parsed_content_char_count = 0
        img_location = []
        img_location_count = 0
        img_index = 1
        download_img_flag = True

        if content_html:
            soup = BeautifulSoup(content_html)
            parsed_content_main_body = soup.body.td.text.replace(u'\n', u'').replace(u'\r',u'').replace(u' ',u'').strip()
            parsed_content_char_count = len(parsed_content_main_body)
            p_content_document_list = soup.body.td.children
            for p in p_content_document_list:
                if isinstance(p,bs4.Tag):
                    if p.findChild('img'):
                        img_url = urlparse.urljoin(u'http://www.jiadianmofang.com',p.img['src'])
                        check_flag, img_file_info = save_img_file_to_server(img_url, mongo_client=self.mongo_client, redis_client=self.redis_client, redis_key=self.redis_key, save_date=pub_date)
                        if check_flag:
                            p_path = img_file_info['img_file_name'] 
                            img_location.append({'img_src':img_url,'img_path':p_path,'img_desc':None,'img_dex':img_index, 'img_height': img_file_info['img_height'], 'img_width': img_file_info['img_width']})
                        else:
                            p_path = p.img['src']
                            img_location.append({'img_src': p_path, 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                        img_location_count += 1
                        img_index += 1
                        parsed_content += u'%s"%s"%s' % (u'<p><img src=', p_path, u'/></p>')
                        if p.text != u' ':
                            p_content =u'%s%s%s' % (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

                    else:
                        if p.text!= u' ':
                            p_content = u'%s%s%s' % (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

            parsed_content = parsed_content.replace(u'\n', u'').replace(u'\r', u'').replace(u'<p></p>',u'')
        return parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count, download_img_flag


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
                print e
        return publish_time




    def get_time(self,source_time):
        if True:
            if u'小时前' in source_time:
                time_back = source_time.split(u'小时前')[0]
                dt = str((datetime.datetime.now() - datetime.timedelta(hours=int(time_back)))).split(r'.')[0]
            elif u'分钟前' in source_time:
                time_back = source_time.split(u'分钟前')[0]
                dt = str((datetime.datetime.now() - datetime.timedelta(minutes=int(time_back)))).split(r'.')[0]
            else:
                temp = str(source_time.replace(u'年', '-').replace(u'月', '-').replace(u'日', ' ')) + ':00'
                print temp
                dt = datetime.datetime.strptime(temp, '%Y-%m-%d %H:%M:%S')
            return str(dt)


