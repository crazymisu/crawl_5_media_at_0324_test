# -*- coding: utf-8 -*-
__author__ = 'liangchu'
__date__ = '2017/5/15'

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


class Diankeji_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()


    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']        

            sel = Selector(response)
            article_list =  sel.xpath('//ul[@class="isgood-list article-list"]/li')
            article = {}
            next_page = urlparse.urljoin(u'http://www.diankeji.com/',response.xpath('//ul[@class="pagination"]/li')[-2].xpath('./a/@href').extract_first())

            for a in article_list:
                small_img_location = []
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)

                article['url'] = urlparse.urljoin('http://www.diankeji.com',a.xpath('./h2/a/@href').extract_first())
                article['domain'] = 'diankeji.com'
                article['title'] = a.xpath('./h2/a/@title').extract_first()
                small_img_src = a.xpath('./a/div/div/img/@src').extract_first()
                small_img_url = urlparse.urljoin(u'http://www.diankeji.com/',small_img_src)
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
                publish_time = sel.xpath('//h5')

                if publish_time:
                    sent_kafka_message['publish_time'] = self.extract_time_text(publish_time)
                    print sent_kafka_message['publish_time']
                info_source = sel.xpath('//h5/text()').extract_first()
                if info_source:
                    sent_kafka_message['info_source'] = info_source.split(u':')[1]
                sent_kafka_message['author'] = sel.xpath('//h5/a/text()').extract_first()


                content_html = sel.xpath('//div[@class="arttext"]').extract_first()
                content = self.parse_article_content(content_html,sent_kafka_message['publish_time'])
                download_img_flag = content[5]
                if download_img_flag:
                    sent_kafka_message['parsed_content'] = content[0]
                    sent_kafka_message['parsed_content_main_body'] = content[1]
                    sent_kafka_message['parsed_content_char_count'] = content[2]
                    sent_kafka_message['img_location'] = content[3]
                    sent_kafka_message['img_location_count'] = content[4]
                tag_list = sel.xpath('//div[@class="clearfix  showtags"]/child::a')
                sent_kafka_message['tags'] = [x.xpath('./text()').extract_first() for x in tag_list]
                sent_kafka_message['authorized'] = u'如需转载电科技的原创文章，请注明来源"电科技"和作者，不得对作品进行摘编或者删改，违者电科技将坚持追究责任'
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
            parsed_content_main_body = soup.body.div.text.replace(u'\n', u'').replace(u'\r',u'').replace(u' ',u'').strip()
            parsed_content_char_count = len(parsed_content_main_body)
            p_content_document_list = soup.body.div.children
            for p in p_content_document_list:
                if isinstance(p,bs4.Tag):
                    if p.findChild('img'):
                        img_url = urlparse.urljoin(u'http://www.diankeji.com/',p.img['src'])
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
                        if p.text != u' ' and p.name!= 'div':
                            p_content =u'%s%s%s' % (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

                    else:
                        if p.text!= u' ' and p.name!= 'div':
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




    def get_webname_time(self, web_and_time_t):
        """
        时间统一格式
        :param self:
        :param web_and_time_t:可能出现的格式有：
            今天 10:50
            1分钟前
            1小时前
            11-13 11:44
            2016-2015-01-31 22:00
            2015-01-31 22:00
        :return:
        """
        web_and_time = web_and_time_t.split()
        now = datetime.now()
        if len(web_and_time) == 2 and u'前' not in web_and_time[1]:  # 今天 10:00   或者 11-13 11:04 或者2015-11-13 11:00
            if u"今天" in web_and_time[0]:
                date_str = "{0}-{1}-{2} {3}".format(now.year, now.month, now.day, web_and_time[1])
                dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M").isoformat()
                return dt
            elif len(web_and_time[0].split("-")) == 4:
                return datetime.datetime.strptime(web_and_time_t[5:], "%Y-%m-%d %H:%M").isoformat()
            elif len(web_and_time[0].split("-")) == 3:
                return datetime.datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M").isoformat()
            else:  # 11-13 11:04
                dt = datetime.datetime.strptime("{0}-{1}".format(now.year, web_and_time_t),
                                                "%Y-%m-%d %H:%M").isoformat()
                return dt
        elif u'前' in web_and_time_t:
            if u'小时' in web_and_time_t:
                time_back = web_and_time_t.split(u'小时')[0]
                dt = (now - datetime.timedelta(hours=int(time_back))).isoformat()
                return dt
            else:
                time_back = web_and_time_t.split(u'分钟')[0]
                dt = (now - datetime.timedelta(minutes=int(time_back))).isoformat()
                return dt
        else:
            return web_and_time_t

