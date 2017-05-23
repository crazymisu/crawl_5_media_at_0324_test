# -*- coding: utf-8 -*-

import re
from datetime import datetime
from bs4 import BeautifulSoup
from scrapy.spiders import CrawlSpider
import traceback
import logging
from datetime import datetime, timedelta
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client


class Parser(object):
    def __init__(self):
        self.init()

    def init(self):
        self.redis_client = get_redis_client()
        self.mongo_client = get_mongo_client()
        self.now_date = str(datetime.now().date())
        key_prefix = 'queue:crawl_source:'
        queue_type = 'big_media_img_url'
        self.redis_key = key_prefix + queue_type

        logging.basicConfig(filename='error.log', level=logging.INFO, format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')
        self.error_logger = logging.getLogger('error')

    def parse_list_page(self, response):
        raise Exception('Please must be do something at parse_list_page')
        pass

    def parse_detail_page(self, response):
        raise Exception('Please must be do something at parse_detail_page')
        pass

    def parse_article_content(self, content_html, publish_time=None):
        return self.extract_article_parsed_content(content_html, publish_time)

    def extract_article_parsed_content(self, content_html, publish_time=None, url=None, response=None):
        parsed_content = ''
        parsed_content_char_count = 0
        img_location = []
        img_location_count = 0
        parsed_content_main_body = ''
        authorized = ''
        download_img_flag = True
        url = url

        if content_html:
            soup = BeautifulSoup(content_html, 'lxml')
            """change ， 多个div处理"""
            for content in soup.find('body').contents:
                news_content = content.contents
                for one_row_content_item in news_content:
                    document_name = one_row_content_item.name
                    if document_name:
                        if document_name == 'p':
                            p_content = one_row_content_item.get_text().strip('\n').strip()
                            if p_content:
                                is_authorization_flag = self.check_article_authorization(p_content)
                                if not is_authorization_flag:
                                    p_content = p_content.replace('\n', '')
                                    parsed_content += '<p>{0}</p>'.format(p_content.encode('utf-8'))
                                    parsed_content_char_count += len(p_content)
                                    parsed_content_main_body += p_content.strip()
                                else:
                                    authorized += p_content.strip()
                        else:
                            p_content_document_list = one_row_content_item.find_all('p')
                            for p_document in p_content_document_list:
                                p_content = p_document.get_text().strip('\n').strip()
                                if p_content:
                                    is_authorization_flag = self.check_article_authorization(p_content)
                                    if not is_authorization_flag:
                                        p_content = p_content.replace('\n', '')
                                        parsed_content += '<p>{0}</p>'.format(p_content.encode('utf-8'))
                                        parsed_content_char_count += len(p_content)
                                        parsed_content_main_body += p_content.strip()
                                    else:
                                        authorized += p_content.replace('\n', '')

                        if document_name == 'img':
                            one_item_img_list = [one_row_content_item]
                        else:
                            one_item_img_list = one_row_content_item.find_all('img')
                        for img_document in one_item_img_list:
                            if response:
                                img_url = response.urljoin(img_document.get('src'))
                            else:
                                img_url = img_document.get('src')
                            if img_url:
                                if url:
                                    temp_headers = {
                                        'Referer': url,
                                        'Connection': 'keep-alive',
                                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36'
                                    }
                                else:
                                    temp_headers = None
                                check_flag, img_file_info = save_img_file_to_server(img_url, self.mongo_client,
                                                                                    self.redis_client, self.redis_key,
                                                                                    publish_time if publish_time else self.now_date,
                                                                                    temp_headers)
                                if check_flag:
                                    img_document['src'] = img_file_info['img_file_name']
                                    img_location.append({'img_src': img_url, 'img_path': img_file_info['img_file_name'],
                                                         'img_index': len(img_location) + 1, 'img_desc': None,
                                                         'img_width': img_file_info['img_width'],
                                                         'img_height': img_file_info['img_height']})
                                    parsed_content += '<p><img src="{0}"/></p>'.format(img_file_info['img_file_name'])
                                else:
                                    img_location.append(
                                        {'img_src': img_url, 'img_path': None, 'img_index': len(img_location) + 1,
                                         'img_desc': None, 'img_width': None, 'img_height': None})
                                    parsed_content += '<p><img src="{0}"/></p>'.format(img_url)
                                img_location_count += 1

        return parsed_content, parsed_content_char_count, img_location, img_location_count, parsed_content_main_body, authorized, download_img_flag

    def check_article_authorization(self, one_row_content):
        authorization_keyword_list = [u'声明：', u'转载', u'编者按', u'供业内参考', u'供业内人士阅读', u'本文转自', u'本文作者']
        is_authorization_flag = False
        authorization_index = 0

        for authorization_keyword in authorization_keyword_list:
            if authorization_keyword in one_row_content:
                authorization_index += 1

        is_authorization_flag = authorization_index >= 2
        return is_authorization_flag

    def get_webname_time(self, web_and_time_t):
        """
              时间统一格式
              :param self:
              :param web_and_time_t:可能出现的格式有：
                  今天 10:50
                  昨天 10:50
                  1分钟前
                  1小时前
                  11-13 11:44
                  2016-2015-01-31 22:00
                  2015-01-31 22:00
                  2015-11-13 11:00:30
                  2015年03月24日 
                  时间：2016-10-25 15:05:34
              :return:类似 2015-11-13 11:00:30的时间格式
              """
        try:
            web_and_time = web_and_time_t.split()
            now = datetime.now()
            if u"时间" in web_and_time_t:
                web_and_time_t = web_and_time_t.split(u'：')[-1]
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
                    try:
                        return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M").isoformat()
                    except:
                        return datetime.strptime(web_and_time_t, "%Y-%m-%d %H:%M:%S").isoformat()
                elif u"月" in web_and_time[0]:
                    month, day = re.findall(r'\d+', web_and_time[0])
                    minute, second = re.findall(r'\d+', web_and_time[1])
                    dt = "{}-{}-{} {}:{}".format(now.year, month, day, minute, second)
                    return dt
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
        except Exception as e:
            return web_and_time_t

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
                # print(traceback.format_exc())
                pass
        return publish_time

    def extract_number_count(self, content_html):
        if content_html:
            try:
                count_list = re.findall(ur'(?:点击|点击次数|点击量|浏览量|浏览)[\s：:]*(\d+)[\s]*', content_html)[0]
                return count_list
            except:
                count_list = re.findall(r'\d+', content_html)
                if count_list:
                    return int(count_list[0])
        return None

    def extract_info_source_text(self, info_source):
        # 开源可能中间含有空格
        if info_source:
            try:
                info_source = \
                    re.findall(ur'(?:来源|出处)[\s：:]*([\u4e00-\u9fa50-9a-zA-Z]+)[\s]*', info_source.replace(' ', ''))[0]
            except:
                info_source = info_source.strip()
            return info_source
        return None

    def extract_author_text(self, author):
        if author:
            try:
                author = re.findall(ur'(?:作者|发布人|编辑)[\s：:]*([\u4e00-\u9fa50-9a-zA-Z]*)[\s]*', author)[0].strip()
            except:
                author = author.strip()
            return author
        return None

    def extract_time_text(self, publish_time):
        publish_time = publish_time.xpath('string(.)').extract_first()
        if publish_time:
            try:
                publish_time = re.findall(ur'(?:时间|日期)[\s：:]*([0-9\-:：年月日\s]+)[\u4e00-\u9fa5]*', publish_time)[0]
                publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
            except:
                try:
                    publish_time = self.parse_toutiao_publish_time(self.get_webname_time(publish_time))
                except:
                    publish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return publish_time
        return None

    def get_small_img_location(self, small_img_url, publish_time, headers=None):
        check_flag, img_file_info = save_img_file_to_server(small_img_url, self.mongo_client, self.redis_client,
                                                            self.redis_key, publish_time, headers=headers)
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
