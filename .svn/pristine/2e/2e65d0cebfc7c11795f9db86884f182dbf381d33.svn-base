# -*- coding: utf-8 -*-
import scrapy
import re
from urlparse import urlparse, urlunparse
from datetime import datetime
import copy
import traceback
import json
import time
import requests
import urllib
import hashlib
import uuid
import os
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import SERVER_VIDEO_PATH, environment


class ToutiaoShouji_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    sub_column = {427: 'V-1', 86:'V-2', 428: 'V-6', 429: 'V-4', 90: 'V-3', 430: 'V-5', 431: 'V-7', 432: 'V-8', 433: 'V-9', 434: 'V-10'}


    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']
            sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
            """
            对于手机的阳光宽屏需要调整爬取5页
            """
            if sent_kafka_message['data_source_id'] == 'CZZ-JH-001310' and 'refer=1' in sent_kafka_message['id']:
                for num in range(2, 6):
                    link = response.url.replace('refer=1','refer='+str(num))
                    sent_kafka_message['url'] = link
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    if IS_CRAWL_NEXT_PAGE:
                        print('+' * 30 + '-' * 30)
                        print('index = {0} ; data_source_class_id = {1}'.format(num, sent_kafka_message['data_source_class_id']))
                        print('+' * 30 + '-' * 30)
                        yield sent_kafka_message

            info = json.loads(response.body_as_unicode())
            newslist = info['data']
            for news in newslist:
                news = json.loads(str(news['content']))
                """列表页信息"""
                sent_kafka_message['toutiao_refer_url'] = news['article_url'].replace('group/','a') if 'article_url' in news else None
                sent_kafka_message['url'] = news['share_url'].replace('group/','a') if 'share_url' in news else None
                sent_kafka_message['toutiao_out_url'] = news['display_url'] if 'display_url' in news else None
                sent_kafka_message['title'] = news['title'] if 'title' in news else None
                sent_kafka_message['click_count'] = news['read_count'] if 'read_count' in news else None
                sent_kafka_message['like_count'] = news['like_count'] if 'like_count' in news else None
                sent_kafka_message['toutiao_category_class'] = news['tag'] if 'tag' in news else None
                if 'comments_count' in news:
                    sent_kafka_message['comment_count'] = news['comments_count']
                elif 'comment_count' in news:
                    sent_kafka_message['comment_count'] = news['comment_count']
                sent_kafka_message['info_source'] = news['source'] if 'source' in news else None
                if 'media_info' in news and news['media_info'] and 'user_info' in news and news['user_info']:
                    sent_kafka_message['info_source_url'] = 'http://www.toutiao.com/c/user/' + str(news['user_info']['user_id']) + '/#mid=' + str(news['media_info']['media_id']) if 'user_id' and 'media_id' in news['media_info'] else None
                sent_kafka_message['desc'] = news['abstract'] if 'abstract' in news else None
                sent_kafka_message['tags'] = news['keywords'].split(',') if 'keywords' in news else None
                publishtime = news['publish_time'] if 'publish_time' in news else None
                sent_kafka_message['publish_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(publishtime))
                sent_kafka_message['small_img_location'] = []
                small_imgs = []
                if 'image_url' in news and news['image_url']:
                    small_imgs.append(news['image_url'])
                elif 'image_list' in news and news['image_list']:
                    for img in news['image_list']:
                        small_imgs.append(img['url'])
                elif 'large_image_list' in news and news['large_image_list']:
                    small_imgs.append(news['large_image_list'][0]['url'])
                for img_src in small_imgs:
                    if img_src:
                        check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client,self.redis_client, self.redis_key,sent_kafka_message['publish_time'])
                        if not check_flag:
                            small_img_location = {'img_src': img_src, 'img_path': None, 'img_index': 1,'img_desc': None, 'img_width': None, 'img_height': None}
                        else:
                            small_img_location = {'img_src': img_src, 'img_path': img_file_info['img_file_name'],'img_index': 1, 'img_desc': None,'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height']}
                        sent_kafka_message['small_img_location'].append(small_img_location)
                sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
                if sent_kafka_message['data_source_id'] == 'CZZ-JH-001310' and sent_kafka_message['data_source_id'] in self.sub_column.keys():
                    print sent_kafka_message['data_source_class_id']
                    sent_kafka_message['data_source_class_id'] = self.sub_column[sent_kafka_message['data_source_class_id']] if 'data_source_class_id' in sent_kafka_message else None
                    print sent_kafka_message['data_source_class_id']

                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            sent_kafka_message['status_code'] = response.status

            tag = response.xpath('//div[@class="y-left chinese-tag"]/a[2]/text()').extract_first()
            # sent_kafka_message['toutiao_category_class'] = tag if tag else None
            sent_kafka_message['body'] = response.body_as_unicode()

            if '365yg.com' in response.url:
                sent_kafka_message['article_genre'] = 'video'
                if environment == 'intranet':
                    PHANTOMJS_HOSTS = 'http://10.44.163.19:9002/'
                else:
                    PHANTOMJS_HOSTS = 'http://101.200.174.92:9002/'
                cookies = {
                    "uuid": "\"w:aff941666f0f413ebf8565aef7522540\"",
                    "tt_webid": "45204727058",
                    "Toutiao_login_msg": "1",
                    "csrftoken": "8e0385e5595173187e6cd48724f60e56",
                    "CNZZDATA1259612802": "1982111200-1482802520-%7C1482818723",
                    "_ga": "GA1.2.392381939.1482804898",
                    "__tasessionId": "gqyfd4rsf1482824577746",
                }
                post_data = {
                    'url': response.url,
                    'coo': cookies,
                    'ua': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1'
                }
                post_data = json.dumps(post_data)
                put_flag = True
                while_index = 0
                while put_flag and while_index < 10:
                    try:
                        r = requests.request("POST", PHANTOMJS_HOSTS, data=post_data, timeout=20)
                        while_index += 1
                        if str(r.status_code) == '200':
                            put_flag = False
                    except Exception as e:
                        print 'error'
                time.sleep(2)
                page = r.text
                response = response.replace(body=page)
                videodown_url = response.xpath('//*[@id="vjs_video_3_html5_api"]/source/@src').extract_first()
                if videodown_url:
                    check_flag, video_info = save_video_file_to_server(videodown_url, self.mongo_client, self.redis_client,self.redis_key, self.now_date)
                    if check_flag:
                        sent_kafka_message['video_location'] = [
                            {'video_src': videodown_url, 'video_path': video_info['video_path'], 'video_index': 1,'video_desc': '', 'video_duration': video_info['video_duration'],'video_width': video_info['video_width'], 'video_height': video_info['video_height']}]
                    else:
                        sent_kafka_message['video_location'] = [
                            {'video_src': videodown_url, 'video_path': None, 'video_index': None, 'video_desc': '','video_duration': None, 'video_width': None, 'video_height': None}]
                else:
                    self.error_logger.error(traceback.format_exc())
                    print(traceback.format_exc())
            elif sent_kafka_message['data_source_id'] == 'CZZ-JH-001315' or tag == u'图片':
                sent_kafka_message['article_genre'] = 'gallery'
                gallery_content = re.findall(r'var gallery[\s\S]*?var siblingList', response.body)
                gcontent = gallery_content[0].split('var')[1].strip(' ').strip('\n').split('=')[1].strip(' ')[:-1]
                gcontent = eval(gcontent) if gcontent else None
                picture = gcontent['sub_images'] if gcontent and 'sub_images' in gcontent  else []
                ptext = gcontent['sub_abstracts'] if gcontent and 'sub_abstracts' in gcontent  else []
                tags = gcontent['labels'] if gcontent and 'labels' in gcontent  else []
                sent_kafka_message['tags'] = []
                for tag in tags:
                    sent_kafka_message['tags'].append(tag.decode('unicode-escape'))
                img_location = []
                parsed_content = ''
                parsed_content_char_count = 0
                img_location_count = 0
                parsed_content_main_body = ''
                for i in range(len(picture)):
                    img_url = picture[i]['url'].replace('\/', '/').decode('unicode-escape')
                    check_flag, img_file_info = save_img_file_to_server(img_url, self.mongo_client, self.redis_client,
                                                                        self.redis_key, self.now_date)
                    if check_flag:
                        img_location.append(
                            {'img_src': img_url, 'img_path': img_file_info['img_file_name'],
                             'img_index': len(img_location) + 1, 'img_desc': ptext[i].strip().replace('\/', '/').decode('unicode-escape'),
                             'img_width': img_file_info['img_width'],
                             'img_height': img_file_info['img_height']})
                        parsed_content += '<p><img src="{0}"/></p>'.format(img_file_info['img_file_name'])
                    else:
                        img_location.append(
                            {'img_src': img_url, 'img_path': None, 'img_index': len(img_location) + 1, 'img_desc': ptext[i].strip().replace('\/', '/').decode('unicode-escape'),
                             'img_width': None, 'img_height': None})
                        parsed_content += '<p><img src="{0}"/></p>'.format(img_url)
                    img_location_count += 1
                    parsed_content += '<p>' + ''.join(ptext[i]).replace('\/', '/').decode('unicode-escape') + '</p>'
                    parsed_content_char_count += len(ptext[i])
                    parsed_content_main_body += ptext[i].strip().replace('\/', '/').decode('unicode-escape')
                sent_kafka_message['parsed_content'] = parsed_content
                sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                sent_kafka_message['img_location'] = img_location
                sent_kafka_message['img_location_count'] = img_location_count
            else:
                sent_kafka_message['article_genre'] = 'article'
                content = response.xpath("//div[@class='article-content']/div").extract_first()
                if content == None:
                    content = response.xpath("//div[@class='article-content']").extract_first()
                article_content = self.extract_article_parsed_content(content, sent_kafka_message['publish_time'])
                sent_kafka_message['parsed_content_char_count'] = article_content[1]
                sent_kafka_message['parsed_content_main_body'] = article_content[4]
                sent_kafka_message['parsed_content'] = article_content[0]
                sent_kafka_message['authorized'] = article_content[5]
                sent_kafka_message['img_location'] = article_content[2]
                sent_kafka_message['img_location_count'] = article_content[3]

            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

