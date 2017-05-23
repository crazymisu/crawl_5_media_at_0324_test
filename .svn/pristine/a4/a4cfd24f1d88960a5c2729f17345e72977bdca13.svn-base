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


class ToutiaoPC_Parser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

        self.data_source_name = {'CZZ-JH-000007':u'今日头条-科技','CZZ-JH-000006': u'今日头条-娱乐', 'CZZ-JH-000003':u'今日头条-热点', 'CZZ-JH-000012':u'今日头条-军事','CZZ-JH-000019':u'今日头条-旅游', 'CZZ-JH-000018':u'今日头条-国际', 'CZZ-JH-000008':u'今日头条-体育', 'CZZ-JH-000009':u'今日头条-汽车', 'CZZ-JH-000020':u'今日头条-育儿','CZZ-JH-000023': u'今日头条-游戏','CZZ-JH-000022': u'今日头条-美食', 'CZZ-JH-000016':u'今日头条-美文' ,'CZZ-JH-000017':u'今日头条-历史' , 'CZZ-JH-000013':u'今日头条-时尚' ,'CZZ-JH-000014': u'今日头条-探索','CZZ-JH-000011': u'今日头条-搞笑' ,'CZZ-JH-000010':u'今日头条-财经', 'CZZ-JH-000002':u'今日头条-推荐', 'CZZ-JH-000004':u'今日头条-视频','CZZ-JH-000005':u'今日头条-社会','CZZ-JH-000015':u'今日头条-养生', 'CZZ-JH-000021':u'今日头条-故事', 'CZZ-JH-001349':u'今日头条-宠物', 'CZZ-JH-001350':u'今日头条-心理','CZZ-JH-001351':u'今日头条-彩票','CZZ-JH-001352': u'今日头条-情感', 'CZZ-JH-001353':u'今日头条-设计', 'CZZ-JH-001354':u'今日头条-小说','CZZ-JH-001355':u'今日头条-移民', 'CZZ-JH-001356':u'今日头条-职场','CZZ-JH-001357': u'今日头条-电影', 'CZZ-JH-001358':u'今日头条-技术','CZZ-JH-001359':u'今日头条-佛学', 'CZZ-JH-001360':u'今日头条-星座','CZZ-JH-001361': u'今日头条-科普', 'CZZ-JH-001362':u'今日头条-摄影', 'CZZ-JH-001363':u'今日头条-文化', 'CZZ-JH-001364':u'今日头条-三农','CZZ-JH-001365':u'今日头条-两性','CZZ-JH-001366': u'今日头条-动漫','CZZ-JH-001367':u'今日头条-数码' , 'CZZ-JH-001368':u'今日头条-科学','CZZ-JH-001369': u'今日头条-房产', 'CZZ-JH-001370':u'今日头条-健康','CZZ-JH-001371':u'今日头条-美女', 'CZZ-JH-001372':u'今日头条-风水','CZZ-JH-001373': u'今日头条-时政','CZZ-JH-001374': u'今日头条-其它' , 'CZZ-JH-001375':u'今日头条-家居', 'CZZ-JH-001376':u'今日头条-收藏', 'CZZ-JH-001377':u'今日头条-传媒' , 'CZZ-JH-001378':u'今日头条-教育'}

        self.data_source_id = {u'科技': 'CZZ-JH-000007', u'娱乐': 'CZZ-JH-000006', u'热点': 'CZZ-JH-000003', u'军事': 'CZZ-JH-000012',u'旅游': 'CZZ-JH-000019', u'国际': 'CZZ-JH-000018', u'体育': 'CZZ-JH-000008', u'汽车': 'CZZ-JH-000009',u'育儿': 'CZZ-JH-000020', u'游戏': 'CZZ-JH-000023', u'美食': 'CZZ-JH-000022', u'美文': 'CZZ-JH-000016',u'历史': 'CZZ-JH-000017', u'时尚': 'CZZ-JH-000013', u'探索': 'CZZ-JH-000014', u'搞笑': 'CZZ-JH-000011',u'财经': 'CZZ-JH-000010', u'推荐': 'CZZ-JH-000002', u'视频': 'CZZ-JH-000004', u'社会': 'CZZ-JH-000005',
u'养生': 'CZZ-JH-000015', u'故事': 'CZZ-JH-000021', u'宠物': 'CZZ-JH-001349', u'心理': 'CZZ-JH-001350',u'彩票': 'CZZ-JH-001351', u'情感': 'CZZ-JH-001352', u'设计': 'CZZ-JH-001353', u'小说': 'CZZ-JH-001354',u'移民': 'CZZ-JH-001355', u'职场': 'CZZ-JH-001356', u'电影': 'CZZ-JH-001357', u'技术': 'CZZ-JH-001358',u'佛学': 'CZZ-JH-001359', u'星座': 'CZZ-JH-001360', u'科普': 'CZZ-JH-001361', u'摄影': 'CZZ-JH-001362',u'文化': 'CZZ-JH-001363', u'三农': 'CZZ-JH-001364', u'两性': 'CZZ-JH-001365', u'动漫': 'CZZ-JH-001366',u'数码': 'CZZ-JH-001367', u'科学': 'CZZ-JH-001368', u'房产': 'CZZ-JH-001369', u'健康': 'CZZ-JH-001370',
u'美女': 'CZZ-JH-001371', u'风水': 'CZZ-JH-001372', u'时政': 'CZZ-JH-001373', u'其它': 'CZZ-JH-001374',u'家居': 'CZZ-JH-001375', u'收藏': 'CZZ-JH-001376', u'传媒': 'CZZ-JH-001377', u'教育': 'CZZ-JH-001378'}


    def parse_list_page(self, response):
        try:
            info = json.loads(response.body, encoding='utf-8')
            # info = json.loads(response.body_as_unicode())
            newslist = info['data']
            for news in newslist:
                # news = json.loads(str(news))
                """列表页信息"""
                if 'ad_label' not in news and 'source_url' in news and news['source_url'][-1] == '/' and 'source' in news and news['source'] != u'头条问答':
                    original_sent_kafka_message = response.meta['queue_value']
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    sent_kafka_message['toutiao_refer_url'] = 'http://www.toutiao.com/group/' + str(news['group_id']) + '/' if 'group_id' in news else None
                    if 'http' not in news['source_url']:
                        url = 'http://www.toutiao.com' + news['source_url']
                    else:
                        url = news['source_url']
                    sent_kafka_message['url'] = url.replace('group/', 'a').replace('item/', 'i')
                    if 'display_url' in news:
                        sent_kafka_message['toutiao_out_url'] = news['display_url']
                    else:
                        try:
                            r = requests.get(url, allow_redirects=False)
                            if 300 <= r.status_code < 400:
                                sent_kafka_message['toutiao_out_url'] = r.headers['Location']
                            else:
                                sent_kafka_message['toutiao_out_url'] = url
                        except Exception as e:
                            self.error_logger.error(traceback.format_exc())
                            print(traceback.format_exc())
                    sent_kafka_message['title'] = news['title']
                    sent_kafka_message['click_count'] = news['go_detail_count'] if 'go_detail_count' in news else None
                    if 'comments_count' in news:
                        sent_kafka_message['comment_count'] = news['comments_count']
                    elif 'comment_count' in news:
                        sent_kafka_message['comment_count'] = news['comment_count']
                    sent_kafka_message['info_source'] = news['source'] if 'source' in news else None
                    if 'media_url' in news:
                        sent_kafka_message['info_source_url'] = 'http://www.toutiao.com' + news['media_url']
                    sent_kafka_message['desc'] = news['abstract'] if 'abstract' in news else None
                    sent_kafka_message['article_genre'] = news['article_genre'] if 'article_genre' in news else 'article'
                    if 'chinese_tag' in news and news['chinese_tag'] in self.data_source_id:
                        sent_kafka_message['toutiao_category_class_id'] = self.data_source_id[news['chinese_tag']]
                        sent_kafka_message['toutiao_category_class'] = news['chinese_tag']
                    else:
                        sent_kafka_message['toutiao_category_class_id'] = 'CZZ-JH-001374'
                        sent_kafka_message['toutiao_category_class'] = u'其他'
                    sent_kafka_message['small_img_location'] = []
                    if 'image_url' in news and news['image_url']:
                        sent_kafka_message['small_img_location'].append(news['image_url'])
                    elif 'image_list' in news and news['image_list']:
                        for img in news['image_list']:
                            sent_kafka_message['small_img_location'].append(img['url'])
                    if sent_kafka_message['article_genre'] == 'video':
                        publishtime = news['behot_time'] if 'behot_time' in news else None
                        sent_kafka_message['publish_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(publishtime))
                        # sent_kafka_message['small_img_location'] = news['large_image_list'][0]['url'] if 'large_image_list' in news else None
                    elif sent_kafka_message['article_genre'] == 'gallery':
                        publishtime = news['create_time'] if 'create_time' in news else None
                        sent_kafka_message['publish_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(publishtime))

                        sent_kafka_message['like_count'] = news['favorite_count'] if 'favorite_count' in news else None
                    sent_kafka_message['parse_function'] = 'parse_detail_page'
                    yield sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']
            if 'http://www.toutiao.com/i' not in sent_kafka_message['url']:
                sent_kafka_message['data_source_id'] = response.meta['queue_value']['toutiao_category_class_id']
                sent_kafka_message['media'] = self.data_source_name[response.meta['queue_value']['toutiao_category_class_id']]
            sent_kafka_message['response_url'] = response.url
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            sent_kafka_message['status_code'] = response.status
            sent_kafka_message['body'] = response.body_as_unicode()

            if sent_kafka_message['article_genre'] == 'article':
                publish_time = response.xpath(
                    "//*[@class='time']/text()").extract_first()
                sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(
                    publish_time)
                tags_list = response.xpath("//li[@class='label-item']/a/text()").extract()
                sent_kafka_message['tags'] = tags_list if tags_list else None

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

            elif sent_kafka_message['article_genre'] == 'gallery':
                gallery_content = re.findall(r'var gallery[\s\S]*?var siblingList', response.body)
                # gallery_author = re.findall(r'openUrl[\s\S]*?,', response.body)
                # sent_kafka_message['info_source_url'] = 'http://www.toutiao.com' + gallery_author[0].split(':')[1].split(',')[0].split('\'')[1]
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
                    check_flag, img_file_info = save_img_file_to_server(img_url, self.mongo_client, self.redis_client,self.redis_key,self.now_date)
                    if check_flag:
                        img_location.append(
                            {'img_src': img_url, 'img_path': img_file_info['img_file_name'], 'img_index': len(img_location) + 1,'img_desc': ptext[i].strip().replace('\/', '/').decode('unicode-escape'), 'img_width': img_file_info['img_width'],
                             'img_height': img_file_info['img_height']})
                        parsed_content += '<p><img src="{0}"/></p>'.format(img_file_info['img_file_name'])
                    else:
                        img_location.append(
                            {'img_src': img_url, 'img_path': None, 'img_index': len(img_location) + 1, 'img_desc': ptext[i].strip().replace('\/', '/').decode('unicode-escape'),'img_width': None, 'img_height': None})
                        parsed_content += '<p><img src="{0}"/></p>'.format(img_url)
                    img_location_count += 1
                    parsed_content += '<p>'+ ''.join(ptext[i]).replace('\/', '/').decode('unicode-escape') +'</p>'
                    parsed_content_char_count += len(ptext[i])
                    parsed_content_main_body += ptext[i].strip().replace('\/', '/').decode('unicode-escape')
                sent_kafka_message['parsed_content'] = parsed_content
                sent_kafka_message['parsed_content_char_count'] = parsed_content_char_count
                sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body
                sent_kafka_message['img_location'] = img_location
                sent_kafka_message['img_location_count'] = img_location_count
            elif sent_kafka_message['article_genre'] == 'video':
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
                        while_index +=1
                        if str(r.status_code) == '200':
                            put_flag = False
                    except Exception as e:
                        print 'error'
                time.sleep(2)
                page = r.text
                response = response.replace(body=page)
                videodown_url = response.xpath('//*[@id="vjs_video_3_html5_api"]/source/@src').extract_first()
                if videodown_url:
                    check_flag, video_info = save_video_file_to_server(videodown_url, self.mongo_client,self.redis_client, self.redis_key, self.now_date)
                    if check_flag:
                        sent_kafka_message['video_location'] = [
                            {'video_src': videodown_url, 'video_path': video_info['video_path'], 'video_index': 1,'video_desc': '', 'video_duration': video_info['video_duration'],'video_width':video_info['video_width'],'video_height':video_info['video_height']}]

                    else:
                        sent_kafka_message['video_location'] = [
                            {'video_src': videodown_url, 'video_path': None, 'video_index': None, 'video_desc': '','video_duration': None,'video_width':None,'video_height':None}]
                else:
                    self.error_logger.error(traceback.format_exc())
                    print(traceback.format_exc())
            small_imgs = response.meta['queue_value']['small_img_location']
            sent_kafka_message['small_img_location'] = []
            for img_src in small_imgs:
                if img_src:
                    check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,self.redis_key,sent_kafka_message['publish_time'])
                    if not check_flag:
                        small_img_location = {'img_src': img_src, 'img_path': None, 'img_index': 1, 'img_desc': None,'img_width': None, 'img_height': None}
                    else:
                        small_img_location = {'img_src': img_src, 'img_path': img_file_info['img_file_name'],'img_index': 1, 'img_desc': None, 'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height']}
                    sent_kafka_message['small_img_location'].append(small_img_location)
            sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])
            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())

