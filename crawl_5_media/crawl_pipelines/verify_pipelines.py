# -*- coding: utf-8 -*-

import copy
from datetime import datetime
import base64
import hashlib

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.utils.mongo_client import get_test_mongo_client
from crawl_5_media.localsettings import SERVER_IMG_PATH


class VerifyPipeline(object):
    def __init__(self):
        self.mongo_client = get_test_mongo_client()
        self.mongo_collcetion = self.mongo_client['crawled_web_page']['crawl_all_10_page_result']

    def process_item(self, item, spider):
        if isinstance(item, SentKafkaMessage):
            if 'body' in item.keys() and item['body']:
                verify_flag, sent_kafka_message = self.verify_item(item)
                if not verify_flag:
                    copy_sent_kafka_message = copy.deepcopy(sent_kafka_message)
                    spider.new_logger.info('save_error_data to mongo data : data_source_id = {0} ; url = {1} ; id = {2} ; crawlid = {3} ; response_url = {4}'.format(copy_sent_kafka_message['data_source_id'], copy_sent_kafka_message['url'], copy_sent_kafka_message['id'], copy_sent_kafka_message['crawlid'], copy_sent_kafka_message['response_url']))
                    self.save_fail_data(copy_sent_kafka_message, spider)
                    return None
                else:
                    item['img_location'] = sent_kafka_message['img_location']
                    item['small_img_location'] = sent_kafka_message['small_img_location']
                    item['video_location'] = sent_kafka_message['video_location']
        return item


    def verify_item(self, sent_kafka_message):
        img_location_verify_flag = True
        small_img_location_verify_flag = True
        title_verify_flag = True

        sent_kafka_message = item_convert_dict(sent_kafka_message)

        if sent_kafka_message['img_location']:
            img_location_verify_flag, img_location = self.verify_img_location(sent_kafka_message['img_location'])
            sent_kafka_message['img_location'] = img_location
        if img_location_verify_flag and sent_kafka_message['small_img_location']:
            small_img_location_verify_flag, small_img_location = self.verify_img_location(sent_kafka_message['small_img_location'])
            sent_kafka_message['small_img_location'] = small_img_location
        if sent_kafka_message['video_location']:
            video_verify_flag, video_location = self.verify_video_location(sent_kafka_message['video_location'])
            sent_kafka_message['video_location'] = video_location
        
        title = sent_kafka_message['title']
        if not (title and title.strip()):
            title_verify_flag = False

        result_verify_flag = img_location_verify_flag and small_img_location_verify_flag and title_verify_flag

        return result_verify_flag, sent_kafka_message


    def verify_img_location(self, img_location):
        verify_flag = True
        for img_document in img_location:
            try:
                if img_document and 'img_path' in img_document.keys() and img_document['img_path'] and img_document['img_path'].strip():
                    if '/data/shareimg' not in img_document['img_path']:
                        verify_flag = False
                else:
                    verify_flag = False
            except Exception as e:
                print e
            try:
                if img_document and verify_flag:
                    for key, value in img_document.iteritems():
                        if not img_document[key]:
                            img_document[key] = None
            except Exception as e:
                print e
        return verify_flag, img_location


    def verify_video_location(self, video_location):
        verify_flag = True
        for video_document in video_location:
            if video_document and verify_flag:
                for key, value in video_document.iteritems():
                    if not video_document[key]:
                        video_document[key] = None
        return verify_flag, video_location


    def save_fail_data(self, copy_sent_kafka_message, spider):
        copy_sent_kafka_message['body'] = base64.b64encode(copy_sent_kafka_message['body'])
        copy_sent_kafka_message['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if spider.database and spider.collcetion:
            self.mongo_client[spider.database][spider.collcetion + '_fail_data'].insert(copy_sent_kafka_message)
        else:
            self.mongo_collcetion.insert(copy_sent_kafka_message)


def translate_chinese_pinyin(key):
    '''
    used in receive reqest, eg. 
    "网页"-->"webpage", 
    "微信"-->"weixin",
    "微博"-->"weibo",
    "垂直站"-->"chuizhizhan"
    "榜单"-->"bangdan"
    "知道"-->"zhidao"
    "贴吧"-->"tieba"
    '''
    if (key == u'\u7f51\u9875') or (key == u'\\u7f51\\u9875'): return 'webpage'
    elif (key == u'\u5fae\u4fe1') or (key == u'\\u5fae\\u4fe1'): return 'weixin'
    elif (key == u'\u5fae\u535a') or  (key == u'\\u5fae\\u535a'): return 'weibo'
    elif (key == u'\u5782\u76f4\u7ad9') or  (key == u'\\u5782\\u76f4\\u7ad9'): return 'chuizhizhan'
    elif (key == u'\u699c\u5355') or  (key == u'\\u699c\\u5355'): return 'bangdan'
    elif (key == u'\u77e5\u9053') or (key ==u'\\u77e5\\u9053'): return "zhidao"
    elif (key == u'\u8d34\u5427') or (key == u'\\u8d34\\u5427'): return "tieba"
    else: return "error"


def get_generate_id(record_type, timestamp, media, url):
    """
        encrypt timestamp and media as unique id
        """
    m = hashlib.md5()
    if record_type in ["weibo", "weixin"]:
         m.update(str(timestamp))
    m.update(url)
    m.update(media)

    return m.hexdigest()


def item_convert_dict(sent_kafka_message):
    return {
                'data_source_type': sent_kafka_message['data_source_type'] if 'data_source_type' in sent_kafka_message.keys() and sent_kafka_message['data_source_type'] else None,
                'data_source_category': sent_kafka_message['data_source_category'] if 'data_source_category' in sent_kafka_message.keys() and sent_kafka_message['data_source_category'] else None,
                'data_source_class': sent_kafka_message['data_source_class'] if 'data_source_class' in sent_kafka_message.keys() and sent_kafka_message['data_source_class'] else None,
                'data_source_class_id': sent_kafka_message['data_source_class_id'] if 'data_source_class_id' in sent_kafka_message.keys() and sent_kafka_message['data_source_class_id'] else None,
                'data_source_id': sent_kafka_message['data_source_id'] if 'data_source_id' in sent_kafka_message.keys() and sent_kafka_message['data_source_id'] else None,
                'media': sent_kafka_message['media'] if 'media' in sent_kafka_message.keys() and sent_kafka_message['media'] else None,
                'crawlid': sent_kafka_message['crawlid'] if 'crawlid' in sent_kafka_message.keys() and sent_kafka_message['crawlid'] else None,
                'appid': sent_kafka_message['appid'] if 'appid' in sent_kafka_message.keys() and sent_kafka_message['appid'] else None,
                'id': sent_kafka_message['id'] if 'id' in sent_kafka_message.keys() and sent_kafka_message['id'] else None,
                'timestamp': sent_kafka_message['timestamp'] if 'timestamp' in sent_kafka_message.keys() and sent_kafka_message['timestamp'] else None,
                'url': sent_kafka_message['url'] if 'url' in sent_kafka_message.keys() and sent_kafka_message['url'] else None,
                'url_domain': sent_kafka_message['url_domain'] if 'url_domain' in sent_kafka_message.keys() and sent_kafka_message['url_domain'] else None,
                'response_url': sent_kafka_message['response_url'] if 'response_url' in sent_kafka_message.keys() and sent_kafka_message['response_url'] else None,
                'status_code': sent_kafka_message['status_code'] if 'status_code' in sent_kafka_message.keys() and sent_kafka_message['status_code'] else None,
                'title': sent_kafka_message['title'] if 'title' in sent_kafka_message.keys() and sent_kafka_message['title'] else None,
                'desc': sent_kafka_message['desc'] if 'desc' in sent_kafka_message.keys() and sent_kafka_message['desc'] else None,
                'body': sent_kafka_message['body'] if 'body' in sent_kafka_message.keys() and sent_kafka_message['body'] else None,
                'publish_time': sent_kafka_message['publish_time'] if 'publish_time' in sent_kafka_message.keys() and sent_kafka_message['publish_time'] else None,
                'author': sent_kafka_message['author'] if 'author' in sent_kafka_message.keys() and sent_kafka_message['author'] else None,
                'info_source': sent_kafka_message['info_source'] if 'info_source' in sent_kafka_message.keys() and sent_kafka_message['info_source'] else None,
                'video_location': sent_kafka_message['video_location'] if 'video_location' in sent_kafka_message.keys() and sent_kafka_message['video_location'] else None,
                'img_location': sent_kafka_message['img_location'] if 'img_location' in sent_kafka_message.keys() and sent_kafka_message['img_location'] else None,
                'img_location_count': sent_kafka_message['img_location_count'] if 'img_location_count' in sent_kafka_message.keys() and sent_kafka_message['img_location_count'] else None,
                'small_img_location': sent_kafka_message['small_img_location'] if 'small_img_location' in sent_kafka_message.keys() and sent_kafka_message['small_img_location'] else None,
                'small_img_location_count': sent_kafka_message['small_img_location_count'] if 'small_img_location_count' in sent_kafka_message.keys() and sent_kafka_message['small_img_location_count'] else None,
                'parsed_content': sent_kafka_message['parsed_content'] if 'parsed_content' in sent_kafka_message.keys() and sent_kafka_message['parsed_content'] else None,
                'parsed_content_main_body': sent_kafka_message['parsed_content_main_body'] if 'parsed_content_main_body' in sent_kafka_message.keys() and sent_kafka_message['parsed_content_main_body'] else None,
                'parsed_content_char_count': sent_kafka_message['parsed_content_char_count'] if 'parsed_content_char_count' in sent_kafka_message.keys() and sent_kafka_message['parsed_content_char_count'] else None,
                'tags': sent_kafka_message['tags'] if 'tags' in sent_kafka_message.keys() and sent_kafka_message['tags'] else None,
                'like_count': sent_kafka_message['like_count'] if 'like_count' in sent_kafka_message.keys() and sent_kafka_message['like_count'] else None,
                'click_count': sent_kafka_message['click_count'] if 'click_count' in sent_kafka_message.keys() and sent_kafka_message['click_count'] else None,
                'comment_count': sent_kafka_message['comment_count'] if 'comment_count' in sent_kafka_message.keys() and sent_kafka_message['comment_count'] else None,
                'repost_count': sent_kafka_message['repost_count'] if 'repost_count' in sent_kafka_message.keys() and sent_kafka_message['repost_count'] else None,
                'authorized': sent_kafka_message['authorized'] if 'authorized' in sent_kafka_message.keys() and sent_kafka_message['authorized'] else None,
                'article_genre': sent_kafka_message['article_genre'] if 'article_genre' in sent_kafka_message.keys() and sent_kafka_message['article_genre'] else None,
                'info_source_url': sent_kafka_message['info_source_url'] if 'info_source_url' in sent_kafka_message.keys() and sent_kafka_message['info_source_url'] else None,
                'toutiao_out_url': sent_kafka_message['toutiao_out_url'] if 'toutiao_out_url' in sent_kafka_message.keys() and sent_kafka_message['toutiao_out_url'] else None,
                'toutiao_refer_url': sent_kafka_message['toutiao_refer_url'] if 'toutiao_refer_url' in sent_kafka_message.keys() and sent_kafka_message['toutiao_refer_url'] else None,
                'toutiao_category_class_id': sent_kafka_message['toutiao_category_class_id'] if 'toutiao_category_class_id' in sent_kafka_message.keys() and sent_kafka_message['toutiao_category_class_id'] else None,
                'toutiao_category_class': sent_kafka_message['toutiao_category_class'] if 'toutiao_category_class' in sent_kafka_message.keys() and sent_kafka_message['toutiao_category_class'] else None
    }