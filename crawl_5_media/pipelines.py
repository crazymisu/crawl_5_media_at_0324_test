# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pickle
import time
import json
import base64
from datetime import datetime

from crawl_5_media.utils.redis_client import get_redis_client, check_hash_md5_url_at_redis
from crawl_5_media.utils.mongo_client import get_test_mongo_client
from crawl_5_media.utils.kafka_client import get_kafka_producer
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.crawl_pipelines.verify_pipelines import item_convert_dict, translate_chinese_pinyin, get_generate_id

class Crawl5MediaPipeline(object):
    def __init__(self):
        self.redis_client = get_redis_client()

        self.producer = get_kafka_producer()
        self.topic_name = 'media.test_crawled_data'
        
        self.mongo_client = get_test_mongo_client()
        self.mongo_collcetion = self.mongo_client['crawled_web_page']['crawl_all_10_page_result']


    def process_item(self, item, spider):
        if isinstance(item, SentKafkaMessage):
            if 'body' in item.keys() and item['body']:
                self.sent_to_kafka(item, spider)
                pass
            else:
                self.redis_client.lpush(spider.key, pickle.dumps(item))
                # import pdb
                # pdb.set_trace()
                # print(item)
                spider.new_logger.info('lpush to redis data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3} ; crawlid = {4} ; redis_key = {5}'.format(item['data_source_id'], item['url'], item['id'], item['parse_function'] if 'parse_function' in item.keys() else 'parse_detail_page', item['crawlid'], spider.key))


    def sent_to_kafka(self, sent_kafka_message, spider):
        send_flag = True if self.producer else False
        sent_kafka_message['body'] = base64.b64encode(sent_kafka_message['body'])
        sent_kafka_message['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sent_kafka_message = self.item_convert_dict(sent_kafka_message)
        byte_kafka_msg = json.dumps(sent_kafka_message)

        sent_kafka_message.pop('body')

        record_type = translate_chinese_pinyin(sent_kafka_message['data_source_type'])
        rid = get_generate_id(record_type, sent_kafka_message['publish_time'], sent_kafka_message['media'], sent_kafka_message['url'])
        sent_kafka_message['_id'] = rid

        if spider.database and spider.collcetion:
            self.mongo_client[spider.database][spider.collcetion].insert(sent_kafka_message)
        else:
            self.mongo_collcetion.insert(sent_kafka_message)

        spider.new_logger.info('insert to mongo data : data_source_id = {0} ; url = {1} ; id = {2} ; crawlid = {3} ; response_url = {4}'.format(sent_kafka_message['data_source_id'], sent_kafka_message['url'], sent_kafka_message['id'], sent_kafka_message['crawlid'], sent_kafka_message['response_url']))
        # check_hash_md5_url_at_redis(sent_kafka_message['url'], self.redis_client, spider.redis_hash_set_key, True)

        # with open('parse_detail_page_result_{0}.json'.format(str(int(time.time()))), 'w') as fp:
        #     json.dump(sent_kafka_message, fp, indent=4)

        while send_flag:
            try:
                self.producer.send(self.topic_name, byte_kafka_msg)
                send_flag = False
            except Exception as e:
                send_flag = True
                self.producer = get_kafka_producer()


    def item_convert_dict(self, sent_kafka_message):
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