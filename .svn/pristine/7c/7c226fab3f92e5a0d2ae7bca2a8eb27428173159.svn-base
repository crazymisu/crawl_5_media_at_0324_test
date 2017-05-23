from datetime import datetime
import json
import base64

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.utils.mongo_client import get_test_mongo_client
from crawl_5_media.utils.redis_client import check_hash_md5_url_at_redis
from crawl_5_media.utils.kafka_client import get_kafka_producer
from crawl_5_media.crawl_pipelines.verify_pipelines import item_convert_dict, translate_chinese_pinyin, get_generate_id


class ToutiaoPCPipeline(object):

    def __init__(self):
        self.mongo_client = get_test_mongo_client()
        self.mongo_database = self.mongo_client['crawled_web_page']

        self.producer = get_kafka_producer()
        self.topic_name = 'media.crawled_toutiao_data'

    def process_item(self, item, spider):
        if isinstance(item, SentKafkaMessage):
            if 'body' in item.keys() and item['body']:
                data_source_id = item['data_source_id']
                if 'CZZ-JH' in data_source_id or 'CZZ-TTH' in data_source_id:
                    article_genre = item['article_genre']
                    if article_genre == 'gallery' or article_genre == 'video':
                        sent_kafka_message = item_convert_dict(item)
                        self.save_to_mongo(article_genre, sent_kafka_message, spider)

                        spider.new_logger.info('insert to mongo data : data_source_id = {0} ; url = {1} ; id = {2} ; crawlid = {3} ; response_url = {4}'.format(sent_kafka_message['data_source_id'], sent_kafka_message['url'], sent_kafka_message['id'], sent_kafka_message['crawlid'], sent_kafka_message['response_url']))
                        check_hash_md5_url_at_redis(sent_kafka_message['url'], spider.redis_client, spider.redis_hash_set_key, True)
                        return None
                    elif article_genre == 'article':
                        sent_kafka_message = item_convert_dict(item)

                        self.sent_to_kafka(sent_kafka_message, spider)
                        self.save_to_mongo(article_genre, sent_kafka_message, spider)

                        spider.new_logger.info('insert to mongo data : data_source_id = {0} ; url = {1} ; id = {2} ; crawlid = {3} ; response_url = {4}'.format(sent_kafka_message['data_source_id'], sent_kafka_message['url'], sent_kafka_message['id'], sent_kafka_message['crawlid'], sent_kafka_message['response_url']))
                        check_hash_md5_url_at_redis(sent_kafka_message['url'], spider.redis_client, spider.redis_hash_set_key, True)
                        return None
        return item

    def save_to_mongo(self, article_genre, item, spider):
        collection_name = 'crawl_toutiao_' + article_genre
        sent_kafka_message = item
        sent_kafka_message.pop('body')
        sent_kafka_message['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        record_type = translate_chinese_pinyin(sent_kafka_message['data_source_type'])
        rid = get_generate_id(record_type, sent_kafka_message['publish_time'], sent_kafka_message['media'], sent_kafka_message['url'])
        sent_kafka_message['_id'] = rid

        if spider.database:
            self.mongo_database = self.mongo_client[spider.database]        
        self.mongo_database[collection_name].insert(sent_kafka_message)

    def sent_to_kafka(self, sent_kafka_message, spider):
        send_flag = True if self.producer else False
        sent_kafka_message['body'] = base64.b64encode(sent_kafka_message['body'])
        sent_kafka_message['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        byte_kafka_msg = json.dumps(sent_kafka_message)

        while send_flag:
            try:
                self.producer.send(self.topic_name, byte_kafka_msg)
                send_flag = False
            except Exception as e:
                send_flag = True
                self.producer = get_kafka_producer()