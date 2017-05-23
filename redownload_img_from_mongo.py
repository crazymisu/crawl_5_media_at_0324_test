# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import time
import pickle
from datetime import datetime, timedelta

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.utils.mongo_client import get_test_mongo_client, get_mongo_client
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server


def read_fail_data_from_mongo(mongo_client, database_name, collection_name, log_mongo_client, redis_client, redis_key):
    mongo_collection = mongo_client[database_name][collection_name]

    start_date = datetime.now().date() + timedelta(days=-10)
    scroll_size = 1000
    find_query = {'timestamp': {'$gte': str(start_date)}}

    remove_data_list = []
    
    find_result = mongo_collection.find(find_query).sort('_id', -1).limit(scroll_size)
    result_count = find_result.count()
    while result_count > 0:
        result_count = result_count - scroll_size

        for find_item in find_result:
            fail_flag, change_data = check_fail_one_data(find_item, log_mongo_client)
            if fail_flag:
                push_sent_kafka_message_to_redis(redis_client, redis_key)

                remove_data_list.append({'_id': change_data['_id']})

        skip_size = find_result.count() - result_count
        find_result.close()
        find_result = mongo_collection.find(find_query).sort('_id', -1).skip(skip_size).limit(scroll_size)
    find_result.close()

    mongo_multiple_remove(mongo_collection, remove_data_list)


def check_fail_one_data(fail_data, log_mongo_client):
    publish_time = fail_data['publish_time']
    timestamp = fail_data['timestamp']
    img_location = fail_data['img_location']
    small_img_location = fail_data['small_img_location']
    parsed_content = fail_data['parsed_content']

    save_date = publish_time if publish_time else timestamp
    success_flag, img_location = check_img_location(img_location, log_mongo_client, save_date)
    fail_data['img_location'] = img_location

    if not success_flag:
        return success_flag, fail_data

    success_flag, small_img_location = check_img_location(small_img_location, log_mongo_client, save_date)
    fail_data['small_img_location'] = small_img_location

    if not success_flag:
        return success_flag, fail_data

    check_flag, parsed_content = check_parsed_content(img_location, parsed_content)
    fail_data['parsed_content'] = parsed_content    

    return check_flag, fail_data


def check_img_location(img_location, log_mongo_client, save_date):
    success_index = 0
    success_flag = True

    if img_location:
        for img_document in img_location:
            if img_document:
                img_path = img_document['img_path']
                img_src = img_document['img_src']

                if img_src and not img_path:
                    print('redownload img src = {0}'.format(img_src))
                    check_flag, img_file_info = save_img_file_to_server(img_src, log_mongo_client, None, None, save_date)
                    if check_flag:
                        img_document['img_path'] = img_file_info['img_file_name']
                        img_document['img_height'] = img_file_info['img_height']
                        img_document['img_width'] = img_file_info['img_width']
                        success_index += 1
                elif img_src and img_path and img_document['img_height'] and img_document['img_width']:
                    success_index += 1

        if success_index != len(img_location):
            success_flag = False

    return success_flag, img_location


def check_parsed_content(img_location, parsed_content):
    change_index = 0
    change_flag = True
    new_parsed_content = ''
    if img_location:
        soup = BeautifulSoup(parsed_content, 'lxml')
        img_document_list = soup.find_all('img')
        for img_document in img_document_list:
            content_img_src = img_document['src']

            for real_img_document in img_location:
                if content_img_src == real_img_document['img_src']:
                    img_document['src'] = real_img_document['img_path']
                    continue        
        new_parsed_content = soup.find('body').extract()
        if new_parsed_content:
            new_parsed_content = str(new_parsed_content).replace('<body>', '').replace('</body>', '')
        if change_index != len(img_document_list):
            change_flag = False

    return change_flag, new_parsed_content if new_parsed_content else parsed_content


def push_sent_kafka_message_to_redis(redis_client, redis_key, push_data):
    sent_kafka_message = dict_data_convert_to_scrapy_item(push_data)
    if sent_kafka_message and isinstance(sent_kafka_message, SentKafkaMessage):
        redis_client.lpush(redis_key, pickle.dumps(sent_kafka_message))


def dict_data_convert_to_scrapy_item(push_data):
    sent_kafka_message = SentKafkaMessage()
    for key, val in push_data.iteritems():
        sent_kafka_message[key] = val
    return sent_kafka_message


def mongo_multiple_remove(mongo_collection, find_query_list, is_order=None):
    ''' 
    批量删除数据 
    mongo_collection: mongo collection
    find_query_list: remove 数据查询条件列表; list type
    is_order: 是否按顺序执行; default=False; bool type
    '''

    if find_query_list:

        if is_order:
            collection_bulk = mongo_collection.initialize_ordered_bulk_op()
        else:
            collection_bulk = mongo_collection.initialize_unordered_bulk_op()
        
        for find_query in find_query_list:
            print('-' * 50)
            print(find_query)
            print('-' * 50)
            collection_bulk.find(find_query).remove()

        print(collection_bulk.execute())


if __name__ == '__main__':
    log_mongo_client = get_mongo_client()
    mongo_client = get_test_mongo_client()
    database_name = 'crawled_web_page'
    collection_name = 'crawled_new_source_result_fail_data'

    redis_client = get_redis_client()
    redis_key = 'queue:crawl_source:5_media_url_info_intranet_3'
    
    read_fail_data_from_mongo(mongo_client, database_name, collection_name, log_mongo_client, redis_client, redis_key)